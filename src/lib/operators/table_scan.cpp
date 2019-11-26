#include <memory>
#include <utility>
#include <vector>

#include "resolve_type.hpp"
#include "storage/reference_segment.hpp"
#include "storage/table.hpp"
#include "table_scan.hpp"
#include "types.hpp"

namespace opossum {
TableScan::TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id, const ScanType scan_type,
                     const AllTypeVariant search_value)
    : AbstractOperator(in), _column_id{column_id}, _scan_type{scan_type}, _search_value{search_value} {}

ColumnID TableScan::column_id() const { return _column_id; }

ScanType TableScan::scan_type() const { return _scan_type; }

const AllTypeVariant& TableScan::search_value() const { return _search_value; }

namespace {

// retrieves the compare operator as lambda function for a given scan_type and value
template <typename T>
std::function<bool(const T&)> get_compare_function(ScanType scan_type, T& search_value) {
  switch (scan_type) {
    case ScanType::OpEquals:
      return [search_value](const T& value) { return value == search_value; };
    case ScanType::OpNotEquals:
      return [search_value](const T& value) { return value != search_value; };
    case ScanType::OpLessThan:
      return [search_value](const T& value) { return value < search_value; };
    case ScanType::OpLessThanEquals:
      return [search_value](const T& value) { return value <= search_value; };
    case ScanType::OpGreaterThan:
      return [search_value](const T& value) { return value > search_value; };
    case ScanType::OpGreaterThanEquals:
      return [search_value](const T& value) { return value >= search_value; };
    default:
      throw std::runtime_error("Unknown ScanType operator.");
  }
}

// The get_bounds_compare_function allows for the usage of the lower_bound and upper_bound functionality
// of the DictionarySegment and therefore returns a compare operator for the given scan_type that utilizes binary search
std::function<bool(const ValueID&)> get_bounds_compare_function(ScanType scan_type, ValueID lower_bound,
                                                                ValueID upper_bound) {
  // handle the case where you always want to return the same bool, no matter what the value_id is
  // this lamba is used when the lower_bound of a DictionarySegment returns invalid
  auto always = [](bool flag) {
    return std::function<bool(const ValueID&)>{[flag](const ValueID& value_id) { return flag; }};
  };

  switch (scan_type) {
    case ScanType::OpEquals:
      if (lower_bound == INVALID_VALUE_ID) {
        return always(false);
      } else if (upper_bound == INVALID_VALUE_ID) {
        return [lower_bound](const ValueID& value_id) { return value_id >= lower_bound; };
      } else {
        return [lower_bound, upper_bound](const ValueID& value_id) {
          return value_id >= lower_bound && value_id < upper_bound;
        };
      }
    case ScanType::OpNotEquals:
      if (lower_bound == INVALID_VALUE_ID) {
        return always(true);
      } else if (upper_bound == INVALID_VALUE_ID) {
        return [lower_bound](const ValueID& value_id) { return value_id < lower_bound; };
      } else {
        return [lower_bound, upper_bound](const ValueID& value_id) {
          return value_id < lower_bound || value_id >= upper_bound;
        };
      }
    case ScanType::OpLessThan:
      return (lower_bound == INVALID_VALUE_ID) ? always(true) : [lower_bound](const ValueID& value_id) {
        return value_id < lower_bound;
      };
    case ScanType::OpLessThanEquals:
      return (upper_bound == INVALID_VALUE_ID) ? always(true) : [upper_bound](const ValueID& value_id) {
        return value_id < upper_bound;
      };
    case ScanType::OpGreaterThan:
      return (upper_bound == INVALID_VALUE_ID) ? always(false) : [upper_bound](const ValueID& value_id) {
        return value_id >= upper_bound;
      };
    case ScanType::OpGreaterThanEquals:
      return (lower_bound == INVALID_VALUE_ID) ? always(false) : [lower_bound](const ValueID& value_id) {
        return value_id >= lower_bound;
      };
    default:
      throw std::runtime_error("Unknown ScanType operator.");
  }
}

// Scans a segment of values and adds their chunk_offset to the returned container
// if the passed compare operator returned true.
// The element_accessor std::function parameter is used to access the actual values of the segment.
template <typename T>
std::vector<ChunkOffset> scan_segment(std::function<bool(const T&)> compare,
                                      std::function<const T(const ChunkOffset&)> element_accessor,
                                      size_t container_size) {
  std::vector<ChunkOffset> chunk_offsets;

  for (ChunkOffset current_chunk_offset{0}; current_chunk_offset < container_size; current_chunk_offset++)
    if (compare(element_accessor(current_chunk_offset))) chunk_offsets.push_back(current_chunk_offset);

  return chunk_offsets;
}

// Scans the input table and creates an output table containing a Chunk of ReferenceSegments pointing to the values
// that fulfill the compare operator with the given scan_type and search_value
template <typename T>
void scan_table(std::shared_ptr<const Table> input_table, const std::shared_ptr<Table>& output_table,
                const ColumnID& column_id, ScanType scan_type, T typed_search_value) {
  for (ChunkID chunk_id{0}; chunk_id < input_table->chunk_count(); chunk_id++) {
    auto& current_chunk = input_table->get_chunk(chunk_id);

    // no need to scan through the chunk if it's empty
    if (!current_chunk.size()) continue;

    auto current_segment = current_chunk.get_segment(column_id);

    std::vector<ChunkOffset> referenced_chunk_offsets;

    // Calculate referenced chunk offsets by scanning the segment values
    // Therefore cast the BaseSegment pointer to the appropriate concrete segment type by try-and-error
    if (auto value_segment = std::dynamic_pointer_cast<ValueSegment<T>>(current_segment); value_segment) {
      referenced_chunk_offsets = scan_segment(
          get_compare_function(scan_type, typed_search_value),
          std::function<const T(const ChunkOffset&)>{
              [values = value_segment->values()](const ChunkOffset& chunk_offset) { return values.at(chunk_offset); }},
          value_segment->size());
    } else if (auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<T>>(current_segment);
               dictionary_segment) {
      // In the case of a DictionarySegment, we want to make use of the fact that the values are sorted
      // That is why we use the lower and upper bound methods here and a different getter for the compare operator
      referenced_chunk_offsets = scan_segment<ValueID>(
          get_bounds_compare_function(scan_type, dictionary_segment->lower_bound(typed_search_value),
                                      dictionary_segment->upper_bound(typed_search_value)),
          std::function<const ValueID(const ChunkOffset&)>{
              [attribute_vector = dictionary_segment->attribute_vector()](const ChunkOffset& chunk_offset) {
                return attribute_vector->get(chunk_offset);
              }},
          dictionary_segment->size());
    } else if (auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(current_segment);
               reference_segment) {
      referenced_chunk_offsets = scan_segment(
          get_compare_function(scan_type, typed_search_value),
          std::function<const T(const ChunkOffset&)>{[&reference_segment](const ChunkOffset& chunk_offset) {
            return type_cast<T>((*reference_segment)[chunk_offset]);
          }},
          reference_segment->size());
    } else {
      throw std::runtime_error("TableScan only supports DictionarySegment, ValueSegment and ReferenceSegment.");
    }

    // Don't append an empty chunks to the output table
    if (referenced_chunk_offsets.empty()) continue;

    auto pos_list = std::make_shared<PosList>();

    // Generate PositionList for current chunk based on calculated chunk offsets
    for (auto& chunk_offset : referenced_chunk_offsets) pos_list->push_back({chunk_id, chunk_offset});

    // Create the output ReferenceSegments from the PositionList for each column
    Chunk output_chunk;
    for (ColumnID current_column_id{0}; current_column_id < input_table->column_count(); current_column_id++) {
      output_chunk.add_segment(std::make_shared<ReferenceSegment>(input_table, current_column_id, pos_list));
    }
    output_table->emplace_chunk(std::move(output_chunk));
  }
}

}  // namespace

std::shared_ptr<const Table> TableScan::_on_execute() {
  auto input_table = _input_table_left();

  auto output_table = std::make_shared<Table>();

  // Create definition for output_table according to definition of input_table
  for (ColumnID column_id; column_id < input_table->column_count(); column_id++)
    output_table->add_column_definition(input_table->column_name(column_id), input_table->column_type(column_id));

  resolve_data_type(input_table->column_type(_column_id), [&](auto type) {
    using Type = typename decltype(type)::type;
    auto typed_search_value = type_cast<Type>(_search_value);

    scan_table<Type>(input_table, output_table, _column_id, _scan_type, typed_search_value);
  });

  // If the generated output_table is empty, we at least have to create an empty chunk
  if (!output_table->row_count()) output_table->create_new_chunk();

  return output_table;
}

}  // namespace opossum
