#include <memory>
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

std::function<bool(const ValueID&)> get_bounds_compare_function(ScanType scan_type, ValueID lower_bound,
                                                                ValueID upper_bound) {
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

template <typename T>
std::vector<ChunkOffset> scan_segment(std::function<bool(const T&)> compare,
                                      std::function<const T(const ChunkOffset&)> element_accessor,
                                      size_t container_size) {
  std::vector<ChunkOffset> chunk_offsets;

  for (ChunkOffset current_chunk_offset{0}; current_chunk_offset < container_size; current_chunk_offset++)
    if (compare(element_accessor(current_chunk_offset))) chunk_offsets.push_back(current_chunk_offset);

  return chunk_offsets;
}

Chunk get_reference_chunk(std::shared_ptr<ReferenceSegment> reference_segment, uint16_t column_count) {
  Chunk chunk;

  for (ColumnID column_id{0}; column_id < column_count; column_id++)
    // Create copy of reference segment before inserting it.
    chunk.add_segment(std::make_shared<ReferenceSegment>(reference_segment->referenced_table(), column_id,
                                                         reference_segment->pos_list()));
  return chunk;
}

template <typename T>
void scan_table(std::shared_ptr<const Table> input_table, std::shared_ptr<Table> output_table, ColumnID column_id,
                ScanType scan_type, T typed_search_value) {
  for (ChunkID chunk_id{0}; chunk_id < input_table->chunk_count(); chunk_id++) {
    auto& current_chunk = input_table->get_chunk(chunk_id);

    if (!current_chunk.size()) continue;

    auto current_segment = current_chunk.get_segment(column_id);

    std::vector<ChunkOffset> referenced_chunk_offsets;

    // Calculate referenced chunk offsets according to Segment-Type
    if (auto value_segment = std::dynamic_pointer_cast<ValueSegment<T>>(current_segment); value_segment) {
      referenced_chunk_offsets = scan_segment(
          get_compare_function(scan_type, typed_search_value),
          std::function<const T(const ChunkOffset&)>{
              [values = value_segment->values()](const ChunkOffset& chunk_offset) { return values.at(chunk_offset); }},
          value_segment->size());
    } else if (auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<T>>(current_segment);
               dictionary_segment) {
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

    // Don't append empty chunks to output_table
    if (referenced_chunk_offsets.empty()) continue;

    auto pos_list = std::make_shared<PosList>();

    // Generate PositionList for current chunk based on calculated chunk offsets
    for (auto& chunk_offset : referenced_chunk_offsets) pos_list->push_back({chunk_id, chunk_offset});

    // Create Reference Segment from PositionList
    auto reference_segment = std::make_shared<ReferenceSegment>(input_table, column_id, pos_list);

    // Inflate ReferenceSegment to a Chunk, where every segment is a
    // copy of ReferenceSegment and insert into output table.
    output_table->emplace_chunk(get_reference_chunk(reference_segment, input_table->column_count()));
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
