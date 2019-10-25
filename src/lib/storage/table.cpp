#include "table.hpp"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "value_segment.hpp"

#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {
Table::Table(uint32_t chunk_size) : _max_chunk_size(chunk_size) { _chunks.push_back(std::make_shared<Chunk>()); }

void Table::add_column(const std::string& name, const std::string& type) {
  DebugAssert(row_count() == 0, "The table already contains data so adding a column is not possible.");

  _column_names.push_back(name);
  _column_types.push_back(type);

  auto segment = make_shared_by_data_type<BaseSegment, ValueSegment>(type);

  _chunks.front()->add_segment(segment);
}

void Table::append(std::vector<AllTypeVariant> values) {
  DebugAssert(values.size() == column_count(), "The number of values doesn't match number of columns.");

  if (_chunks.back()->size() >= _max_chunk_size) {
    _chunks.push_back(std::make_shared<Chunk>());

    for (const auto& _column_type : _column_types) {
      auto segment = make_shared_by_data_type<BaseSegment, ValueSegment>(_column_type);

      _chunks.back()->add_segment(segment);
    }
  }

  _chunks.back()->append(values);
}

uint16_t Table::column_count() const { return _chunks.front()->column_count(); }

uint64_t Table::row_count() const {
  uint64_t number_of_rows = 0;
  for (const auto& chunk : _chunks) {
    number_of_rows += chunk->size();
  }
  return number_of_rows;
}

ChunkID Table::chunk_count() const { return (ChunkID)_chunks.size(); }

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  auto column_id =
      std::distance(_column_names.cbegin(), std::find(_column_names.cbegin(), _column_names.cend(), column_name));

  DebugAssert(column_id < static_cast<int64_t>(_column_names.size()), "The column couldn't be found.");

  return (ColumnID)column_id;
}

uint32_t Table::max_chunk_size() const { return _max_chunk_size; }

const std::vector<std::string>& Table::column_names() const { return _column_names; }

const std::string& Table::column_name(ColumnID column_id) const { return _column_names.at(column_id); }

const std::string& Table::column_type(ColumnID column_id) const { return _column_types.at(column_id); }

Chunk& Table::get_chunk(ChunkID chunk_id) { return *(_chunks.at(chunk_id)); }

const Chunk& Table::get_chunk(ChunkID chunk_id) const { return *(_chunks.at(chunk_id)); }

    void Table::emplace_chunk(Chunk chunk) {
        if(row_count() == 0) {
            _chunks.at(0) = std::shared_ptr<Chunk>(&chunk);
        } else {
            _chunks.push_back(std::shared_ptr<Chunk>(&chunk));
        }

    }

}  // namespace opossum
