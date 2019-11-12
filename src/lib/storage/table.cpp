#include "table.hpp"

#include <algorithm>
#include <future>
#include <iomanip>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "dictionary_segment.hpp"
#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "value_segment.hpp"

namespace opossum {

Table::Table(uint32_t chunk_size) : _max_chunk_size(chunk_size) { _chunks.emplace_back(); }

void Table::add_column(const std::string& name, const std::string& type) {
  std::lock_guard<std::mutex> lock_guard(*_access_mutex);
  DebugAssert(row_count() == 0, "The table already contains data so adding a column is not possible.");

  _column_names.push_back(name);
  _column_types.push_back(type);

  auto segment = make_shared_by_data_type<BaseSegment, ValueSegment>(type);

  _chunks.front().add_segment(segment);
}

void Table::append(std::vector<AllTypeVariant> values) {
  std::lock_guard<std::mutex> lock_guard(*_access_mutex);
  if (_chunks.back().size() + 1 > _max_chunk_size) {
    _chunks.emplace_back();

    for (const auto& _column_type : _column_types) {
      auto segment = make_shared_by_data_type<BaseSegment, ValueSegment>(_column_type);

      _chunks.back().add_segment(segment);
    }
  }

  _chunks.back().append(values);
}

uint16_t Table::column_count() const { return _chunks.front().column_count(); }

uint64_t Table::row_count() const { return (_chunks.size() - 1) * _max_chunk_size + _chunks.back().size(); }

ChunkID Table::chunk_count() const { return static_cast<ChunkID>(_chunks.size()); }

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  auto element_it = std::find(_column_names.cbegin(), _column_names.cend(), column_name);

  if (element_it == _column_names.cend()) throw std::out_of_range("Column name doesn't exist");

  auto column_id = std::distance(_column_names.cbegin(), element_it);

  return static_cast<ColumnID>(column_id);
}

uint32_t Table::max_chunk_size() const { return _max_chunk_size; }

const std::vector<std::string>& Table::column_names() const { return _column_names; }

const std::string& Table::column_name(ColumnID column_id) const { return _column_names.at(column_id); }

const std::string& Table::column_type(ColumnID column_id) const { return _column_types.at(column_id); }

Chunk& Table::get_chunk(ChunkID chunk_id) {
  std::lock_guard<std::mutex> lock_guard(*_access_mutex);
  return _chunks.at(chunk_id);
}

const Chunk& Table::get_chunk(ChunkID chunk_id) const { return _chunks.at(chunk_id); }

void Table::compress_chunk(ChunkID chunk_id) {
  std::lock_guard<std::mutex> lock_guard(*_access_mutex);
  Chunk compressed_chunk;
  auto& current_chunk = _chunks.at(chunk_id);

  std::vector<std::thread> threads;
  std::vector<std::shared_ptr<BaseSegment>> compressed_segments(_column_types.size());
  threads.reserve(_column_types.size());

  // compressing one chunks means turning all the ValueSegments into DictionarySegments
  for (std::size_t i = 0; i < _column_types.size(); i++) {
    threads.emplace_back([& compressed_segment = compressed_segments[i], &column_type = _column_types[i],
                          value_segment = current_chunk.get_segment(ColumnID(i))] {
      compressed_segment = make_shared_by_data_type<BaseSegment, DictionarySegment>(column_type, value_segment);
    });
  }

  for (std::size_t i = 0; i < _column_types.size(); i++) {
    threads[i].join();
    compressed_chunk.add_segment(compressed_segments[i]);
  }

  // then switch the current chunk with the newly compressed one
  current_chunk = std::move(compressed_chunk);
}

}  // namespace opossum
