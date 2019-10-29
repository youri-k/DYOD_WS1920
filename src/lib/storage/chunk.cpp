#include <iomanip>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "base_segment.hpp"
#include "chunk.hpp"

#include "utils/assert.hpp"

namespace opossum {

void Chunk::add_segment(std::shared_ptr<BaseSegment> segment) { _segments.push_back(segment); }

void Chunk::append(const std::vector<AllTypeVariant>& values) {
  DebugAssert(_segments.size() == values.size(), "The number of passed values doesn't match the number of columns.");

  for (ColumnID current_column_id{0}; current_column_id < column_count(); current_column_id++) {
    _segments[current_column_id]->append(values[current_column_id]);
  }
}

std::shared_ptr<BaseSegment> Chunk::get_segment(ColumnID column_id) const {
  DebugAssert(column_id < _segments.size(), "No segment exists for the given column_id.");
  return _segments.at(column_id);
}

uint16_t Chunk::column_count() const { return _segments.size(); }

uint32_t Chunk::size() const { return _segments.empty() ? 0u : _segments[0]->size(); }

}  // namespace opossum
