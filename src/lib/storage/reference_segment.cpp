#include <memory>

#include "reference_segment.hpp"

namespace opossum {

ReferenceSegment::ReferenceSegment(const std::shared_ptr<const Table> referenced_table,
                                   const opossum::ColumnID referenced_column_id,
                                   const std::shared_ptr<const PosList> pos)
    : _referenced_table(referenced_table), _referenced_column_id(referenced_column_id), _positions(pos) {}

AllTypeVariant ReferenceSegment::operator[](const ChunkOffset chunk_offset) const {
  // Get the correct position within the PositionList for the given chunk_offset
  const auto& pos = (*_positions)[chunk_offset];

  // Then get the Chunk from the referenced Table, and the Segment for the referenced ColumnID
  const auto& chunk = _referenced_table->get_chunk(pos.chunk_id);
  const auto& segment = chunk.get_segment(_referenced_column_id);

  // Finally, return the value from the Segment (can either be Dictionary of Value) for the given chunk_offset
  return (*segment)[pos.chunk_offset];
}

size_t ReferenceSegment::size() const { return _positions->size(); }

const std::shared_ptr<const Table> ReferenceSegment::referenced_table() const { return _referenced_table; }

ColumnID ReferenceSegment::referenced_column_id() const { return _referenced_column_id; }

const std::shared_ptr<const PosList> ReferenceSegment::pos_list() const { return _positions; }

size_t ReferenceSegment::estimate_memory_usage() const { return sizeof(RowID) * _positions->size(); }

}  // namespace opossum
