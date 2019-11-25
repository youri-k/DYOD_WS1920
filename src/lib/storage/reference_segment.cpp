#include <memory>

#include "reference_segment.hpp"

namespace opossum {

ReferenceSegment::ReferenceSegment(const std::shared_ptr<const Table> referenced_table,
                                   const opossum::ColumnID referenced_column_id,
                                   const std::shared_ptr<const PosList> pos)
    : _referenced_table(referenced_table), _referenced_column_id(referenced_column_id), _pos(pos) {}

AllTypeVariant ReferenceSegment::operator[](const ChunkOffset chunk_offset) const {
  const auto& pos = (*_pos)[chunk_offset];
  const auto& chunk = _referenced_table->get_chunk(pos.chunk_id);
  const auto& segment = chunk.get_segment(referenced_column_id());
  return (*segment)[pos.chunk_offset];
}

size_t ReferenceSegment::size() const { return _pos->size(); }

const std::shared_ptr<const Table> ReferenceSegment::referenced_table() const { return _referenced_table; }

ColumnID ReferenceSegment::referenced_column_id() const { return _referenced_column_id; }

const std::shared_ptr<const PosList> ReferenceSegment::pos_list() const { return _pos; }

size_t ReferenceSegment::estimate_memory_usage() const { return sizeof(RowID) * _pos->size(); }

}  // namespace opossum
