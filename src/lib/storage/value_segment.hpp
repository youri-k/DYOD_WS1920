#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_segment.hpp"

namespace opossum {

// ValueSegment is a segment type that stores all its values in a vector
template <typename T>
class ValueSegment : public BaseSegment {
 public:
  // return the value at a certain position. If you want to write efficient operators, back off!
  AllTypeVariant operator[](const ChunkOffset chunk_offset) const final;

  // add a value to the end
  void append(const AllTypeVariant& val) final;

  // return the number of entries
  size_t size() const final;

  // Return all values. This is the preferred method to check a value at a certain index. Usually you need to
  // access more than a single value anyway.
  // e.g. const auto& values = value_segment.values(); and then: values[i]; in your loop.
  const std::vector<T>& values() const;

  // returns the calculated memory usage
  size_t estimate_memory_usage() const final;

 protected:
  // Implementation goes here
};

}  // namespace opossum
