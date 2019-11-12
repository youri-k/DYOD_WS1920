#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/resolve_type.hpp"
#include "../lib/storage/table.hpp"

namespace opossum {

class StorageTableTest : public BaseTest {
 protected:
  void SetUp() override {
    t.add_column("col_1", "int");
    t.add_column("col_2", "string");
  }

  Table t{2};
};

TEST_F(StorageTableTest, ChunkCount) {
  EXPECT_EQ(t.chunk_count(), 1u);
  t.append({4, "Hello,"});
  t.append({6, "world"});
  t.append({3, "!"});
  EXPECT_EQ(t.chunk_count(), 2u);
}

TEST_F(StorageTableTest, GetChunk) {
  t.get_chunk(ChunkID{0});
  // TODO(anyone): Do we want checks here?
  // EXPECT_THROW(t.get_chunk(ChunkID{q}), std::exception);
  t.append({4, "Hello,"});
  t.append({6, "world"});
  t.append({3, "!"});
  t.get_chunk(ChunkID{1});
}

TEST_F(StorageTableTest, ColumnCount) { EXPECT_EQ(t.column_count(), 2u); }

TEST_F(StorageTableTest, RowCount) {
  EXPECT_EQ(t.row_count(), 0u);
  t.append({4, "Hello,"});
  t.append({6, "world"});
  t.append({3, "!"});
  EXPECT_EQ(t.row_count(), 3u);
}

TEST_F(StorageTableTest, GetColumnName) {
  EXPECT_EQ(t.column_name(ColumnID{0}), "col_1");
  EXPECT_EQ(t.column_name(ColumnID{1}), "col_2");
  // TODO(anyone): Do we want checks here?
  EXPECT_THROW(t.column_name(ColumnID{2}), std::exception);
}

TEST_F(StorageTableTest, GetColumnType) {
  EXPECT_EQ(t.column_type(ColumnID{0}), "int");
  EXPECT_EQ(t.column_type(ColumnID{1}), "string");
  // TODO(anyone): Do we want checks here?
  EXPECT_THROW(t.column_type(ColumnID{2}), std::exception);
}

TEST_F(StorageTableTest, GetColumnIdByName) {
  EXPECT_EQ(t.column_id_by_name("col_2"), 1u);
  EXPECT_THROW(t.column_id_by_name("no_column_name"), std::exception);
}

TEST_F(StorageTableTest, GetChunkSize) { EXPECT_EQ(t.max_chunk_size(), 2u); }

TEST_F(StorageTableTest, GetAllColumnNames) {
  std::vector<std::string> test_vector{"col_1", "col_2"};
  EXPECT_EQ(t.column_names(), test_vector);
}

TEST_F(StorageTableTest, GetConstantChunk) {
  const auto table = Table();
  const Chunk& chunk = table.get_chunk(ChunkID{0});
  EXPECT_EQ(chunk.column_count(), 0);
}

TEST_F(StorageTableTest, CompressChunk) {
  t.append({4, "Hello,"});
  t.append({4, "world"});

  EXPECT_EQ(t.get_chunk(ChunkID{0}).get_segment(ColumnID{0})->estimate_memory_usage(), 8u);

  t.compress_chunk(ChunkID{0});

  EXPECT_EQ(t.get_chunk(ChunkID{0}).get_segment(ColumnID{0})->estimate_memory_usage(), 6u);
}

}  // namespace opossum
