#include <memory>
#include <string>

#include "gtest/gtest.h"

#include "../../lib/all_type_variant.hpp"
#include "../../lib/resolve_type.hpp"
#include "../../lib/storage/base_segment.hpp"
#include "../../lib/storage/dictionary_segment.hpp"
#include "../../lib/storage/value_segment.hpp"
#include "../../lib/types.hpp"

class StorageDictionarySegmentTest : public ::testing::Test {
 protected:
  std::shared_ptr<opossum::ValueSegment<int>> vc_int = std::make_shared<opossum::ValueSegment<int>>();
  std::shared_ptr<opossum::ValueSegment<std::string>> vc_str = std::make_shared<opossum::ValueSegment<std::string>>();
};

TEST_F(StorageDictionarySegmentTest, CompressSegmentString) {
  vc_str->append("Bill");
  vc_str->append("Steve");
  vc_str->append("Alexander");
  vc_str->append("Steve");
  vc_str->append("Hasso");
  vc_str->append("Bill");

  auto col = opossum::make_shared_by_data_type<opossum::BaseSegment, opossum::DictionarySegment>("string", vc_str);
  auto dict_col = std::dynamic_pointer_cast<opossum::DictionarySegment<std::string>>(col);

  // Test attribute_vector size
  EXPECT_EQ(dict_col->size(), 6u);

  // Test dictionary size (uniqueness)
  EXPECT_EQ(dict_col->unique_values_count(), 4u);

  // Test sorting
  auto dict = dict_col->dictionary();
  EXPECT_EQ((*dict)[0], "Alexander");
  EXPECT_EQ((*dict)[1], "Bill");
  EXPECT_EQ((*dict)[2], "Hasso");
  EXPECT_EQ((*dict)[3], "Steve");
}

TEST_F(StorageDictionarySegmentTest, LowerUpperBound) {
  for (int i = 0; i <= 10; i += 2) vc_int->append(i);

  auto col = opossum::make_shared_by_data_type<opossum::BaseSegment, opossum::DictionarySegment>("int", vc_int);
  auto dict_col = std::dynamic_pointer_cast<opossum::DictionarySegment<int>>(col);

  EXPECT_EQ(dict_col->lower_bound(4), (opossum::ValueID)2);
  EXPECT_EQ(dict_col->upper_bound(4), (opossum::ValueID)3);

  EXPECT_EQ(dict_col->lower_bound(5), (opossum::ValueID)3);
  EXPECT_EQ(dict_col->upper_bound(5), (opossum::ValueID)3);

  EXPECT_EQ(dict_col->lower_bound(15), opossum::INVALID_VALUE_ID);
  EXPECT_EQ(dict_col->upper_bound(15), opossum::INVALID_VALUE_ID);
}

TEST_F(StorageDictionarySegmentTest, MemoryUsage) {
  for (int i = 0; i < 10; i++) vc_int->append(i);

  auto col = opossum::make_shared_by_data_type<opossum::BaseSegment, opossum::DictionarySegment>("int", vc_int);
  auto dict_col = std::dynamic_pointer_cast<opossum::DictionarySegment<int>>(col);

  EXPECT_EQ(dict_col->estimate_memory_usage(), 50);

  for (int i = 0; i < 10; i++) vc_int->append(i);

  col = opossum::make_shared_by_data_type<opossum::BaseSegment, opossum::DictionarySegment>("int", vc_int);
  dict_col = std::dynamic_pointer_cast<opossum::DictionarySegment<int>>(col);

  EXPECT_EQ(dict_col->estimate_memory_usage(), 60);
}

TEST_F(StorageDictionarySegmentTest, GetValue) {
  for (int i = 0; i < 10; i++)
    for (int j = 0; j < 3; j++) vc_int->append(i);

  auto col = opossum::make_shared_by_data_type<opossum::BaseSegment, opossum::DictionarySegment>("int", vc_int);
  auto dict_col = std::dynamic_pointer_cast<opossum::DictionarySegment<int>>(col);

  EXPECT_EQ(dict_col->get(0), 0);
  EXPECT_EQ(dict_col->get(3), 1);
  EXPECT_EQ(dict_col->operator[](opossum::ChunkOffset{4}), opossum::AllTypeVariant(1));
  EXPECT_EQ(dict_col->operator[](opossum::ChunkOffset{29}), opossum::AllTypeVariant(9));
}

TEST_F(StorageDictionarySegmentTest, AppendValue) {
  vc_str->append("TEST");

  auto col = opossum::make_shared_by_data_type<opossum::BaseSegment, opossum::DictionarySegment>("string", vc_str);
  auto dict_col = std::dynamic_pointer_cast<opossum::DictionarySegment<std::string>>(col);

  EXPECT_THROW(dict_col->append("POSSIBLE"), std::runtime_error);
}

TEST_F(StorageDictionarySegmentTest, DifferentDatasetSizes) {
  for (int i = 0; i < 200; i++) vc_int->append(i);

  auto col = opossum::make_shared_by_data_type<opossum::BaseSegment, opossum::DictionarySegment>("int", vc_int);
  auto dict_col = std::dynamic_pointer_cast<opossum::DictionarySegment<int>>(col);

  EXPECT_EQ(dict_col->attribute_vector()->width(), 1);

  for (int i = 0; i < 500; i++) vc_int->append(i);

  col = opossum::make_shared_by_data_type<opossum::BaseSegment, opossum::DictionarySegment>("int", vc_int);
  dict_col = std::dynamic_pointer_cast<opossum::DictionarySegment<int>>(col);

  EXPECT_EQ(dict_col->attribute_vector()->width(), 2);

  for (int i = 0; i < 70000; i++) vc_int->append(i);

  col = opossum::make_shared_by_data_type<opossum::BaseSegment, opossum::DictionarySegment>("int", vc_int);
  dict_col = std::dynamic_pointer_cast<opossum::DictionarySegment<int>>(col);

  EXPECT_EQ(dict_col->attribute_vector()->width(), 4);
}

// TODO(student): You should add some more tests here (full coverage would be appreciated) and possibly in other files.
