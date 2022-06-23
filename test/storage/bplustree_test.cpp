#include "storage/index/bplustree.h"

#include "loggers/index_logger.h"
#include "test_util/test_harness.h"

namespace terrier::storage::index {

struct BPlusTreeTests : public TerrierTest {};

// NOLINTNEXTLINE
TEST_F(BPlusTreeTests, EmptyTest) { EXPECT_TRUE(true); }

TEST_F(BPlusTreeTests, ForwardIterator) {
  VerboseLevel = TreeSummary;
  std::unique_ptr<BPlusTree> tree = std::make_unique<BPlusTree>();
  const int key_num = 1024 * 1024;

  // First insert from 0 to 1 million
  for (int i = 0; i < key_num; i++) {
    tree->Insert(i, i);
  }

  tree->CheckIntegrity();
  tree->PrintInnerStructure();

  auto it = tree->Begin();

  int64_t i = 0;
  while (!it.IsEnd()) {
    EXPECT_EQ(it->key, it->value);
    EXPECT_EQ(it->key, i);

    i++;
    it++;
  }

  // EXPECT_EQ(i, key_num);

  // auto it2 = tree->Begin(key_num - 1);
  // auto it3 = it2;

  // it2++;
  // EXPECT_TRUE(it2.IsEnd());

  // EXPECT_EQ(it3->first, key_num - 1);

  // auto it4 = tree->Begin(key_num + 1);
  // EXPECT_TRUE(it4.IsEnd());
}

}  // namespace terrier::storage::index
