#pragma once

#include <functional>

#include "loggers/index_logger.h"

namespace terrier::storage::index {

template <typename KeyType, typename ValueType, typename KeyComparator = std::less<KeyType>,
          typename KeyEqualityChecker = std::equal_to<KeyType>, typename KeyHashFunc = std::hash<KeyType>,
          typename ValueEqualityChecker = std::equal_to<ValueType>>
class BPlusTree {
 public:
  using KeyValuePair = std::pair<KeyType, ValueType>;

  BPlusTree() {
    INDEX_LOG_INFO(
        "B+ Tree Constructor called. "
        "Setting up execution environment...");
  }

  ~BPlusTree() {
    INDEX_LOG_INFO(
        "B+ Tree Destructor called. "
        "Cleaning up execution environment...");
  }

  void PrintContents() { INDEX_LOG_INFO("B+ Tree Contents:"); }

  void Insert(const KeyType &key, const ValueType &value) {}

  /*
   * Iterator Interface
   */
  class ForwardIterator;

  /*
   * Begin() - Return an iterator pointing the first element in the tree
   *
   * If the tree is currently empty, then the iterator is both a begin
   * iterator and an end iterator (i.e. both flags are set to true). This
   * is a valid state.
   */
  ForwardIterator Begin() { return ForwardIterator{this}; }

  /*
   * Iterator Interface
   */
  class ForwardIterator {
   private:
    KeyValuePair *kv_pair;

   public:
    ForwardIterator(BPlusTree *tree) : kv_pair{nullptr} {}

    bool IsEnd() { return true; }

    /*
     * operator->() - Returns the value pointer pointed to by this iterator
     *
     * Note that this function returns a constant pointer which can be used
     * to access members of the value, but cannot modify
     */
    inline const KeyValuePair *operator->() { return &*kv_pair; }

    /*
     * Postfix operator++ - Move the iterator ahead, and return the old one
     *
     * For end() iterator we do not do anything but return the same iterator
     */
    inline ForwardIterator operator++(int) { return *this; }
  };
};

}  // namespace terrier::storage::index
