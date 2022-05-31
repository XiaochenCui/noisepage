#pragma once

#include <functional>

#include "loggers/index_logger.h"

namespace terrier::storage::index {

// template <typename KeyType, typename ValueType, typename KeyComparator = std::less<KeyType>,
//           typename KeyEqualityChecker = std::equal_to<KeyType>, typename KeyHashFunc = std::hash<KeyType>,
//           typename ValueEqualityChecker = std::equal_to<ValueType>>
typedef int64_t KeyType;
typedef int64_t ValueType;
typedef std::less<KeyType> KeyComparator;
typedef std::equal_to<KeyType> KeyEqualityChecker;
typedef std::hash<KeyType> KeyHashFunc;
typedef std::equal_to<ValueType> ValueEqualityChecker;
class BPlusTree {
 private:
  /*
   * Static Constant Options and Values of the B+ Tree
   */

  // The number of key/data slots in each leaf node.
  static const uint16_t leaf_slotmax = 256;

  // The number of key slots in each inner node.
  static const uint16_t inner_slotmax = 256;

 private:
  /*
   * Node Classes for In-Memory Nodes
   */

  /*
   * The header structure of each node in-memory. This structure is extended
   * by InnerNode or LeafNode.
   */
  struct node {
    // Level in the b-tree, if level == 0 -> leaf node
    uint8_t level;

    // Number of key slotuse use, so the number of valid children or data
    // pointers
    uint16_t slotused;

    node(const uint8_t l) : slotused(0) { level = l; }

    // True if this is a leaf node.
    bool IsLeafPage() const { return (level == 0); }
  };

  /*
   * Extended structure of a leaf node in memory. Contains pairs of keys and
   */
  struct LeafNode : public node {
    // Double linked list pointers to traverse the leaves
    LeafNode *right_sibling;

    // Double linked list pointers to traverse the leaves
    LeafNode *left_sibling;

    KeyType keys[leaf_slotmax];
    ValueType values[leaf_slotmax];

    explicit LeafNode() : node(0), right_sibling(nullptr), left_sibling(nullptr) {}

    // True if the node's slots are full.
    bool IsFull() const { return (node::slotused == leaf_slotmax); }

    void InsertAt(const uint16_t position, const KeyType &key, const ValueType &value) {
      INDEX_LOG_INFO("Inserting key: {} at position: {}", key, position);

      if (position > node::slotused) {
        INDEX_LOG_ERROR("Insertion position is greater than slotused");
        throw std::out_of_range("Insertion position is greater than slotused");
        return;
      }

      if (IsFull()) {
        INDEX_LOG_ERROR("Leaf node is full");
        throw std::out_of_range("Leaf node is full");
        return;
      }

      std::copy_backward(keys + position, keys + node::slotused, keys + node::slotused + 1);
      std::copy_backward(values + position, values + node::slotused, values + node::slotused + 1);
      keys[position] = key;
      values[position] = value;
      node::slotused++;
    }
  };

  struct InnerNode : public node {
    // Keys of children or data pointers
    KeyType keys[inner_slotmax];

    // Pointers to children
    node *children[inner_slotmax + 1];

    explicit InnerNode() : node(0) {}
  };

 private:
  /*
   * Tree Object Data Members
   */

  // Pointer to the B+ tree's root node, either leaf or inner node.
  node *root_;

 public:
  using KeyValuePair = std::pair<KeyType, ValueType>;

  /*
   * Default constructor initializing an empty B+ tree.
   */
  explicit BPlusTree() : root_(nullptr) {
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

  /*
   * Insert() - Insert a key-value pair
   *
   * Note that this function also takes a unique_key argument, to indicate whether
   * we allow the same key with different values. For a primary key index this
   * should be set true. By default we allow non-unique key
   */
  void Insert(const KeyType &key, const ValueType &value, bool unique_key = false) {
    INDEX_LOG_INFO("Inserting key: {}", key);
    if (root_ == nullptr) {
      root_ = new LeafNode();
    }

    InsertDescend(root_, key, value);
    return;
  }

  /*
   * Insert an item into the B+ tree.
   *
   * Descend down the nodes to a leaf, insert the key/data pair in a free
   * slot. If the node overflows, then it must be split and the new split node
   * inserted into the parent. Unroll / this splitting up to the root.
   */
  void InsertDescend(node *n, const KeyType &key, const ValueType &value) {
    if (n->IsLeafPage()) {
      LeafNode *leaf = static_cast<LeafNode *>(n);

      if (leaf->IsFull()) {
        // On page filled, create a new leaf page as right sibling and
        // move half of entries to it
        LeafNode *new_right_sibling = new LeafNode();

        uint16_t mid = leaf->slotused / 2;
        std::copy(leaf->keys + mid, leaf->keys + leaf->slotused, new_right_sibling->keys);
        std::copy(leaf->values + mid, leaf->values + leaf->slotused, new_right_sibling->values);

        KeyType split_key = new_right_sibling->keys[0];
        if (KeyComparator{}(key, split_key)) {
          // Insert the old key in the old leaf page
          leaf->InsertAt(mid, split_key, leaf->values[mid]);
        } else {
          // Insert the new key in the new leaf page
          new_right_sibling->InsertAt(0, key, value);
        }

        InnerNode *new_parent = new InnerNode();
      }

      uint16_t position = FindFirstGreaterOrEqual(leaf, key);
      leaf->InsertAt(position, key, value);

      return;
    }
    throw std::logic_error("Not implemented");
  }

  uint16_t FindFirstGreaterOrEqual(LeafNode *leaf, const KeyType &key) {
    if (leaf->slotused == 0) {
      return 0;
    }

    for (uint16_t i = 0; i < leaf->slotused; i++) {
      if (KeyComparator{}(key, leaf->keys[i])) {
        return i;
      }
    }
    return leaf->slotused;
  }

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
