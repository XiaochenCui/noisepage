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

    virtual ~node() = default;

    // True if this is a leaf node.
    bool IsLeafPage() const { return (level == 0); }

    /**
     * Insert an item into the sub-tree rooted at this node.
     *
     * On node overflow happens, if a new_node which has the same level as this
     * node is created, it will be pack up to the caller by `new_node` parameter.
     *
     * @param new_node The new node created by the split, we pass the argument as
     * "reference to pointer" to return the newly created node to the caller.
     */
    virtual void Insert(const KeyType &key, const ValueType &value, KeyType &new_key, node *&new_node) {
      throw std::runtime_error("call virtual function: node::Insert");
    }

    /**
     * Recursively free up nodes.
     */
    virtual void ClearChildren() { throw std::runtime_error("call virtual function: node::ClearChildren"); }
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

    ~LeafNode() = default;

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

    void Insert(const KeyType &key, const ValueType &value, KeyType &new_key, node *&new_node) override {
      if (this->IsFull()) {
        // On page filled, create a new leaf page as right sibling and
        // move half of entries to it
        LeafNode *new_right_sibling = new LeafNode();

        uint16_t mid = this->slotused / 2;
        std::copy(this->keys + mid, this->keys + this->slotused, new_right_sibling->keys);
        std::copy(this->values + mid, this->values + this->slotused, new_right_sibling->values);

        this->slotused = mid;

        KeyType split_key = new_right_sibling->keys[0];
        if (KeyComparator()(key, split_key)) {
          // Insert the old key in the old this page
          this->InsertAt(mid, split_key, this->values[mid]);
        } else {
          // Insert the new key in the new this page
          new_right_sibling->InsertAt(0, key, value);
        }

        new_node = new_right_sibling;
        return;
      }

      uint16_t position = FindFirstGreaterOrEqual(key);
      this->InsertAt(position, key, value);

      return;
    }

    uint16_t FindFirstGreaterOrEqual(const KeyType &key) {
      if (this->slotused == 0) {
        return 0;
      }

      for (uint16_t i = 0; i < this->slotused; i++) {
        if (KeyComparator()(key, this->keys[i])) {
          return i;
        }
      }
      return this->slotused;
    }

    /**
     * Recursively free up nodes.
     *
     * Do nothing since leaf node doesn't have any children
     */
    void ClearChildren() {}
  };

  struct InnerNode : public node {
    // Keys of children or data pointers
    KeyType keys[inner_slotmax];

    // Pointers to children
    node *children[inner_slotmax + 1];

    // Use `array()` syntax to initialize the array in the constructor, otherwise
    // an array will not be initialised by default
    explicit InnerNode(const uint8_t level) : node(level), children() {}

    ~InnerNode() = default;

    // True if the node's slots are full.
    bool IsFull() const { return (node::slotused == leaf_slotmax); }

    void Insert(const KeyType &key, const ValueType &value, KeyType &new_key, node *&new_node) override {
      // stage 1 : find the proper child node to insert
      node *child = nullptr;
      for (uint16_t i = 0; i < node::slotused; i++) {
        if (KeyComparator()(key, this->keys[i])) {
          child = this->children[i];
          break;
        }
      }
      if (child == nullptr) {
        child = this->children[node::slotused];
      }

      if (child == nullptr) {
        throw std::runtime_error("child is nullptr");
      }

      // stage 2 : insert the key into the child node
      child->Insert(key, value, new_key, new_node);
      if (new_node != nullptr) {
        // On child split, insert the `new_node` to children list closed to `child`, and set `new_key` as the spliter
        // between them.
        throw std::runtime_error("not implemented");
      }
    }

    void InsertAt(const uint16_t position, node *new_child) {
      INDEX_LOG_INFO("Inserting node: {} at position: {}", new_child, position);

      if (position > node::slotused) {
        INDEX_LOG_ERROR("Insertion position is greater than slotused");
        throw std::out_of_range("Insertion position is greater than slotused");
        return;
      }

      if (IsFull()) {
        INDEX_LOG_ERROR("Inner node is full");
        throw std::out_of_range("Inner node is full");
        return;
      }

      std::copy_backward(keys + position, keys + node::slotused, keys + node::slotused + 1);
      std::copy_backward(children + position, children + node::slotused + 1, children + node::slotused + 2);

      // std::copy_backward(inner->slotkey + slot, inner->slotkey + inner->slotuse, inner->slotkey + inner->slotuse +
      // 1); std::copy_backward(inner->childid + slot, inner->childid + inner->slotuse + 1,
      //                    inner->childid + inner->slotuse + 2);

      // inner->slotkey[slot] = newkey;
      // inner->childid[slot + 1] = newchild;
      // inner->slotuse++;
    }

    /**
     * Recursively free up nodes.
     */
    void ClearChildren() {
      for (uint16_t i = 0; i < node::slotused; i++) {
        if (children[i] != nullptr) {
          children[i]->ClearChildren();
          delete children[i];
          children[i] = nullptr;
        }
      }
    }
  };

 private:
  /*
   * Tree Object Data Members
   */

  // Pointer to the B+ tree's root node, either leaf or inner node.
  node *root;

 public:
  using KeyValuePair = std::pair<KeyType, ValueType>;

  /*
   * Default constructor initializing an empty B+ tree.
   */
  explicit BPlusTree() : root(nullptr) {
    INDEX_LOG_INFO(
        "B+ Tree Constructor called. "
        "Setting up execution environment...");
  }

  ~BPlusTree() {
    INDEX_LOG_INFO(
        "B+ Tree Destructor called. "
        "Cleaning up execution environment...");
    if (root != nullptr) {
      root->ClearChildren();
      delete root;
    }
  }

  void PrintInnerStructure() { INDEX_LOG_INFO("B+ Tree Contents:"); }

  /*
   * Insert() - Insert a key-value pair
   *
   * Note that this function also takes a unique_key argument, to indicate whether
   * we allow the same key with different values. For a primary key index this
   * should be set true. By default we allow non-unique key
   */
  void Insert(const KeyType &key, const ValueType &value, bool unique_key = false) {
    INDEX_LOG_INFO("Inserting key: {}", key);
    if (root == nullptr) {
      root = new LeafNode();
    }

    node *new_child = nullptr;
    KeyType new_key = KeyType();

    try {
      root->Insert(key, value, new_key, new_child);
    } catch (const std::exception &e) {
      INDEX_LOG_ERROR("Exception while inserting key: {}, error: {}", key, e.what());
      throw e;
    }

    if (new_child != nullptr) {
      // This occurs when the root node is full and a new node had been
      // created, so we have to create a new root node and set the old root
      // and new child as its children.
      INDEX_LOG_INFO("New child created");
      InnerNode *new_root = new InnerNode(root->level + 1);
      new_root->keys[0] = new_key;

      new_root->children[0] = root;
      new_root->children[1] = new_child;

      new_root->slotused = 1;

      root = new_root;
    }
    return;
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
