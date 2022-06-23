#pragma once

#include <functional>
#include <iostream>

#include "loggers/index_logger.h"

template <typename... Args>
std::string string_format(const std::string &format, Args... args) {
  int size_s = std::snprintf(nullptr, 0, format.c_str(), args...) + 1;  // Extra space for '\0'
  if (size_s <= 0) {
    throw std::runtime_error("Error during formatting.");
  }
  auto size = static_cast<size_t>(size_s);
  std::unique_ptr<char[]> buf(new char[size]);
  std::snprintf(buf.get(), size, format.c_str(), args...);
  return std::string(buf.get(), buf.get() + size - 1);  // We don't want the '\0' inside
}

namespace terrier::storage::index {

const uint8_t TreeSummary = 0;
const uint8_t ExpandLeafNodes = 1;
const uint8_t ShowTupleContent = 2;
uint8_t VerboseLevel = 0;

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

  enum class NodeType {
    Leaf,
    Inner,
  };

  struct node;

  // struct Position {
  //   LeafNode *leaf;
  //   uint16_t slot;
  // }

  /*
   * The header structure of each node in-memory. This structure is extended
   * by InnerNode or LeafNode.
   */
  struct node {
    // Number of key slotuse use, so the number of valid children or data
    // pointers
    uint16_t slotused;

    NodeType type;

    node(NodeType type) : slotused(0), type(type) {}

    virtual ~node() = default;

    /**
     * Insert an item into the sub-tree rooted at this node.
     *
     * On node overflow happens, if a new_node which has the same level as this
     * node is created, it will be pack up to the caller by `new_node` parameter.
     *
     * @return The new node created on node overflow, and the split key which should
     * be inserted to the parent node. The new node is the right child of the split
     * key.
     */
    virtual std::pair<std::shared_ptr<node>, KeyType> Insert(const KeyType &key, const ValueType &value) {
      throw std::runtime_error("call virtual function: node::Insert");
    }

   public:
    virtual void PrintTree(uint8_t level = 0) const {}

    /**
     * Validate the structural integrity of the index data structure. If any of the integrity checks fail, then this
     * function should fix the integrity issue.
     */
    virtual void CheckIntegrity(KeyType *lower_bound, KeyType *upper_bound) {}

    bool IsLeaf() const { return type == NodeType::Leaf; }

    virtual void Search(const KeyType &key) const {}

   private:
    virtual std::string Outline(bool verbose = false) const {
      throw std::runtime_error("call virtual function: node::PrettyRepresentation");
    }
  };

  /*
   * Extended structure of a leaf node in memory. Contains pairs of keys and
   */
  struct LeafNode : public node {
    // Double linked list pointers to traverse the leaves
    // std::shared_ptr<LeafNode> right_sibling;
    LeafNode *right_sibling;

    // Double linked list pointers to traverse the leaves
    // std::shared_ptr<LeafNode> left_sibling;
    LeafNode *left_sibling;

    KeyType keys[leaf_slotmax];
    ValueType values[leaf_slotmax];

    explicit LeafNode() : node(NodeType::Leaf), right_sibling(nullptr), left_sibling(nullptr) {}

    ~LeafNode() = default;

    // True if the node's slots are full.
    bool IsFull() const { return (slotused == leaf_slotmax); }

    void InsertAt(const uint16_t position, const KeyType &key, const ValueType &value) {
      // INDEX_LOG_INFO("Inserting key: {} at position: {}", key, position);

      if (position > slotused) {
        INDEX_LOG_ERROR("Insertion position is greater than slotused");
        throw std::out_of_range("Insertion position is greater than slotused");
        return;
      }

      if (IsFull()) {
        INDEX_LOG_ERROR("Leaf node is full");
        throw std::out_of_range("Leaf node is full");
        return;
      }

      std::copy_backward(keys + position, keys + slotused, keys + slotused + 1);
      std::copy_backward(values + position, values + slotused, values + slotused + 1);
      keys[position] = key;
      values[position] = value;
      slotused++;
    }

    std::pair<std::shared_ptr<node>, KeyType> Insert(const KeyType &key, const ValueType &value) final {
      if (this->IsFull()) {
        // On page filled, create a new leaf page as right sibling and
        // move half of entries to it
        std::shared_ptr<LeafNode> new_right_sibling = std::make_shared<LeafNode>();

        uint16_t mid = this->slotused / 2;
        std::copy(this->keys + mid, this->keys + this->slotused, new_right_sibling->keys);
        std::copy(this->values + mid, this->values + this->slotused, new_right_sibling->values);

        new_right_sibling->slotused = this->slotused - mid;
        this->slotused = mid;

        KeyType split_key = new_right_sibling->keys[0];
        if (KeyComparator()(key, split_key)) {
          // Insert the old key in the old page.
          this->Insert(key, value);
        } else {
          // Insert the new key in the new page.
          new_right_sibling->Insert(key, value);
        }

        // Update the sibling pointers.
        if (this->right_sibling != nullptr) {
          this->right_sibling->left_sibling = new_right_sibling.get();
          new_right_sibling->right_sibling = this->right_sibling;
        }

        this->right_sibling = new_right_sibling.get();
        new_right_sibling->left_sibling = this;

        return std::make_pair(new_right_sibling, split_key);
      }

      uint16_t position = FindFirstGreaterOrEqual(key);
      this->InsertAt(position, key, value);

      return std::make_pair(nullptr, KeyType());
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

    void PrintTree(uint8_t level) const final {
      if (VerboseLevel < ExpandLeafNodes) {
        return;
      }

      std::string content = "";
      for (int i = 0; i < level; i++) {
        content += "│   ";
      }
      content += "├──";

      content += this->Outline(VerboseLevel >= ShowTupleContent);
      content += "\n";
      std::cout << content;
    }

    std::string Outline(bool verbose) const final {
      std::string repr =
          string_format("LeafNode (address: %p, slotused: %d (capacity: %d), ", this, slotused, leaf_slotmax);

      if (verbose) {
        repr += "contents: [";
        for (int i = 0; i < slotused; i++) {
          repr += string_format("%d, ", keys[i]);
        }
        repr += "])";
      } else {
        ValueType first_key = keys[0];
        ValueType last_key = keys[slotused - 1];
        repr += string_format("contents: [%d, ..., %d])", first_key, last_key);
      }

      return repr;
    }

    void CheckIntegrity(KeyType *lower_bound, KeyType *upper_bound) final {
      // Check order of keys.
      for (uint16_t i = 1; i < slotused; i++) {
        if (KeyComparator()(keys[i], keys[i - 1])) {
          INDEX_LOG_ERROR("LeafNode integrity check failed, keys are not sorted, key[{}] = {}, key[{}] = {}", i - 1,
                          keys[i - 1], i, keys[i]);
          throw std::out_of_range("LeafNode integrity check failed");
        }
      }

      // Check lower bound.
      KeyType first_key = keys[0];
      if (lower_bound != nullptr && KeyComparator()(first_key, *lower_bound)) {
        INDEX_LOG_ERROR(
            "LeafNode integrity check failed, first key is less than lower bound, key[0] = {}, lower_bound = {}",
            first_key, *lower_bound);
        throw std::out_of_range("LeafNode integrity check failed");
      }

      // Check upper bound.
      KeyType last_key = keys[slotused - 1];
      if (upper_bound != nullptr && KeyComparator()(*upper_bound, last_key)) {
        INDEX_LOG_ERROR(
            "LeafNode integrity check failed, last key is greater than upper bound, key[{}] = {}, upper_bound = {}",
            slotused - 1, last_key, *upper_bound);
        throw std::out_of_range("LeafNode integrity check failed");
      }
    }
  };

  struct InnerNode : public node {
    // Keys of children or data pointers
    KeyType keys[inner_slotmax];

    // Pointers to children
    std::shared_ptr<node> children[inner_slotmax + 1];

    // Use `array()` syntax to initialize all element of children to `nullptr`. This is necessary
    // to check is a child is valid or not.
    explicit InnerNode() : node(NodeType::Inner), children() {}

    ~InnerNode() = default;

    // True if the node's slots are full.
    bool IsFull() const { return (slotused == leaf_slotmax); }

    std::pair<std::shared_ptr<node>, KeyType> Insert(const KeyType &key, const ValueType &value) final {
      // stage 1 : find the proper child node to insert
      uint16_t child_position = FindFirstGreaterOrEqual(key);
      std::shared_ptr<node> child = this->children[child_position];

      // stage 2 : insert the key into the child node
      auto r = child->Insert(key, value);
      if (r.first != nullptr) {
        // On child split, insert the `new_node` to children list closed to `child`, and set `new_key` as the spliter
        // between them.

        if (this->IsFull()) {
          std::shared_ptr<InnerNode> new_right_sibling = std::make_shared<InnerNode>();

          uint16_t mid = this->slotused / 2;
          // Mve the keys start from `mid+1` to the new node, the key at `mid` will be popped up.
          std::copy(this->keys + mid + 1, this->keys + this->slotused, new_right_sibling->keys);
          // Move the children start from `mid+1` to the new node. The left child of `mid+1` was moved to the new node.
          std::copy(this->children + mid + 1, this->children + this->slotused + 1, new_right_sibling->children);

          // `-1` stands for the middle key of the original node, which will be popped up.
          new_right_sibling->slotused = this->slotused - mid - 1;
          this->slotused = mid;

          KeyType split_key = this->keys[mid];

          if (child_position <= mid) {
            this->InsertAt(child_position, r.second, std::move(r.first));
          } else {
            // `mid + 1` child node was left in the original node, so we have to minus `mid + 1` to get the correct
            // position.
            new_right_sibling->InsertAt(child_position - (mid + 1), r.second, std::move(r.first));
          }

          // INDEX_LOG_INFO("InnerNode split, split_key: {}", split_key);
          return std::make_pair(new_right_sibling, split_key);
        }

        // KeyType copy = *r.second;
        InsertAt(child_position, r.second, std::move(r.first));

        return std::make_pair(nullptr, KeyType());
      }
      return r;
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

    // Insert the key and its right child to the given position.
    void InsertAt(const uint16_t position, KeyType new_key, std::shared_ptr<node> right_child) {
      // INDEX_LOG_INFO("Inserting child node at position: {}", position);

      if (position > slotused) {
        INDEX_LOG_ERROR("Insertion position is greater than slotused");
        throw std::out_of_range("Insertion position is greater than slotused");
        return;
      }

      if (IsFull()) {
        INDEX_LOG_ERROR("Inner node is full");
        throw std::out_of_range("Inner node is full");
        return;
      }

      // Shift all keys from `position` right by one.
      std::copy_backward(keys + position, keys + slotused, keys + slotused + 1);
      keys[position] = new_key;

      // Shift all children from `position + 1` (i.e. the right child of `key`) right by one.
      std::copy_backward(children + position + 1, children + slotused + 1, children + slotused + 2);
      children[position + 1] = right_child;

      slotused++;
    }

    void PrintTree(uint8_t level) const final {
      std::string content = "";
      for (int i = 0; i < level; i++) {
        content += "│   ";
      }
      content += "├──";

      content += this->Outline(VerboseLevel > TreeSummary);
      content += "\n";
      std::cout << content;

      // Print children
      for (uint16_t i = 0; i <= slotused; i++) {
        if (children[i] != nullptr) {
          children[i]->PrintTree(level + 1);
        } else {
          content += string_format("error child at position %d, child is nullptr", i);
        }
      }
    }

    std::string Outline(bool verbose) const final {
      std::string repr = string_format("InnerNode (address: %p, slotused: %d, ", this, this->slotused);

      if (verbose) {
        repr += "keys: [";
        for (uint16_t i = 0; i < slotused; i++) {
          repr += string_format("%d, ", keys[i]);
        }
        repr += "])";
      } else {
        ValueType first_key = keys[0];
        ValueType last_key = keys[slotused - 1];
        repr += string_format("keys: [%d, ..., %d])", first_key, last_key);
      }

      return repr;
    }

    void CheckIntegrity(KeyType *lower_bound, KeyType *upper_bound) final {
      // Check if any child node is nullptr.
      for (uint16_t i = 0; i <= slotused; i++) {
        if (children[i] == nullptr) {
          INDEX_LOG_ERROR("Child node is nullptr, position: {}", i);
          throw std::out_of_range("Child node is nullptr");
        }
      }

      // Check order of keys.
      for (uint16_t i = 1; i < slotused; i++) {
        if (KeyComparator()(keys[i], keys[i - 1])) {
          INDEX_LOG_ERROR("InnerNode integrity check failed, keys are not sorted, key[{}] = {}, key[{}] = {}", i - 1,
                          keys[i - 1], i, keys[i]);
          throw std::out_of_range("InnerNode integrity check failed");
        }
      }

      // Check lower bound.
      KeyType first_key = keys[0];
      if (lower_bound != nullptr && KeyComparator()(first_key, *lower_bound)) {
        INDEX_LOG_ERROR(
            "InnerNode integrity check failed, first key is less than lower bound, key[0] = {}, lower_bound = {}",
            first_key, *lower_bound);
        throw std::out_of_range("InnerNode integrity check failed");
      }

      // Check upper bound.
      KeyType last_key = keys[slotused - 1];
      if (upper_bound != nullptr && KeyComparator()(*upper_bound, last_key)) {
        INDEX_LOG_ERROR(
            "InnerNode integrity check failed, last key is greater than upper bound, key[{}] = {}, upper_bound = {}",
            slotused - 1, last_key, *upper_bound);
        throw std::out_of_range("InnerNode integrity check failed");
      }

      // Recursively check children.
      children[0]->CheckIntegrity(lower_bound, &keys[0]);
      children[slotused]->CheckIntegrity(&keys[slotused - 1], upper_bound);
      for (uint16_t i = 1; i < slotused; i++) {
        children[i]->CheckIntegrity(&keys[i - 1], &keys[i]);
      }
    }
  };

 private:
  /*
   * Tree Object Data Members
   */

  // Pointer to the B+ tree's root node, either leaf or inner node.
  std::shared_ptr<node> root;

 private:
  std::shared_ptr<LeafNode> GetFirstLeaf() {
    if (root == nullptr) {
      return nullptr;
    }

    std::shared_ptr<node> current_node = root;
    while (!current_node->IsLeaf()) {
      current_node = std::static_pointer_cast<InnerNode>(current_node)->children[0];
    }
    return std::static_pointer_cast<LeafNode>(current_node);
  }

 public:
  // using KeyValuePair = std::pair<KeyType, ValueType>;
  struct KeyValue {
    KeyType key;
    ValueType value;

   public:
    KeyValue(KeyType key = KeyType(), ValueType value = ValueType()) : key(key), value(value) {}
  };

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
    // if (root != nullptr) {
    //   root->ClearChildren();
    //   delete root;
    // }
  }

  void CheckIntegrity() {
    if (root == nullptr) {
      INDEX_LOG_INFO("B+ Tree is empty");
      return;
    }

    try {
      root->CheckIntegrity(nullptr, nullptr);
    } catch (const std::exception &e) {
      PrintInnerStructure();
      throw e;
    }
  }

  void PrintInnerStructure() {
    std::string content = "\n";

    if (root == nullptr) {
      content += "Empty tree";
      std::cout << content;
      return;
    }

    content.append("B+ Tree Contents:\n");
    content.append("=================\n");
    std::cout << content;
    this->root->PrintTree();
  }

  /*
   * Insert() - Insert a key-value pair
   *
   * Note that this function also takes a unique_key argument, to indicate whether
   * we allow the same key with different values. For a primary key index this
   * should be set true. By default we allow non-unique key
   */
  void Insert(const KeyType &key, const ValueType &value, bool unique_key = false) {
    if (root == nullptr) {
      root = std::make_shared<LeafNode>();
    }

    try {
      auto r = root->Insert(key, value);
      std::shared_ptr<node> new_child = std::move(r.first);
      KeyType new_key = r.second;
      if (new_child != nullptr) {
        // This occurs when the root node is full and a new node had been
        // created, so we have to create a new root node and set the old root
        // and new child as its children.
        std::shared_ptr<InnerNode> new_root = std::make_shared<InnerNode>();
        new_root->keys[0] = new_key;

        new_root->children[0] = std::move(root);
        new_root->children[1] = new_child;

        new_root->slotused = 1;

        root = std::move(new_root);
      }
    } catch (const std::exception &e) {
      INDEX_LOG_ERROR("Exception while inserting key: {}, error: {}", key, e.what());
      this->PrintInnerStructure();
      throw e;
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
  ForwardIterator Begin() { return ForwardIterator{this->GetFirstLeaf().get()}; }

  /*
   * Begin() - Return an iterator using a given key
   *
   * The iterator returned will points to a data item whose key is greater than
   * or equal to the given start key. If such key does not exist then it will
   * be the smallest key that is greater than start_key
   */
  ForwardIterator Begin(const KeyType &start_key) { return ForwardIterator{this, start_key}; }

  /*
   * Iterator Interface
   */
  class ForwardIterator {
   private:
    LeafNode *current_leaf;
    uint16_t current_slot;

    KeyValue kv;

   public:
    ForwardIterator(LeafNode *leaf, uint16_t slot = 0) : current_leaf(leaf), current_slot(slot), kv() {}

    ForwardIterator(BPlusTree *tree, const KeyType &start_key) : current_leaf(nullptr), current_slot(0), kv() {
      if (tree->root == nullptr) {
        return;
      }
    }

    bool IsEnd() {
      if (current_leaf == nullptr) {
        return true;
      }

      if (current_leaf->right_sibling == nullptr && current_slot >= current_leaf->slotused) {
        return true;
      }

      return false;
    }

    /*
     * operator->() - Returns the value pointer pointed to by this iterator
     *
     * Note that this function returns a constant pointer which can be used
     * to access members of the value, but cannot modify
     */
    inline const KeyValue *operator->() {
      if (this->IsEnd()) {
        throw std::out_of_range("Iterator is at end");
      }

      kv.key = current_leaf->keys[current_slot];
      kv.value = current_leaf->values[current_slot];
      return &kv;
    }

    /*
     * Postfix operator++ - Move the iterator ahead, and return the old one
     *
     * For end() iterator we do not do anything but return the same iterator
     */
    inline ForwardIterator operator++(int) {
      if (this->IsEnd()) {
        throw std::out_of_range("Iterator is at end");
      }

      if (current_slot + 1 < current_leaf->slotused) {
        current_slot++;
      } else {
        current_leaf = current_leaf->right_sibling;
        current_slot = 0;
      }

      return *this;
    }
  };
};

}  // namespace terrier::storage::index
