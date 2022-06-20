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

  /*
   * The header structure of each node in-memory. This structure is extended
   * by InnerNode or LeafNode.
   */
  struct node {
    // Number of key slotuse use, so the number of valid children or data
    // pointers
    uint16_t slotused;

    node() : slotused(0) {}

    virtual ~node() = default;

    /**
     * Insert an item into the sub-tree rooted at this node.
     *
     * On node overflow happens, if a new_node which has the same level as this
     * node is created, it will be pack up to the caller by `new_node` parameter.
     *
     * @param new_node The new node created by the split, we pass the argument as
     * "reference to pointer" to return the newly created node to the caller.
     */
    virtual std::pair<node *, KeyType *> Insert(const KeyType &key, const ValueType &value) {
      throw std::runtime_error("call virtual function: node::Insert");
    }

    /**
     * Recursively free up nodes.
     */
    virtual void ClearChildren() { throw std::runtime_error("call virtual function: node::ClearChildren"); }

   public:
    virtual void PrintTree(uint8_t level = 0) const {}

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
    LeafNode *right_sibling;

    // Double linked list pointers to traverse the leaves
    LeafNode *left_sibling;

    KeyType keys[leaf_slotmax];
    ValueType values[leaf_slotmax];

    explicit LeafNode() : node(), right_sibling(nullptr), left_sibling(nullptr) {}

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

    std::pair<node *, KeyType *> Insert(const KeyType &key, const ValueType &value) override {
      if (this->IsFull()) {
        // On page filled, create a new leaf page as right sibling and
        // move half of entries to it
        LeafNode *new_right_sibling = new LeafNode();

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

        return std::make_pair(new_right_sibling, &split_key);
      }

      uint16_t position = FindFirstGreaterOrEqual(key);
      this->InsertAt(position, key, value);

      return std::make_pair(nullptr, nullptr);
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

    void PrintTree(uint8_t level) const final {
      if (VerboseLevel < ExpandLeafNodes) {
        return;
      }

      std::string content = "";
      for (int i = 0; i < level; i++) {
        content += "│   ";
      }
      content += "├──";

      bool verbose = VerboseLevel > ShowTupleContent ? true : false;
      content += this->Outline(verbose);
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
  };

  struct InnerNode : public node {
    // Keys of children or data pointers
    KeyType keys[inner_slotmax];

    // Pointers to children
    node *children[inner_slotmax + 1];

    // Use `array()` syntax to initialize the array in the constructor, otherwise
    // an array will not be initialised by default
    explicit InnerNode() : node(), children() {}

    ~InnerNode() = default;

    // True if the node's slots are full.
    bool IsFull() const { return (slotused == leaf_slotmax); }

    std::pair<node *, KeyType *> Insert(const KeyType &key, const ValueType &value) override {
      // stage 1 : find the proper child node to insert
      node *child = nullptr;
      uint16_t child_position = 0;
      for (child_position = 0; child_position < slotused; child_position++) {
        if (KeyComparator()(key, this->keys[child_position])) {
          child = this->children[child_position];
          break;
        }
      }
      if (child == nullptr) {
        child = this->children[slotused];
      }

      if (child == nullptr) {
        throw std::runtime_error("child is nullptr");
      }

      // stage 2 : insert the key into the child node
      auto r = child->Insert(key, value);
      if (r.first != nullptr) {
        // On child split, insert the `new_node` to children list closed to `child`, and set `new_key` as the spliter
        // between them.

        if (this->IsFull()) {
          InnerNode *new_right_sibling = new InnerNode();

          uint16_t mid = this->slotused / 2;
          // Mve the keys start from `mid+1` to the new node, the key at `mid` will be popped up.
          std::copy(this->keys + mid + 1, this->keys + this->slotused, new_right_sibling->keys);
          // Move the children start from `mid+1` to the new node. The left child of `mid+1` was moved to the new node.
          std::copy(this->children + mid + 1, this->children + this->slotused + 1, new_right_sibling->children);

          // `-1` stands for the middle key of the original node, which will be popped up.
          new_right_sibling->slotused = this->slotused - mid - 1;
          this->slotused = mid;

          KeyType split_key = new_right_sibling->keys[0];
          if (KeyComparator()(key, split_key)) {
            // Insert the old key in the old page.
            this->Insert(key, value);
          } else {
            // Insert the new key in the new page.
            new_right_sibling->Insert(key, value);
          }
          return std::make_pair(new_right_sibling, &split_key);
        }

        // The new child is the new right sibling of the `child`.
        {
          // The target position to insert the new key is `child_position`.
          std::copy_backward(keys + child_position, keys + slotused, keys + slotused + 1);
          keys[child_position] = *r.second;

          // The target position to insert the new child is `child_position + 1`.
          std::copy_backward(children + child_position, children + slotused + 1, children + slotused + 2);
          children[child_position + 1] = r.first;

          // Increase the slot used.
          slotused++;
        }

        return std::make_pair(nullptr, nullptr);
      }
      return r;
    }

    void InsertAt(const uint16_t position, node *new_child) {
      INDEX_LOG_INFO("Inserting child node at position: {}", position);

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

      std::copy_backward(keys + position, keys + slotused, keys + slotused + 1);
      std::copy_backward(children + position, children + slotused + 1, children + slotused + 2);
    }

    /**
     * Recursively free up nodes.
     */
    void ClearChildren() final {
      // Clear children recursively.
      for (uint16_t i = 0; i <= slotused; i++) {
        if (children[i] != nullptr) {
          children[i]->ClearChildren();
          delete children[i];
          children[i] = nullptr;
        }
      }

      // Clear keys.
      
    }

    void PrintTree(uint8_t level) const final {
      std::string content = "";
      for (int i = 0; i < level; i++) {
        content += "│   ";
      }
      content += "├──";

      content += this->Outline(true);
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
      std::string repr = string_format("InnerNode (address: %p, slotused: %d, keys: [", this, this->slotused);
      for (uint16_t i = 0; i < slotused; i++) {
        repr += string_format("%d, ", keys[i]);
      }
      repr += "])";
      return repr;
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
    // INDEX_LOG_INFO("Inserting key: {}", key);
    if (root == nullptr) {
      root = new LeafNode();
    }

    node *new_child = nullptr;
    KeyType *new_key = nullptr;

    try {
      auto r = root->Insert(key, value);
      new_child = r.first;
      new_key = r.second;
    } catch (const std::exception &e) {
      INDEX_LOG_ERROR("Exception while inserting key: {}, error: {}", key, e.what());
      this->PrintInnerStructure();
      throw e;
    }

    if (new_child != nullptr) {
      // This occurs when the root node is full and a new node had been
      // created, so we have to create a new root node and set the old root
      // and new child as its children.
      INDEX_LOG_INFO("New child created");
      InnerNode *new_root = new InnerNode();
      new_root->keys[0] = *new_key;

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
