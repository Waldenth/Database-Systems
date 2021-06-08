#pragma once

#include <algorithm>
#include <iterator>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/schema.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/index/index.h"
#include "storage/table/table_heap.h"

namespace bustub {

/**
 * Typedefs
 */
using table_oid_t = uint32_t;
using column_oid_t = uint32_t;
using index_oid_t = uint32_t;

/**
 * Metadata about a table.
 */
struct TableMetadata {
  TableMetadata(Schema schema, std::string name, std::unique_ptr<TableHeap> &&table, table_oid_t oid)
      : schema_(std::move(schema)), name_(std::move(name)), table_(std::move(table)), oid_(oid) {}
  Schema schema_;
  std::string name_;
  std::unique_ptr<TableHeap> table_;
  table_oid_t oid_;
};

/**
 * Metadata about a index
 */
struct IndexInfo {
  IndexInfo(Schema key_schema, std::string name, std::unique_ptr<Index> &&index, index_oid_t index_oid,
            std::string table_name, size_t key_size)
      : key_schema_(std::move(key_schema)),
        name_(std::move(name)),
        index_(std::move(index)),
        index_oid_(index_oid),
        table_name_(std::move(table_name)),
        key_size_(key_size) {}
  Schema key_schema_;
  std::string name_;
  std::unique_ptr<Index> index_;
  index_oid_t index_oid_;
  std::string table_name_;
  const size_t key_size_;
};

/**
 * Catalog is a non-persistent catalog that is designed for the executor to use.
 * It handles table creation and table lookup.
 */
class Catalog {
 public:
  /**
   * Creates a new catalog object.
   * @param bpm the buffer pool manager backing tables created by this catalog
   * @param lock_manager the lock manager in use by the system
   * @param log_manager the log manager in use by the system
   */
  Catalog(BufferPoolManager *bpm, LockManager *lock_manager, LogManager *log_manager)
      : bpm_{bpm}, lock_manager_{lock_manager}, log_manager_{log_manager} {}

  /**
   * Create a new table and return its metadata.
   * @param txn the transaction in which the table is being created
   * @param table_name the name of the new table
   * @param schema the schema of the new table
   * @return a pointer to the metadata of the new table
   */
  TableMetadata *CreateTable(Transaction *txn, const std::string &table_name, const Schema &schema) {
    BUSTUB_ASSERT(names_.count(table_name) == 0, "Table names should be unique!");

    std::unique_ptr<TableHeap> table_heap = std::make_unique<TableHeap>(bpm_, lock_manager_, log_manager_, txn);
    std::unique_ptr<TableMetadata> table_meta_data =
        std::make_unique<TableMetadata>(schema, table_name, std::move(table_heap), next_table_oid_);

    auto result = table_meta_data.get();

    tables_.emplace(result->oid_, std::move(table_meta_data));
    names_.emplace(result->name_, result->oid_);

    ++next_table_oid_;
    return result;
  }

  /** @return table metadata by name */
  TableMetadata *GetTable(const std::string &table_name) {
    auto name_it = names_.find(table_name);
    if (name_it == names_.end()) {
      throw std::out_of_range("cannot find table by table name");
    }

    return GetTable(name_it->second);
  }

  /** @return table metadata by oid */
  TableMetadata *GetTable(table_oid_t table_oid) {
    auto table_it = tables_.find(table_oid);
    if (table_it == tables_.end()) {
      throw std::out_of_range("cannot find table by table oid");
    }

    return table_it->second.get();
  }

  /**
   * Create a new index, populate existing data of the table and return its metadata.
   * @param txn the transaction in which the index is being created
   * @param index_name the name of the new index
   * @param table_name the name of the table
   * @param schema the schema of the table
   * @param key_schema the schema of the key
   * @param key_attrs key attributes
   * @param keysize size of the key
   * @return a pointer to the metadata of the new index
   */
  template <class KeyType, class ValueType, class KeyComparator>
  IndexInfo *CreateIndex(Transaction *txn, const std::string &index_name, const std::string &table_name,
                         const Schema &schema, const Schema &key_schema, const std::vector<uint32_t> &key_attrs,
                         size_t keysize) {
    std::unique_ptr<IndexMetadata> index_meta_data =
        std::make_unique<IndexMetadata>(std::string(index_name), std::string(table_name), &schema, key_attrs);
    std::unique_ptr<Index> index = std::make_unique<BPLUSTREE_INDEX_TYPE>(index_meta_data.release(), bpm_);
    std::unique_ptr<IndexInfo> index_info = std::make_unique<IndexInfo>(
        key_schema, std::string(index_name), std::move(index), next_index_oid_, std::string(table_name), keysize);

    auto result = index_info.get();

    indexes_.emplace(result->index_oid_, std::move(index_info));
    index_names_[result->table_name_].emplace(result->name_, result->index_oid_);
    ++next_index_oid_;

    // populate existing data of the table
    auto table_meta_data = GetTable(result->table_name_);
    auto table_heap = table_meta_data->table_.get();
    for (auto it = table_heap->Begin(txn); it != table_heap->End(); ++it) {
      result->index_->InsertEntry(it->KeyFromTuple(schema, result->key_schema_, result->index_->GetKeyAttrs()),
                                  it->GetRid(), txn);
    }

    return result;
  }

  IndexInfo *GetIndex(const std::string &index_name, const std::string &table_name) {
    auto index_name_it = index_names_.find(table_name);
    if (index_name_it == index_names_.end()) {
      throw std::out_of_range("cannot find index by table name");
    }

    auto inner_map = index_name_it->second;
    auto inner_it = inner_map.find(index_name);
    if (inner_it == inner_map.end()) {
      throw std::out_of_range("cannot find index by index name");
    }

    return GetIndex(inner_it->second);
  }

  IndexInfo *GetIndex(index_oid_t index_oid) {
    auto index_it = indexes_.find(index_oid);
    if (index_it == indexes_.end()) {
      throw std::out_of_range("cannot find index by index oid");
    }

    return index_it->second.get();
  }

  std::vector<IndexInfo *> GetTableIndexes(const std::string &table_name) {
    std::vector<IndexInfo *> result;

    auto index_name_it = index_names_.find(table_name);
    if (index_name_it == index_names_.end()) {
      return result;
    }

    auto inner_map = index_name_it->second;
    std::transform(inner_map.begin(), inner_map.end(), std::back_inserter(result),
                   [&idx_map = indexes_](auto pair) { return idx_map[pair.second].get(); });
    return result;
  }

 private:
  [[maybe_unused]] BufferPoolManager *bpm_;
  [[maybe_unused]] LockManager *lock_manager_;
  [[maybe_unused]] LogManager *log_manager_;

  /** tables_ : table identifiers -> table metadata. Note that tables_ owns all table metadata. */
  std::unordered_map<table_oid_t, std::unique_ptr<TableMetadata>> tables_;
  /** names_ : table names -> table identifiers */
  std::unordered_map<std::string, table_oid_t> names_;
  /** The next table identifier to be used. */
  std::atomic<table_oid_t> next_table_oid_{0};
  /** indexes_: index identifiers -> index metadata. Note that indexes_ owns all index metadata */
  std::unordered_map<index_oid_t, std::unique_ptr<IndexInfo>> indexes_;
  /** index_names_: table name -> index names -> index identifiers */
  std::unordered_map<std::string, std::unordered_map<std::string, index_oid_t>> index_names_;
  /** The next index identifier to be used */
  std::atomic<index_oid_t> next_index_oid_{0};
};
}  // namespace bustub
