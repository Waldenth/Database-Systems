//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// catalog_test.cpp
//
// Identification: test/catalog/catalog_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>
#include <unordered_set>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog.h"
#include "catalog/table_generator.h"
#include "concurrency/transaction_manager.h"
#include "execution/executor_context.h"
#include "gtest/gtest.h"
#include "storage/b_plus_tree_test_util.h"  // NOLINT
#include "type/value_factory.h"

namespace bustub {

TEST(CatalogTest, CreateTableTest) {
  auto disk_manager = new DiskManager("catalog_test.db");
  auto bpm = new BufferPoolManager(32, disk_manager);
  auto catalog = new Catalog(bpm, nullptr, nullptr);
  std::string table_name = "potato";

  // The table shouldn't exist in the catalog yet.
  EXPECT_THROW(catalog->GetTable(table_name), std::out_of_range);

  // Put the table into the catalog.
  std::vector<Column> columns;
  columns.emplace_back("A", TypeId::INTEGER);
  columns.emplace_back("B", TypeId::BOOLEAN);

  Schema schema(columns);
  auto *table_metadata = catalog->CreateTable(nullptr, table_name, schema);
  (void)table_metadata;

  // Notice that this test case doesn't check anything! :(
  // It is up to you to extend it

  delete catalog;
  delete bpm;
  delete disk_manager;
  remove("catalog_test.db");
  remove("catalog_test.log");
}

TEST(CatalogTest, GetTableTest) {
  auto disk_manager = new DiskManager("catalog_test.db");
  auto bpm = new BufferPoolManager(32, disk_manager);
  auto catalog = new Catalog(bpm, nullptr, nullptr);
  std::string table_name = "potato";

  // Put the table into the catalog.
  std::vector<Column> columns;
  columns.emplace_back("A", TypeId::INTEGER);
  columns.emplace_back("B", TypeId::BOOLEAN);

  Schema schema(columns);
  auto table_metadata = catalog->CreateTable(nullptr, table_name, schema);
  EXPECT_EQ(table_metadata->oid_, 0);

  auto retrieved_table_metadata = catalog->GetTable("potato");
  EXPECT_EQ(retrieved_table_metadata->name_, table_name);
  EXPECT_EQ(retrieved_table_metadata->oid_, 0);

  retrieved_table_metadata = catalog->GetTable(0);
  EXPECT_EQ(retrieved_table_metadata->name_, table_name);

  EXPECT_THROW(catalog->GetTable("tomato"), std::out_of_range);
  EXPECT_THROW(catalog->GetTable(1), std::out_of_range);

  delete catalog;
  delete bpm;
  delete disk_manager;
  remove("catalog_test.db");
  remove("catalog_test.log");
}

TEST(CatalogTest, CreateMultipleTableTest) {
  auto disk_manager = new DiskManager("catalog_test.db");
  auto bpm = new BufferPoolManager(32, disk_manager);
  auto catalog = new Catalog(bpm, nullptr, nullptr);
  std::string table_name = "potato";

  // Put the table into the catalog.
  std::vector<Column> columns;
  columns.emplace_back("A", TypeId::INTEGER);
  columns.emplace_back("B", TypeId::BOOLEAN);

  Schema schema(columns);

  auto table_metadata = catalog->CreateTable(nullptr, table_name, schema);
  EXPECT_EQ(table_metadata->oid_, 0);

  table_name = "tomato";
  table_metadata = catalog->CreateTable(nullptr, table_name, schema);
  EXPECT_EQ(table_metadata->oid_, 1);

  auto retrivied_table_metadata = catalog->GetTable("potato");
  EXPECT_EQ(retrivied_table_metadata->oid_, 0);
  retrivied_table_metadata = catalog->GetTable("tomato");
  EXPECT_EQ(retrivied_table_metadata->oid_, 1);

  EXPECT_EQ(catalog->GetTable("potato"), catalog->GetTable(0));
  EXPECT_EQ(catalog->GetTable("tomato"), catalog->GetTable(1));

  delete catalog;
  delete bpm;
  delete disk_manager;
  remove("catalog_test.db");
  remove("catalog_test.log");
}

TEST(CatalogTest, CreateIndexTest) {
  auto disk_manager = new DiskManager("catalog_test.db");
  auto bpm = new BufferPoolManager(32, disk_manager);
  auto catalog = new Catalog(bpm, nullptr, nullptr);

  // The index shouldn't exist in the catalog yet.
  EXPECT_THROW(catalog->GetIndex("foo", "potato"), std::out_of_range);

  // Put the table into the catalog.
  std::vector<Column> columns;
  columns.emplace_back("A", TypeId::INTEGER);
  columns.emplace_back("B", TypeId::BOOLEAN);

  Schema schema(columns);
  catalog->CreateTable(nullptr, "potato", schema);
  auto *index_info = catalog->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(nullptr, "foo", "potato", schema,
                                                                                    schema, {0, 1}, 8);
  (void)index_info;

  // Notice that this test case doesn't check anything! :(
  // It is up to you to extend it

  delete catalog;
  delete bpm;
  delete disk_manager;
  remove("catalog_test.db");
  remove("catalog_test.log");
}

TEST(CatalogTest, GetIndexTest) {
  auto disk_manager = new DiskManager("catalog_test.db");
  auto bpm = new BufferPoolManager(32, disk_manager);
  auto catalog = new Catalog(bpm, nullptr, nullptr);

  // Put the table into the catalog.
  std::vector<Column> columns;
  columns.emplace_back("A", TypeId::INTEGER);
  columns.emplace_back("B", TypeId::BOOLEAN);

  Schema schema(columns);
  catalog->CreateTable(nullptr, "potato", schema);
  auto index_info = catalog->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(nullptr, "foo", "potato", schema,
                                                                                   schema, {0, 1}, 8);
  EXPECT_EQ(index_info->index_oid_, 0);

  auto retrivied_index_info = catalog->GetIndex("foo", "potato");
  EXPECT_EQ(retrivied_index_info->name_, "foo");
  EXPECT_EQ(retrivied_index_info->table_name_, "potato");
  EXPECT_EQ(retrivied_index_info->index_oid_, 0);

  retrivied_index_info = catalog->GetIndex(0);
  EXPECT_EQ(retrivied_index_info->name_, "foo");
  EXPECT_EQ(retrivied_index_info->table_name_, "potato");
  EXPECT_EQ(retrivied_index_info->index_oid_, 0);

  EXPECT_THROW(catalog->GetIndex("bar", "potato"), std::out_of_range);
  EXPECT_THROW(catalog->GetTable(1), std::out_of_range);

  delete catalog;
  delete bpm;
  delete disk_manager;
  remove("catalog_test.db");
  remove("catalog_test.log");
}

TEST(CatalogTest, CreateMultipleIndexTest) {
  auto disk_manager = new DiskManager("catalog_test.db");
  auto bpm = new BufferPoolManager(32, disk_manager);
  auto catalog = new Catalog(bpm, nullptr, nullptr);

  // Put the table into the catalog.
  std::vector<Column> columns;
  columns.emplace_back("A", TypeId::INTEGER);
  columns.emplace_back("B", TypeId::BOOLEAN);

  Schema schema(columns);
  catalog->CreateTable(nullptr, "potato", schema);
  auto index_info = catalog->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(nullptr, "foo", "potato", schema,
                                                                                   schema, {0, 1}, 8);
  EXPECT_EQ(index_info->index_oid_, 0);

  index_info = catalog->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(nullptr, "bar", "potato", schema, schema,
                                                                              {0, 1}, 8);
  EXPECT_EQ(index_info->index_oid_, 1);

  auto retrivied_index_info = catalog->GetIndex("foo", "potato");
  EXPECT_EQ(retrivied_index_info->index_oid_, 0);

  retrivied_index_info = catalog->GetIndex("bar", "potato");
  EXPECT_EQ(retrivied_index_info->index_oid_, 1);

  EXPECT_EQ(catalog->GetIndex("foo", "potato"), catalog->GetIndex(0));
  EXPECT_EQ(catalog->GetIndex("bar", "potato"), catalog->GetIndex(1));

  auto indexes = catalog->GetTableIndexes("potato");
  EXPECT_EQ(indexes.size(), 2);
  EXPECT_TRUE((indexes[0]->name_ == "foo" && indexes[1]->name_ == "bar") ||
              (indexes[0]->name_ == "bar" || indexes[1]->name_ == "foo"));

  delete catalog;
  delete bpm;
  delete disk_manager;
  remove("catalog_test.db");
  remove("catalog_test.log");
}

TEST(CatalogTest, CreateIndexWithExistingDataTest) {
  auto disk_manager = std::make_unique<DiskManager>("catalog_test.db");
  auto bpm = std::make_unique<BufferPoolManager>(32, disk_manager.get());
  page_id_t page_id;
  bpm->NewPage(&page_id);
  auto lock_manager = std::make_unique<LockManager>();
  auto txn_mgr = std::make_unique<TransactionManager>(lock_manager.get(), nullptr);
  auto catalog = std::make_unique<Catalog>(bpm.get(), lock_manager.get(), nullptr);
  auto txn = txn_mgr->Begin();
  auto exec_ctx = std::make_unique<ExecutorContext>(txn, catalog.get(), bpm.get(), txn_mgr.get(), lock_manager.get());
  // Generate some test tables.
  TableGenerator gen{exec_ctx.get()};
  gen.GenerateTestTables();

  auto table_info = catalog->GetTable("test_1");
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);
  auto *index_info = catalog->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(
      txn, "index1", table_info->name_, table_info->schema_, *key_schema, {0}, 8);

  auto index_key = Tuple({ValueFactory::GetBigIntValue(50)}, key_schema);
  std::vector<RID> rids;
  index_info->index_->ScanKey(index_key, &rids, txn);
  EXPECT_EQ(rids.size(), 1);

  Tuple indexed_tuple;
  auto get_tuple_success = table_info->table_->GetTuple(rids[0], &indexed_tuple, txn);
  EXPECT_TRUE(get_tuple_success);
  EXPECT_EQ(indexed_tuple.GetValue(&table_info->schema_, table_info->schema_.GetColIdx("colA")).GetAs<int32_t>(), 50);

  txn_mgr->Commit(txn);
  disk_manager->ShutDown();
  remove("catalog_test.db");
  remove("catalog_test.log");
  delete txn;
  delete key_schema;
}

}  // namespace bustub
