/**
 * transaction_test.cpp
 */

#include <atomic>
#include <cstdio>
#include <memory>
#include <random>
#include <string>
#include <thread>  // NOLINT
#include <unordered_map>
#include <utility>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/table_generator.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_engine.h"
#include "execution/executor_context.h"
#include "execution/executors/delete_executor.h"
#include "execution/executors/insert_executor.h"
#include "execution/executors/update_executor.h"
#include "execution/expressions/aggregate_value_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/delete_plan.h"
#include "execution/plans/insert_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/nested_index_join_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/update_plan.h"
#include "gtest/gtest.h"
#include "storage/b_plus_tree_test_util.h"  // NOLINT
#include "type/value_factory.h"

#define TEST_TIMEOUT_BEGIN                           \
  std::promise<bool> promisedFinished;               \
  auto futureResult = promisedFinished.get_future(); \
                              std::thread([](std::promise<bool>& finished) {
#define TEST_TIMEOUT_FAIL_END(X)                                                                  \
  finished.set_value(true);                                                                       \
  }, std::ref(promisedFinished)).detach();                                                        \
  EXPECT_TRUE(futureResult.wait_for(std::chrono::milliseconds(X)) != std::future_status::timeout) \
      << "Test Failed Due to Time Out";

namespace bustub {

class TransactionTest : public ::testing::Test {
 public:
  // This function is called before every test.
  void SetUp() override {
    ::testing::Test::SetUp();
    // For each test, we create a new DiskManager, BufferPoolManager, TransactionManager, and Catalog.
    disk_manager_ = std::make_unique<DiskManager>("executor_test.db");
    bpm_ = std::make_unique<BufferPoolManager>(2560, disk_manager_.get());
    page_id_t page_id;
    bpm_->NewPage(&page_id);
    lock_manager_ = std::make_unique<LockManager>();
    txn_mgr_ = std::make_unique<TransactionManager>(lock_manager_.get(), log_manager_.get());
    catalog_ = std::make_unique<Catalog>(bpm_.get(), lock_manager_.get(), log_manager_.get());
    // Begin a new transaction, along with its executor context.
    txn_ = txn_mgr_->Begin();
    exec_ctx_ =
        std::make_unique<ExecutorContext>(txn_, catalog_.get(), bpm_.get(), txn_mgr_.get(), lock_manager_.get());
    // Generate some test tables.
    TableGenerator gen{exec_ctx_.get()};
    gen.GenerateTestTables();

    execution_engine_ = std::make_unique<ExecutionEngine>(bpm_.get(), txn_mgr_.get(), catalog_.get());
  }

  // This function is called after every test.
  void TearDown() override {
    // Commit our transaction.
    txn_mgr_->Commit(txn_);
    // Shut down the disk manager and clean up the transaction.
    disk_manager_->ShutDown();
    remove("executor_test.db");
    remove("executor_test.log");
    delete txn_;
  };

  /** @return the executor context in our test class */
  ExecutorContext *GetExecutorContext() { return exec_ctx_.get(); }
  ExecutionEngine *GetExecutionEngine() { return execution_engine_.get(); }
  Transaction *GetTxn() { return txn_; }
  TransactionManager *GetTxnManager() { return txn_mgr_.get(); }
  Catalog *GetCatalog() { return catalog_.get(); }
  BufferPoolManager *GetBPM() { return bpm_.get(); }
  LockManager *GetLockManager() { return lock_manager_.get(); }

  // The below helper functions are useful for testing.

  const AbstractExpression *MakeColumnValueExpression(const Schema &schema, uint32_t tuple_idx,
                                                      const std::string &col_name) {
    uint32_t col_idx = schema.GetColIdx(col_name);
    auto col_type = schema.GetColumn(col_idx).GetType();
    allocated_exprs_.emplace_back(std::make_unique<ColumnValueExpression>(tuple_idx, col_idx, col_type));
    return allocated_exprs_.back().get();
  }

  const AbstractExpression *MakeConstantValueExpression(const Value &val) {
    allocated_exprs_.emplace_back(std::make_unique<ConstantValueExpression>(val));
    return allocated_exprs_.back().get();
  }

  const AbstractExpression *MakeComparisonExpression(const AbstractExpression *lhs, const AbstractExpression *rhs,
                                                     ComparisonType comp_type) {
    allocated_exprs_.emplace_back(std::make_unique<ComparisonExpression>(lhs, rhs, comp_type));
    return allocated_exprs_.back().get();
  }

  const AbstractExpression *MakeAggregateValueExpression(bool is_group_by_term, uint32_t term_idx) {
    allocated_exprs_.emplace_back(
        std::make_unique<AggregateValueExpression>(is_group_by_term, term_idx, TypeId::INTEGER));
    return allocated_exprs_.back().get();
  }

  const Schema *MakeOutputSchema(const std::vector<std::pair<std::string, const AbstractExpression *>> &exprs) {
    std::vector<Column> cols;
    cols.reserve(exprs.size());
    for (const auto &input : exprs) {
      if (input.second->GetReturnType() != TypeId::VARCHAR) {
        cols.emplace_back(input.first, input.second->GetReturnType(), input.second);
      } else {
        cols.emplace_back(input.first, input.second->GetReturnType(), MAX_VARCHAR_SIZE, input.second);
      }
    }
    allocated_output_schemas_.emplace_back(std::make_unique<Schema>(cols));
    return allocated_output_schemas_.back().get();
  }

 private:
  std::unique_ptr<TransactionManager> txn_mgr_;
  Transaction *txn_{nullptr};
  std::unique_ptr<DiskManager> disk_manager_;
  std::unique_ptr<LogManager> log_manager_ = nullptr;
  std::unique_ptr<LockManager> lock_manager_;
  std::unique_ptr<BufferPoolManager> bpm_;
  std::unique_ptr<Catalog> catalog_;
  std::unique_ptr<ExecutorContext> exec_ctx_;
  std::unique_ptr<ExecutionEngine> execution_engine_;
  std::vector<std::unique_ptr<AbstractExpression>> allocated_exprs_;
  std::vector<std::unique_ptr<Schema>> allocated_output_schemas_;
  static constexpr uint32_t MAX_VARCHAR_SIZE = 128;
};

// --- Helper functions ---
void CheckGrowing(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::GROWING); }

void CheckShrinking(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::SHRINKING); }

void CheckAborted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::ABORTED); }

void CheckCommitted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::COMMITTED); }

void CheckTxnLockSize(Transaction *txn, size_t shared_size, size_t exclusive_size) {
  EXPECT_EQ(txn->GetSharedLockSet()->size(), shared_size);
  EXPECT_EQ(txn->GetExclusiveLockSet()->size(), exclusive_size);
}

// NOLINTNEXTLINE
TEST_F(TransactionTest, SimpleInsertRollbackTest) {
  // txn1: INSERT INTO empty_table2 VALUES (200, 20), (201, 21), (202, 22)
  // txn1: abort
  // txn2: SELECT * FROM empty_table2;
  // Create Values to insert
  std::vector<Value> val1{ValueFactory::GetIntegerValue(200), ValueFactory::GetIntegerValue(20)};
  std::vector<Value> val2{ValueFactory::GetIntegerValue(201), ValueFactory::GetIntegerValue(21)};
  std::vector<Value> val3{ValueFactory::GetIntegerValue(202), ValueFactory::GetIntegerValue(22)};
  std::vector<std::vector<Value>> raw_vals{val1, val2, val3};
  auto table_info = GetCatalog()->GetTable("empty_table2");
  // index
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);
  auto index_info = GetExecutorContext()->GetCatalog()->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(
      GetTxn(), "index1", "empty_table2", GetExecutorContext()->GetCatalog()->GetTable("empty_table2")->schema_,
      *key_schema, {0}, 8);
  // Create insert plan node
  InsertPlanNode insert_plan{std::move(raw_vals), table_info->oid_};

  auto txn1 = GetTxnManager()->Begin();
  auto exec_ctx1 = std::make_unique<ExecutorContext>(txn1, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());

  GetExecutionEngine()->Execute(&insert_plan, nullptr, txn1, exec_ctx1.get());
  GetTxnManager()->Abort(txn1);
  delete txn1;

  // Iterate through table make sure that values were not inserted.
  auto &schema = table_info->schema_;
  auto colA = MakeColumnValueExpression(schema, 0, "colA");
  auto colB = MakeColumnValueExpression(schema, 0, "colB");
  auto out_schema = MakeOutputSchema({{"colA", colA}, {"colB", colB}});
  SeqScanPlanNode scan_plan{out_schema, nullptr, table_info->oid_};

  auto txn2 = GetTxnManager()->Begin();
  auto exec_ctx2 = std::make_unique<ExecutorContext>(txn2, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());

  std::vector<Tuple> result_set;
  GetExecutionEngine()->Execute(&scan_plan, &result_set, txn2, exec_ctx2.get());

  // Size
  ASSERT_EQ(result_set.size(), 0);
  // also check index not inserted
  Tuple index_key = Tuple{val1, &schema};
  std::vector<RID> rids;
  index_info->index_->ScanKey(index_key, &rids, txn2);
  ASSERT_EQ(rids.size(), 0);

  GetTxnManager()->Commit(txn2);
  delete txn2;

  delete key_schema;
}

// NOLINTNEXTLINE
TEST_F(TransactionTest, SimpleDeleteRollbackTest) {
  // txn1: DELETE FROM test_1 WHERE colA = 100
  // txn1: abort
  // txn2: SELECT * FROM test_1 WHERE colA = 100;
  auto table_info = GetCatalog()->GetTable("test_1");
  auto &schema = table_info->schema_;
  auto colA = MakeColumnValueExpression(schema, 0, "colA");
  auto const100 = MakeConstantValueExpression(ValueFactory::GetIntegerValue(100));
  auto predicate = MakeComparisonExpression(colA, const100, ComparisonType::Equal);
  auto out_schema = MakeOutputSchema({{"colA", colA}});
  // index
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);
  auto index_info = GetExecutorContext()->GetCatalog()->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(
      GetTxn(), "index1", "test_1", GetExecutorContext()->GetCatalog()->GetTable("test_1")->schema_, *key_schema, {0},
      8);

  SeqScanPlanNode scan_plan{out_schema, predicate, table_info->oid_};

  auto txn1 = GetTxnManager()->Begin();
  auto exec_ctx1 = std::make_unique<ExecutorContext>(txn1, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());

  std::vector<Tuple> result_set;
  GetExecutionEngine()->Execute(&scan_plan, &result_set, txn1, exec_ctx1.get());
  ASSERT_EQ(result_set.size(), 1);
  ASSERT_EQ(result_set[0].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 100);

  Tuple index_key = result_set[0].KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
  std::vector<RID> rids;
  index_info->index_->ScanKey(index_key, &rids, txn1);
  ASSERT_EQ(rids.size(), 1);

  GetTxnManager()->Commit(txn1);
  delete txn1;

  // Create delete plan node
  DeletePlanNode delete_plan{&scan_plan, table_info->oid_};

  auto txn2 = GetTxnManager()->Begin();
  auto exec_ctx2 = std::make_unique<ExecutorContext>(txn2, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());
  GetExecutionEngine()->Execute(&delete_plan, nullptr, txn2, exec_ctx2.get());
  GetTxnManager()->Abort(txn2);
  delete txn2;

  // Iterate through table make sure that values were not deleted.
  auto txn3 = GetTxnManager()->Begin();
  auto exec_ctx3 = std::make_unique<ExecutorContext>(txn3, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());

  result_set.clear();
  GetExecutionEngine()->Execute(&scan_plan, &result_set, txn3, exec_ctx3.get());

  // Size
  ASSERT_EQ(result_set.size(), 1);
  ASSERT_EQ(result_set[0].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 100);
  // also check index not deleted
  index_key = result_set[0].KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
  rids.clear();
  index_info->index_->ScanKey(index_key, &rids, txn3);
  ASSERT_EQ(rids.size(), 1);

  GetTxnManager()->Commit(txn3);
  delete txn3;

  delete key_schema;
}

// NOLINTNEXTLINE
TEST_F(TransactionTest, SimpleUpdateRollbackTest) {
  // txn1: UPDATE test_1 SET colA = colA + 1000, colB = colB + 1 WHERE colA = 100
  // txn1: abort
  // txn2: SELECT * FROM test_1 WHERE colA = 100;
  auto table_info = GetCatalog()->GetTable("test_1");
  auto &schema = table_info->schema_;
  auto colA = MakeColumnValueExpression(schema, 0, "colA");
  auto colB = MakeColumnValueExpression(schema, 0, "colB");
  auto const100 = MakeConstantValueExpression(ValueFactory::GetIntegerValue(100));
  auto predicate = MakeComparisonExpression(colA, const100, ComparisonType::Equal);
  auto out_schema = MakeOutputSchema({{"colA", colA}, {"colB", colB}});
  // index
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);
  auto index_info = GetExecutorContext()->GetCatalog()->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(
      GetTxn(), "index1", "test_1", GetExecutorContext()->GetCatalog()->GetTable("test_1")->schema_, *key_schema, {0},
      8);

  SeqScanPlanNode scan_plan{out_schema, predicate, table_info->oid_};

  auto txn1 = GetTxnManager()->Begin();
  auto exec_ctx1 = std::make_unique<ExecutorContext>(txn1, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());

  std::vector<Tuple> result_set;
  GetExecutionEngine()->Execute(&scan_plan, &result_set, txn1, exec_ctx1.get());
  ASSERT_EQ(result_set.size(), 1);
  ASSERT_TRUE(result_set[0].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>() == 100);

  Tuple index_key = result_set[0].KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
  std::vector<RID> rids;
  index_info->index_->ScanKey(index_key, &rids, txn1);
  ASSERT_EQ(rids.size(), 1);

  auto original_colB_value = result_set[0].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>();
  GetTxnManager()->Commit(txn1);
  delete txn1;

  // Create update plan node
  std::unordered_map<uint32_t, UpdateInfo> update_attrs{{0, {UpdateType::Add, 1000}}, {1, {UpdateType::Add, 1}}};
  UpdatePlanNode update_plan{&scan_plan, table_info->oid_, update_attrs};

  auto txn2 = GetTxnManager()->Begin();
  auto exec_ctx2 = std::make_unique<ExecutorContext>(txn2, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());

  GetExecutionEngine()->Execute(&update_plan, nullptr, txn2, exec_ctx2.get());
  GetTxnManager()->Abort(txn2);
  delete txn2;

  // Iterate through table make sure that values were not updated.
  auto txn3 = GetTxnManager()->Begin();
  auto exec_ctx3 = std::make_unique<ExecutorContext>(txn3, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());
  result_set.clear();
  GetExecutionEngine()->Execute(&scan_plan, &result_set, txn3, exec_ctx3.get());

  // Size
  ASSERT_EQ(result_set.size(), 1);
  ASSERT_TRUE(result_set[0].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>() == 100);
  // also check index not updated
  index_key = result_set[0].KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
  rids.clear();
  index_info->index_->ScanKey(index_key, &rids, txn3);
  ASSERT_EQ(rids.size(), 1);

  auto new_colB_value = result_set[0].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>();
  ASSERT_EQ(original_colB_value, new_colB_value);

  GetTxnManager()->Commit(txn3);
  delete txn3;

  delete key_schema;
}

// NOLINTNEXTLINE
TEST_F(TransactionTest, DirtyReadsTest) {
  // txn1: INSERT INTO empty_table2 VALUES (200, 20), (201, 21), (202, 22)
  // txn2: SELECT * FROM empty_table2;
  // txn1: abort

  // Create Values to insert
  std::vector<Value> val1{ValueFactory::GetIntegerValue(200), ValueFactory::GetIntegerValue(20)};
  std::vector<Value> val2{ValueFactory::GetIntegerValue(201), ValueFactory::GetIntegerValue(21)};
  std::vector<Value> val3{ValueFactory::GetIntegerValue(202), ValueFactory::GetIntegerValue(22)};
  std::vector<std::vector<Value>> raw_vals{val1, val2, val3};

  auto table_info = GetCatalog()->GetTable("empty_table2");
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);
  auto index_info = GetCatalog()->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(
      GetTxn(), "index1", "empty_table2", table_info->schema_, *key_schema, {0}, 8);

  // Create insert plan node
  InsertPlanNode insert_plan{std::move(raw_vals), table_info->oid_};
  auto txn1 = GetTxnManager()->Begin(nullptr, IsolationLevel::READ_UNCOMMITTED);
  auto exec_ctx1 = std::make_unique<ExecutorContext>(txn1, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());

  GetExecutionEngine()->Execute(&insert_plan, nullptr, txn1, exec_ctx1.get());

  // Iterate through table to read the tuples.
  auto &schema = table_info->schema_;
  auto colA = MakeColumnValueExpression(schema, 0, "colA");
  auto colB = MakeColumnValueExpression(schema, 0, "colB");
  auto out_schema = MakeOutputSchema({{"colA", colA}, {"colB", colB}});

  SeqScanPlanNode scan_plan{out_schema, nullptr, table_info->oid_};
  auto txn2 = GetTxnManager()->Begin(nullptr, IsolationLevel::READ_UNCOMMITTED);
  auto exec_ctx2 = std::make_unique<ExecutorContext>(txn2, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());

  std::vector<Tuple> result_set;
  GetExecutionEngine()->Execute(&scan_plan, &result_set, txn2, exec_ctx2.get());

  // Size
  ASSERT_EQ(result_set.size(), 3);

  // index
  Tuple index_key = result_set[0].KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
  std::vector<RID> rids;
  index_info->index_->ScanKey(index_key, &rids, txn2);

  GetTxnManager()->Abort(txn1);
  delete txn1;

  // First value
  ASSERT_EQ(result_set[0].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 200);
  ASSERT_EQ(result_set[0].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 20);

  // Second value
  ASSERT_EQ(result_set[1].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 201);
  ASSERT_EQ(result_set[1].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 21);

  // Third value
  ASSERT_EQ(result_set[2].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 202);
  ASSERT_EQ(result_set[2].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 22);

  // can also read index
  ASSERT_EQ(rids.size(), 1);

  GetTxnManager()->Commit(txn2);
  delete txn2;

  delete key_schema;
}

// NOLINTNEXTLINE
TEST_F(TransactionTest, UnrepeatableReadsTest) {
  // txn1: INSERT INTO empty_table2 VALUES (200, 20), (201, 21), (202, 22)
  // txn1: commit
  // txn2: SELECT * FROM empty_table2;
  // txn3: UPDATE empty_table2 SET colB = colB + 10;
  // txn3: commit
  // txn2: SELECT * FROM empty_table2;
  // txn2: commit

  // Create Values to insert
  std::vector<Value> val1{ValueFactory::GetIntegerValue(200), ValueFactory::GetIntegerValue(20)};
  std::vector<Value> val2{ValueFactory::GetIntegerValue(201), ValueFactory::GetIntegerValue(21)};
  std::vector<Value> val3{ValueFactory::GetIntegerValue(202), ValueFactory::GetIntegerValue(22)};
  std::vector<std::vector<Value>> raw_vals{val1, val2, val3};

  auto table_info = GetCatalog()->GetTable("empty_table2");
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);
  auto index_info = GetCatalog()->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(
      GetTxn(), "index1", "empty_table2", table_info->schema_, *key_schema, {0}, 8);

  // Create insert plan node
  InsertPlanNode insert_plan{std::move(raw_vals), table_info->oid_};
  auto txn1 = GetTxnManager()->Begin(nullptr, IsolationLevel::READ_COMMITTED);
  auto exec_ctx1 = std::make_unique<ExecutorContext>(txn1, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());

  GetExecutionEngine()->Execute(&insert_plan, nullptr, txn1, exec_ctx1.get());
  GetTxnManager()->Commit(txn1);
  delete txn1;

  // Iterate through table to read the tuples.
  auto &schema = table_info->schema_;
  auto colA = MakeColumnValueExpression(schema, 0, "colA");
  auto colB = MakeColumnValueExpression(schema, 0, "colB");
  auto out_schema = MakeOutputSchema({{"colA", colA}, {"colB", colB}});

  SeqScanPlanNode scan_plan{out_schema, nullptr, table_info->oid_};
  auto txn2 = GetTxnManager()->Begin(nullptr, IsolationLevel::READ_COMMITTED);
  auto exec_ctx2 = std::make_unique<ExecutorContext>(txn2, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());

  std::vector<Tuple> result_set;
  GetExecutionEngine()->Execute(&scan_plan, &result_set, txn2, exec_ctx2.get());

  // Size
  ASSERT_EQ(result_set.size(), 3);
  // First value
  ASSERT_EQ(result_set[0].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 200);
  ASSERT_EQ(result_set[0].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 20);
  // Second value
  ASSERT_EQ(result_set[1].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 201);
  ASSERT_EQ(result_set[1].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 21);
  // Third value
  ASSERT_EQ(result_set[2].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 202);
  ASSERT_EQ(result_set[2].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 22);

  // index
  Tuple index_key = result_set[0].KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
  std::vector<RID> rids;
  index_info->index_->ScanKey(index_key, &rids, txn2);
  // can also read index
  ASSERT_EQ(rids.size(), 1);

  // Create update plan node
  std::unordered_map<uint32_t, UpdateInfo> update_attrs{{1, {UpdateType::Add, 10}}};
  UpdatePlanNode update_plan{&scan_plan, table_info->oid_, update_attrs};
  auto txn3 = GetTxnManager()->Begin(nullptr, IsolationLevel::READ_COMMITTED);
  auto exec_ctx3 = std::make_unique<ExecutorContext>(txn3, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());
  GetExecutionEngine()->Execute(&update_plan, nullptr, txn3, exec_ctx3.get());
  GetTxnManager()->Commit(txn3);
  delete txn3;

  // read again
  result_set.clear();
  GetExecutionEngine()->Execute(&scan_plan, &result_set, txn2, exec_ctx2.get());

  // Size
  ASSERT_EQ(result_set.size(), 3);
  // First value
  ASSERT_EQ(result_set[0].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 200);
  ASSERT_EQ(result_set[0].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 30);
  // Second value
  ASSERT_EQ(result_set[1].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 201);
  ASSERT_EQ(result_set[1].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 31);
  // Third value
  ASSERT_EQ(result_set[2].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 202);
  ASSERT_EQ(result_set[2].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 32);

  // index
  index_key = result_set[0].KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
  rids.clear();
  index_info->index_->ScanKey(index_key, &rids, txn2);
  // can also read index
  ASSERT_EQ(rids.size(), 1);

  GetTxnManager()->Commit(txn2);
  delete txn2;

  delete key_schema;
}

// NOLINTNEXTLINE
TEST_F(TransactionTest, RepeatableReadsTest) {
  // txn1: INSERT INTO empty_table2 VALUES (200, 20), (201, 21), (202, 22)
  // txn1: commit
  // txn2: begin
  // txn3: begin
  // txn2: SELECT * FROM empty_table2;
  // txn3: UPDATE empty_table2 SET colB = colB + 10;
  // txn2: commit
  // txn3: commit

  // Create Values to insert
  std::vector<Value> val1{ValueFactory::GetIntegerValue(200), ValueFactory::GetIntegerValue(20)};
  std::vector<Value> val2{ValueFactory::GetIntegerValue(201), ValueFactory::GetIntegerValue(21)};
  std::vector<Value> val3{ValueFactory::GetIntegerValue(202), ValueFactory::GetIntegerValue(22)};
  std::vector<std::vector<Value>> raw_vals{val1, val2, val3};

  auto table_info = GetCatalog()->GetTable("empty_table2");
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);
  auto index_info = GetCatalog()->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(
      GetTxn(), "index1", "empty_table2", table_info->schema_, *key_schema, {0}, 8);

  // Create insert plan node
  InsertPlanNode insert_plan{std::move(raw_vals), table_info->oid_};
  auto txn1 = GetTxnManager()->Begin(nullptr, IsolationLevel::REPEATABLE_READ);
  auto exec_ctx1 = std::make_unique<ExecutorContext>(txn1, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());

  GetExecutionEngine()->Execute(&insert_plan, nullptr, txn1, exec_ctx1.get());
  GetTxnManager()->Commit(txn1);
  delete txn1;

  // Iterate through table to read the tuples.
  auto &schema = table_info->schema_;
  auto colA = MakeColumnValueExpression(schema, 0, "colA");
  auto colB = MakeColumnValueExpression(schema, 0, "colB");
  auto out_schema = MakeOutputSchema({{"colA", colA}, {"colB", colB}});

  SeqScanPlanNode scan_plan{out_schema, nullptr, table_info->oid_};

  auto txn2 = GetTxnManager()->Begin(nullptr, IsolationLevel::REPEATABLE_READ);
  auto exec_ctx2 = std::make_unique<ExecutorContext>(txn2, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());

  auto txn3 = GetTxnManager()->Begin(nullptr, IsolationLevel::REPEATABLE_READ);
  auto exec_ctx3 = std::make_unique<ExecutorContext>(txn3, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());

  std::thread t3([&] {
    // Create update plan node
    std::unordered_map<uint32_t, UpdateInfo> update_attrs{{1, {UpdateType::Add, 10}}};
    UpdatePlanNode update_plan{&scan_plan, table_info->oid_, update_attrs};

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    GetExecutionEngine()->Execute(&update_plan, nullptr, txn3, exec_ctx3.get());

    GetTxnManager()->Commit(txn3);
  });

  std::vector<Tuple> result_set;
  GetExecutionEngine()->Execute(&scan_plan, &result_set, txn2, exec_ctx2.get());
  // Size
  ASSERT_EQ(result_set.size(), 3);
  // First value
  ASSERT_EQ(result_set[0].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 200);
  ASSERT_EQ(result_set[0].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 20);
  // Second value
  ASSERT_EQ(result_set[1].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 201);
  ASSERT_EQ(result_set[1].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 21);
  // Third value
  ASSERT_EQ(result_set[2].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 202);
  ASSERT_EQ(result_set[2].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 22);
  // index
  Tuple index_key = result_set[0].KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
  std::vector<RID> rids;
  index_info->index_->ScanKey(index_key, &rids, txn2);
  // can also read index
  ASSERT_EQ(rids.size(), 1);

  std::this_thread::sleep_for(std::chrono::milliseconds(150));
  GetTxnManager()->Commit(txn2);

  t3.join();

  delete txn2;
  delete txn3;

  delete key_schema;
}

}  // namespace bustub
