//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_{plan} {
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_);
}

void IndexScanExecutor::Init() {
  index_iter = std::make_unique<INDEXITERATOR_TYPE>(GetBPlusTreeIndex()->GetBeginIterator());
}

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  // fetch raw tuple from table
  Tuple raw_tuple;
  do {
    if (*index_iter == GetBPlusTreeIndex()->GetEndIterator()) {
      return false;
    }

    bool fetched = table_info_->table_->GetTuple((*(*index_iter)).second, &raw_tuple, exec_ctx_->GetTransaction());
    if (!fetched) {
      return false;
    }

    ++(*index_iter);
  } while (plan_->GetPredicate() != nullptr &&
           !plan_->GetPredicate()->Evaluate(&raw_tuple, &(table_info_->schema_)).GetAs<bool>());

  // lock on to-read rid
  switch (exec_ctx_->GetTransaction()->GetIsolationLevel()) {
    case IsolationLevel::READ_UNCOMMITTED:
      // no S lock
      break;
    case IsolationLevel::READ_COMMITTED:
      if (!exec_ctx_->GetTransaction()->IsSharedLocked(raw_tuple.GetRid()) &&
          !exec_ctx_->GetTransaction()->IsExclusiveLocked(raw_tuple.GetRid()) &&
          !(
              // S lock
              exec_ctx_->GetLockManager()->LockShared(exec_ctx_->GetTransaction(), raw_tuple.GetRid()) &&
              // but release immediately
              exec_ctx_->GetLockManager()->Unlock(exec_ctx_->GetTransaction(), raw_tuple.GetRid()))) {
        return false;
      }
      break;
    case IsolationLevel::REPEATABLE_READ:
      if (!exec_ctx_->GetTransaction()->IsSharedLocked(raw_tuple.GetRid()) &&
          !exec_ctx_->GetTransaction()->IsExclusiveLocked(raw_tuple.GetRid()) &&
          // S lock
          !exec_ctx_->GetLockManager()->LockShared(exec_ctx_->GetTransaction(), raw_tuple.GetRid())) {
        return false;
      }
      break;
    default:
      break;
  }

  // populate output tuple
  std::vector<Value> values;
  std::transform(plan_->OutputSchema()->GetColumns().begin(), plan_->OutputSchema()->GetColumns().end(),
                 std::back_inserter(values), [&raw_tuple, &table_info = table_info_](const Column &col) {
                   return col.GetExpr()->Evaluate(&raw_tuple, &(table_info->schema_));
                 });

  *tuple = Tuple{values, plan_->OutputSchema()};
  *rid = raw_tuple.GetRid();

  return true;
}

}  // namespace bustub
