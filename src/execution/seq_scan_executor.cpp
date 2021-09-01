//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <algorithm>
#include <vector>

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_{plan} {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
}

void SeqScanExecutor::Init() {
  table_iter = std::make_unique<TableIterator>(table_info_->table_->Begin(exec_ctx_->GetTransaction()));
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  // fetch raw tuple from table
  Tuple raw_tuple;
  do {
    if (*table_iter == table_info_->table_->End()) {
      return false;
    }

    raw_tuple = *(*table_iter);

    ++(*table_iter);
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
