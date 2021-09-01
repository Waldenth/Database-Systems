//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <algorithm>
#include <utility>
#include <vector>

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)} {
  inner_table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
  inner_index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexName(), inner_table_info_->name_);
}

void NestIndexJoinExecutor::Init() { child_executor_->Init(); }

bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple left_tuple;
  RID left_rid;
  Tuple right_raw_tuple;

  // fetch next qualified left tuple and right tuple pair
  do {
    if (!child_executor_->Next(&left_tuple, &left_rid)) {
      return false;
    }
  } while (!Probe(&left_tuple, &right_raw_tuple) ||
           (plan_->Predicate() != nullptr &&
            !plan_->Predicate()
                 ->EvaluateJoin(&left_tuple, plan_->OuterTableSchema(), &right_raw_tuple, &(inner_table_info_->schema_))
                 .GetAs<bool>()));

  // lock on to-read left and right rid
  switch (exec_ctx_->GetTransaction()->GetIsolationLevel()) {
    case IsolationLevel::READ_UNCOMMITTED:
      // no S lock
      break;
    case IsolationLevel::READ_COMMITTED:
      if (!exec_ctx_->GetTransaction()->IsSharedLocked(left_rid) &&
          !exec_ctx_->GetTransaction()->IsExclusiveLocked(left_rid) &&
          !(
              // S lock
              exec_ctx_->GetLockManager()->LockShared(exec_ctx_->GetTransaction(), left_rid) &&
              // but release immediately
              exec_ctx_->GetLockManager()->Unlock(exec_ctx_->GetTransaction(), left_rid))) {
        return false;
      }
      if (!exec_ctx_->GetTransaction()->IsSharedLocked(right_raw_tuple.GetRid()) &&
          !exec_ctx_->GetTransaction()->IsExclusiveLocked(right_raw_tuple.GetRid()) &&
          !(
              // S lock
              exec_ctx_->GetLockManager()->LockShared(exec_ctx_->GetTransaction(), right_raw_tuple.GetRid()) &&
              // but release immediately
              exec_ctx_->GetLockManager()->Unlock(exec_ctx_->GetTransaction(), right_raw_tuple.GetRid()))) {
        return false;
      }
      break;
    case IsolationLevel::REPEATABLE_READ:
      if (!exec_ctx_->GetTransaction()->IsSharedLocked(left_rid) &&
          !exec_ctx_->GetTransaction()->IsExclusiveLocked(left_rid) &&
          // S lock
          !exec_ctx_->GetLockManager()->LockShared(exec_ctx_->GetTransaction(), left_rid)) {
        return false;
      }
      if (!exec_ctx_->GetTransaction()->IsSharedLocked(right_raw_tuple.GetRid()) &&
          !exec_ctx_->GetTransaction()->IsExclusiveLocked(right_raw_tuple.GetRid()) &&
          // S lock
          !exec_ctx_->GetLockManager()->LockShared(exec_ctx_->GetTransaction(), right_raw_tuple.GetRid())) {
        return false;
      }
      break;
    default:
      break;
  }

  // populate output tuple
  std::vector<Value> values;
  std::transform(plan_->OutputSchema()->GetColumns().begin(), plan_->OutputSchema()->GetColumns().end(),
                 std::back_inserter(values),
                 [&left_tuple = left_tuple, &right_raw_tuple, &plan = plan_,
                  &inner_table_schema = inner_table_info_->schema_](const Column &col) {
                   return col.GetExpr()->EvaluateJoin(&left_tuple, plan->OuterTableSchema(), &right_raw_tuple,
                                                      &inner_table_schema);
                 });

  *tuple = Tuple(values, plan_->OutputSchema());

  return true;
}

bool NestIndexJoinExecutor::Probe(Tuple *left_tuple, Tuple *right_raw_tuple) {
  Value key_value = plan_->Predicate()->GetChildAt(0)->EvaluateJoin(left_tuple, plan_->OuterTableSchema(),
                                                                    right_raw_tuple, &(inner_table_info_->schema_));
  Tuple probe_key = Tuple{{key_value}, inner_index_info_->index_->GetKeySchema()};

  std::vector<RID> result_set;
  GetBPlusTreeIndex()->ScanKey(probe_key, &result_set, exec_ctx_->GetTransaction());
  if (result_set.empty()) {
    return false;
  }

  return inner_table_info_->table_->GetTuple(result_set[0], right_raw_tuple, exec_ctx_->GetTransaction());
}

}  // namespace bustub
