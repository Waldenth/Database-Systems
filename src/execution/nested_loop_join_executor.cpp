//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <algorithm>
#include <iterator>
#include <utility>

#include "common/macros.h"
#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_{plan},
      left_executor_{std::move(left_executor)},
      right_executor_{std::move(right_executor)} {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  RID left_rid;
  // check if left tuple is initialized
  if (left_tuple.GetLength() == 0 && !left_executor_->Next(&left_tuple, &left_rid)) {
    return false;
  }

  // lock on to-read left rid
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
      break;
    case IsolationLevel::REPEATABLE_READ:
      if (!exec_ctx_->GetTransaction()->IsSharedLocked(left_rid) &&
          !exec_ctx_->GetTransaction()->IsExclusiveLocked(left_rid) &&
          // S lock
          !exec_ctx_->GetLockManager()->LockShared(exec_ctx_->GetTransaction(), left_rid)) {
        return false;
      }
      break;
    default:
      break;
  }

  // fetch next qualified left tuple and right tuple pair
  Tuple right_tuple;
  RID right_rid;
  do {
    if (!Advance(&left_rid, &right_tuple, &right_rid)) {
      return false;
    }
  } while (plan_->Predicate() != nullptr && !plan_->Predicate()
                                                 ->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(),
                                                                &right_tuple, right_executor_->GetOutputSchema())
                                                 .GetAs<bool>());

  // lock on to-read rid
  switch (exec_ctx_->GetTransaction()->GetIsolationLevel()) {
    case IsolationLevel::READ_UNCOMMITTED:
      // no S lock
      break;
    case IsolationLevel::READ_COMMITTED:
      if (!exec_ctx_->GetTransaction()->IsSharedLocked(right_rid) &&
          !exec_ctx_->GetTransaction()->IsExclusiveLocked(right_rid) &&
          !(
              // S lock
              exec_ctx_->GetLockManager()->LockShared(exec_ctx_->GetTransaction(), right_rid) &&
              // but release immediately
              exec_ctx_->GetLockManager()->Unlock(exec_ctx_->GetTransaction(), right_rid))) {
        return false;
      }
      break;
    case IsolationLevel::REPEATABLE_READ:
      if (!exec_ctx_->GetTransaction()->IsSharedLocked(right_rid) &&
          !exec_ctx_->GetTransaction()->IsExclusiveLocked(right_rid) &&
          // S lock
          !exec_ctx_->GetLockManager()->LockShared(exec_ctx_->GetTransaction(), right_rid)) {
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
                 [&left_tuple = left_tuple, &left_executor = left_executor_, &right_tuple,
                  &right_executor = right_executor_](const Column &col) {
                   return col.GetExpr()->EvaluateJoin(&left_tuple, left_executor->GetOutputSchema(), &right_tuple,
                                                      right_executor->GetOutputSchema());
                 });

  *tuple = Tuple(values, plan_->OutputSchema());

  return true;
}

bool NestedLoopJoinExecutor::Advance(RID *left_rid, Tuple *right_tuple, RID *right_rid) {
  if (!right_executor_->Next(right_tuple, right_rid)) {
    if (!left_executor_->Next(&left_tuple, left_rid)) {
      return false;
    }

    right_executor_->Init();
    right_executor_->Next(right_tuple, right_rid);
  }

  return true;
}

}  // namespace bustub
