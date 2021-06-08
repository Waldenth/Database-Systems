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
      right_executor_{std::move(right_executor)} {
  auto left_cols = left_executor_->GetOutputSchema()->GetColumns();
  auto right_cols = right_executor_->GetOutputSchema()->GetColumns();
  auto final_cols = plan_->OutputSchema()->GetColumns();

  std::transform(
      final_cols.begin(), final_cols.end(), std::back_inserter(map_), [&left_cols, &right_cols](auto final_col) {
        auto left_it = std::find_if(left_cols.begin(), left_cols.end(),
                                    [&final_col](auto col) { return col.GetName() == final_col.GetName(); });
        if (left_it != left_cols.end()) {
          return std::make_pair(0, left_it - left_cols.begin());
        }

        auto right_it = std::find_if(right_cols.begin(), right_cols.end(),
                                     [&final_col](auto col) { return col.GetName() == final_col.GetName(); });
        if (right_it != right_cols.end()) {
          return std::make_pair(1, right_it - right_cols.begin());
        }
        UNREACHABLE("Column in nested loop join output schema does not exist in either left table or right table");
      });
}

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

  std::vector<Value> values;
  std::transform(map_.begin(), map_.end(), std::back_inserter(values),
                 [&left_tuple = left_tuple, &left_executor = left_executor_, &right_tuple,
                  &right_executor = right_executor_](auto pair) {
                   return pair.first == 0 ? left_tuple.GetValue(left_executor->GetOutputSchema(), pair.second)
                                          : right_tuple.GetValue(right_executor->GetOutputSchema(), pair.second);
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
