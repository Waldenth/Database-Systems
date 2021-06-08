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

  auto final_cols = plan_->OutputSchema()->GetColumns();
  std::transform(
      final_cols.begin(), final_cols.end(), std::back_inserter(map_),
      [&table_schema = table_info_->schema_](auto final_col) { return table_schema.GetColIdx(final_col.GetName()); });
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

  // generate output tuple
  std::vector<Value> values;
  std::transform(
      map_.begin(), map_.end(), std::back_inserter(values),
      [&raw_tuple, &table_schema = table_info_->schema_](auto idx) { return raw_tuple.GetValue(&table_schema, idx); });
  *tuple = Tuple{values, plan_->OutputSchema()};
  *rid = raw_tuple.GetRid();

  return true;
}

}  // namespace bustub
