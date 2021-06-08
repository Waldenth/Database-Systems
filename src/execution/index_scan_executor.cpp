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

  auto final_cols = plan_->OutputSchema()->GetColumns();
  std::transform(
      final_cols.begin(), final_cols.end(), std::back_inserter(map_),
      [&table_schema = table_info_->schema_](auto final_col) { return table_schema.GetColIdx(final_col.GetName()); });
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
