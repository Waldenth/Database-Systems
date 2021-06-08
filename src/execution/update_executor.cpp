//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <algorithm>
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)} {
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan_->TableOid());
}

void UpdateExecutor::Init() {
  child_executor_->Init();

  table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple dummy_tuple;
  RID emit_rid;
  if (!child_executor_->Next(&dummy_tuple, &emit_rid)) {
    return false;
  }

  Tuple to_update_tuple;
  auto fetched = table_info_->table_->GetTuple(emit_rid, &to_update_tuple, exec_ctx_->GetTransaction());
  if (!fetched) {
    return false;
  }

  Tuple updated_tuple = GenerateUpdatedTuple(to_update_tuple);
  bool updated = table_info_->table_->UpdateTuple(updated_tuple, emit_rid, exec_ctx_->GetTransaction());

  if (updated) {
    std::for_each(table_indexes.begin(), table_indexes.end(),
                  [&to_update_tuple, &updated_tuple, &emit_rid, &table_schema = table_info_->schema_,
                   &ctx = exec_ctx_](IndexInfo *index) {
                    index->index_->DeleteEntry(
                        to_update_tuple.KeyFromTuple(table_schema, index->key_schema_, index->index_->GetKeyAttrs()),
                        emit_rid, ctx->GetTransaction());
                    index->index_->InsertEntry(
                        updated_tuple.KeyFromTuple(table_schema, index->key_schema_, index->index_->GetKeyAttrs()),
                        emit_rid, ctx->GetTransaction());
                  });
  }

  return updated;
}
}  // namespace bustub
