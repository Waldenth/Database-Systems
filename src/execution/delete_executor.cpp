//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)} {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
}

void DeleteExecutor::Init() {
  child_executor_->Init();

  table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple to_delete_tuple;
  RID emit_rid;
  if (!child_executor_->Next(&to_delete_tuple, &emit_rid)) {
    return false;
  }

  bool marked = table_info_->table_->MarkDelete(emit_rid, exec_ctx_->GetTransaction());

  if (marked) {
    std::for_each(
        table_indexes.begin(), table_indexes.end(),
        [&to_delete_tuple, &emit_rid, &table_schema = table_info_->schema_, &ctx = exec_ctx_](IndexInfo *index) {
          index->index_->DeleteEntry(
              to_delete_tuple.KeyFromTuple(table_schema, index->key_schema_, index->index_->GetKeyAttrs()), emit_rid,
              ctx->GetTransaction());
        });
  }

  return marked;
}

}  // namespace bustub
