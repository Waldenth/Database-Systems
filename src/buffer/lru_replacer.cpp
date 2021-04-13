//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : capacity{num_pages} {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  const std::lock_guard<mutex_t> guard(mutex);

  if (lst.empty()) {
    return false;
  }

  auto f = lst.front();
  lst.pop_front();

  auto hash_it = hash.find(f);
  if (hash_it != hash.end()) {
    hash.erase(hash_it);
    *frame_id = f;
    return true;
  }

  return false;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  const std::lock_guard<mutex_t> guard(mutex);

  auto hash_it = hash.find(frame_id);
  if (hash_it != hash.end()) {
    lst.erase(hash_it->second);
    hash.erase(hash_it);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  const std::lock_guard<mutex_t> guard(mutex);

  if (hash.size() >= capacity) {
    return;
  }

  auto hash_it = hash.find(frame_id);
  if (hash_it != hash.end()) {
    return;
  }

  lst.push_back(frame_id);
  hash.emplace(frame_id, std::prev(lst.end(), 1));
}

size_t LRUReplacer::Size() {
  const std::lock_guard<mutex_t> guard(mutex);

  return hash.size();
}

}  // namespace bustub
