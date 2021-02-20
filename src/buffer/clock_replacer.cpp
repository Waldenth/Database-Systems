//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages)
    : circular{num_pages + 1, ClockReplacer::Status::EMPTY}, hand{0}, capacity{num_pages + 1} {
  circular.reserve(num_pages + 1);
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  size_t unempty_count = 0;
  frame_id_t victim_frame_id = 0;

  for (size_t i = 0, idx = (hand + i + 1) % capacity; i < capacity; i++, idx = (hand + i + 1) % capacity) {
    if (circular[idx] == ClockReplacer::Status::ACCESSED) {
      unempty_count++;
      circular[idx] = ClockReplacer::Status::UNTOUCHED;
    } else if (circular[idx] == ClockReplacer::Status::UNTOUCHED) {
      unempty_count++;
      victim_frame_id = victim_frame_id ? victim_frame_id : idx;
    }
  }

  if (!unempty_count) {
    frame_id = nullptr;
    return false;
  }

  if (!victim_frame_id) {
    for (size_t i = 0, idx = (hand + i + 1) % capacity; i < capacity; i++, idx = (hand + i + 1) % capacity) {
      if (circular[idx] == ClockReplacer::Status::UNTOUCHED) {
        victim_frame_id = idx;
        break;
      }
    }
  }

  *frame_id = victim_frame_id;
  hand = victim_frame_id;

  circular[victim_frame_id % capacity] = ClockReplacer::Status::EMPTY;

  return true;
}

void ClockReplacer::Pin(frame_id_t frame_id) { circular[frame_id % capacity] = ClockReplacer::Status::EMPTY; }

void ClockReplacer::Unpin(frame_id_t frame_id) { circular[frame_id % capacity] = ClockReplacer::Status::ACCESSED; }

size_t ClockReplacer::Size() {
  return std::count_if(circular.begin(), circular.end(),
                       [](ClockReplacer::Status status) { return status != ClockReplacer::Status::EMPTY; });
}

}  // namespace bustub
