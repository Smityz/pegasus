// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "pegasus_hotkey_collector.h"

#include <math.h>

namespace pegasus {
namespace server {

unsigned long z, w, jsr, jcong; // Seeds
void randinit(unsigned long x_)
{
    z = x_;
    w = x_;
    jsr = x_;
    jcong = x_;
}
unsigned long znew() { return (z = 36969 * (z & 0xfffful) + (z >> 16)); }
unsigned long wnew() { return (w = 18000 * (w & 0xfffful) + (w >> 16)); }
unsigned long MWC() { return ((znew() << 16) + wnew()); }
unsigned long SHR3()
{
    jsr ^= (jsr << 17);
    jsr ^= (jsr >> 13);
    return (jsr ^= (jsr << 5));
}
unsigned long CONG() { return (jcong = 69069 * jcong + 1234567); }
unsigned long rand_int() // [0,2^32-1]
{
    return ((MWC() ^ CONG()) + SHR3());
}

void hotkey_collector::capture_fine_data(const std::string &data)
{
    unsigned long index = rand_int() % 103;
    std::unique_lock<std::mutex> lck(_fine_capture_unit[index].mutex, std::defer_lock);
    if (lck.try_lock()) {
        _fine_capture_unit[index].queue.emplace(data);
        while (_fine_capture_unit[index].queue.size() > 1000) {
            _fine_capture_unit[index].queue.pop();
        }
    }
}

bool hotkey_collector::analyse_fine_data()
{
    for (int i = 0; i < 103; i++) {
        const std::lock_guard<std::mutex> lock(_fine_capture_unit[i].mutex);
        while (!_fine_capture_unit[i].queue.empty()) {
            _fine_count[_fine_capture_unit[i].queue.front()]++;
            _fine_capture_unit[i].queue.pop();
        }
    }
    if (_fine_count.size() == 0) {
        derror("analyse_fine_data map size = 0");
        return false;
    }
    int count_max = -1;
    std::string count_max_key;
    for (const auto &iter : _fine_count)
        if (iter.second > count_max) {
            count_max = iter.second;
            count_max_key = iter.first;
        }
    _fine_result = count_max_key;
    return true;
}

void hotkey_collector::capture_data(dsn::message_ex **requests, const int count)
{
    if (count == 0) {
        return;
    }
    for (int i = 0; i < count; i++) {
        if (requests[i] == nullptr)
            continue;
        capture_data(requests[i]->buffers[1].to_string());
    }
}

void hotkey_collector::capture_data(const ::dsn::blob &key) { capture_data(key.to_string()); }

void hotkey_collector::capture_data(const std::string &data)
{
    if (_collector_status.load(std::memory_order_seq_cst) == 0) {
        return;
    }
    if (_collector_status.load(std::memory_order_seq_cst) == 1) {
        capture_coarse_data(data);
    }
    if (_collector_status.load(std::memory_order_seq_cst) == 2) {
        capture_fine_data(data);
    }
    if (_collector_status.load(std::memory_order_seq_cst) == 3) {
        return;
    }
}

void hotkey_collector::analyse_data()
{
    if (_collector_status.load(std::memory_order_seq_cst) == 0) {
        return;
    }
    if (_collector_status.load(std::memory_order_seq_cst) == 1) {
        int coarse_result = analyse_coarse_data();
        if (coarse_result != -1) {
            _collector_status.store(2, std::memory_order_seq_cst);
            _coarse_result.store(coarse_result, std::memory_order_seq_cst);
        }
    }
    if (_collector_status.load(std::memory_order_seq_cst) == 2) {
        if (analyse_fine_data()) {
            _collector_status.store(3, std::memory_order_seq_cst);
        }
    }
    if (_collector_status.load(std::memory_order_seq_cst) == 3) {
        derror("Hotkey result: [%s]", _fine_result);
        clear();
    }
    if (dsn_now_s() - _timestamp > kMaxTime) {
        derror("ERR_NOT_FOUND_HOTKEY");
        clear();
    }
}

void hotkey_collector::capture_coarse_data(const std::string &data)
{
    size_t key_hash_val = std::hash<std::string>{}(data) % 103;
    _coarse_count[key_hash_val].fetch_add(1, std::memory_order_release);
}

const int hotkey_collector::analyse_coarse_data()
{
    std::vector<uint> data_samples;
    data_samples.reserve(103);
    double total = 0, sd = 0, avg = 0;
    for (int i = 0; i < 103; i++) {
        data_samples.push_back(_coarse_count[i].load(std::memory_order_seq_cst));
        total += data_samples.back();
    }
    if (total < 1000)
        return -1;
    avg = total / 103;
    for (auto data_sample : data_samples) {
        sd += pow((data_sample - avg), 2);
    }
    sd = sqrt(sd / 103);
    std::vector<uint> hotkey_hash_bucket;
    for (int i = 0; i < data_samples.size(); i++) {
        double hot_point = (data_samples[i] - avg) / sd;
        hot_point = ceil(std::max(hot_point, double(0)));
        if (hot_point > 3) {
            hotkey_hash_bucket.push_back(i);
        }
    }
    if (hotkey_hash_bucket.size() == 1) {
        return hotkey_hash_bucket.back();
    }
    if (hotkey_hash_bucket.size() >= 2) {
        derror("Multiple hotkey_hash_bucket is hot in this app, select the hottest one to detect");
        int hottest = -1, hottest_index = -1;
        for (int i = 0; i < hotkey_hash_bucket.size(); i++) {
            if (hottest < hotkey_hash_bucket[i]) {
                hottest = hotkey_hash_bucket[i];
                hottest_index = i;
            }
        }
        return hottest_index;
    }
    return -1;
}

} // namespace server
} // namespace pegasus
