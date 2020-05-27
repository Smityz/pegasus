// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "pegasus_hotkey_collector.h"

#include <dsn/tool-api/task.h>
#include <dsn/dist/replication/replication.codes.h>

namespace pegasus {
namespace server {

const int kMaxQueueSize = 1000;

hotkey_fine_data_collector::hotkey_fine_data_collector(const hotkey_collector *base)
    : _hotbucket_hash(base->hotbucket_hash),
      _target_bucket(base->get_coarse_result()),
      _hotkey_collector_data_fragmentation(base->hotkey_collector_data_fragmentation),
      _data_variance_threshold(base->data_variance_threshold)
{
    if (base->hotkey_type == dsn::apps::hotkey_type::READ) {
        auto threads = dsn::get_threadpool_threads_info(THREAD_POOL_LOCAL_APP);
        // threads_pool and locol thread
        _qmap.reset(new std::unordered_map<int, int>);
        for (int i = 0; i < threads.size(); i++) {
            _qmap->insert({threads[i]->native_tid(), i});
        }
        int queue_num = threads.size();
        rw_queues.reserve(queue_num);
        for (int i = 0; i < queue_num; i++) {
            // this method is from https://github.com/cameron314/readerwriterqueue/issues/50
            rw_queues.emplace_back(kMaxQueueSize);
        }
    } else {
        rw_queues.emplace_back(kMaxQueueSize);
        _qmap.reset();
    }
}

inline int hotkey_fine_data_collector::get_queue_index()
{
    if (_qmap == nullptr) {
        return 0;
    } else {
        int thread_native_tid = ::dsn::utils::get_current_tid();
        auto result = _qmap->find(thread_native_tid);
        dassert(result != _qmap->end(), "Can't find the queue corresponding to the thread");
        return result->second;
    }
}

void hotkey_fine_data_collector::capture_fine_data(const std::string &data)
{
    std::cout << data << std::endl;
    if (_hotbucket_hash(data, _hotkey_collector_data_fragmentation) != _target_bucket)
        return;
    rw_queues[get_queue_index()].try_emplace(data);
}

bool hotkey_fine_data_collector::analyse_fine_data(std::string &result)
{
    for (int i = 0; i < rw_queues.size(); i++) {
        std::string hash_key;
        // try_emplace and try_dequeue happens at the same time
        // set a dequeue_count in case of endless loop
        int dequeue_count = 0;
        while (rw_queues[i].try_dequeue(hash_key) && dequeue_count <= kMaxQueueSize) {
            _fine_count[hash_key]++;
            dequeue_count++;
        }
    }
    if (_fine_count.size() == 0) {
        derror("analyse_fine_data map size = 0");
        return false;
    }
    std::vector<int> data_samples;
    std::vector<int> hot_values;
    data_samples.reserve(_hotkey_collector_data_fragmentation);
    hot_values.reserve(_hotkey_collector_data_fragmentation);
    std::string count_max_key;
    int count_max = -1;
    for (const auto &iter : _fine_count) {
        data_samples.push_back(iter.second);
        if (iter.second > count_max) {
            count_max = iter.second;
            count_max_key = iter.first;
        }
    }
    if (hotkey_collector::variance_cal(data_samples, hot_values, _data_variance_threshold)) {
        result = count_max_key;
        return true;
    }
    derror("analyse_fine_data failed");
    return false;
}

} // namespace server
} // namespace pegasus
