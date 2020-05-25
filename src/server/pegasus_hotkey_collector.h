// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <functional>
#include <dsn/utility/error_code.h>
#include <rrdb/rrdb_types.h>
#include <gtest/gtest_prod.h>
#include <readerwriterqueue/readerwriterqueue.h>
#include <dsn/dist/fmt_logging.h>

namespace pegasus {
namespace server {

typedef std::function<int(const std::string &, const int split_count)> hotbucket_hash_func;
typedef std::shared_ptr<std::unordered_map<int, int>> thread_queue_map;

class hotkey_coarse_data_collector;
class hotkey_fine_data_collector;

class hotkey_collector
{
public:
    hotkey_collector(dsn::apps::hotkey_type::type hotkey_type = dsn::apps::hotkey_type::WRITE);
    // after receiving START RPC, start to capture and analyse
    bool init();
    // after receiving STOP RPC or timeout, clear historical data
    void clear();
    void capture_blob_data(const ::dsn::blob &key);
    void capture_msg_data(dsn::message_ex **requests_point, const int count);
    void capture_str_data(const std::string &data);
    void capture_multi_get_data(const ::dsn::apps::multi_get_request &request,
                                const ::dsn::apps::multi_get_response &resp);
    // analyse_data is a periodic task, only valid when _collector_state == COARSE || FINE
    void analyse_data();
    std::string get_status();
    // ture: result = hotkey, false: can't find hotkey
    bool get_result(std::string &result);
    // 3 sigma to ensure accuracy
    static bool variance_cal(const std::vector<int> &data_samples,
                             std::vector<int> &hot_values,
                             const int threshold);
    int get_coarse_result() const { return _coarse_result; }

    enum collector_state_set
    {
        STOP = 0, // data has been cleard, ready to start
        COARSE,   // is running corase capture and analyse
        FINE,     // is running fine capture and analyse
        FINISH    // capture and analyse is done, ready to get result
    };

    hotbucket_hash_func hotbucket_hash;
    uint64_t hotkey_collector_max_work_time;
    uint64_t hotkey_collector_data_fragmentation;
    // related to variance_cal()
    uint64_t data_variance_threshold;
    // hotkey_type == READ, using THREAD_POOL_LOCAL_APP threadpool to distribute queue
    // hotkey_type == WRITE, using single queue
    dsn::apps::hotkey_type::type hotkey_type;

private:
    std::atomic<collector_state_set> _collector_state;
    uint64_t _timestamp;
    std::unique_ptr<hotkey_coarse_data_collector> _coarse_data_collector;
    std::unique_ptr<hotkey_fine_data_collector> _fine_data_collector;
    std::string _fine_result;
    int _coarse_result;

    FRIEND_TEST(pegasus_hotkey_collector_test, init_destory_timeout);
};

class hotkey_coarse_data_collector
{
public:
    hotkey_coarse_data_collector(const hotkey_collector *base);
    void capture_coarse_data(const std::string &data);
    const int analyse_coarse_data();

private:
    hotbucket_hash_func _hotbucket_hash;
    uint64_t _hotkey_collector_data_fragmentation;
    uint64_t _data_variance_threshold;
    std::vector<std::atomic<int>> _coarse_count;
};

class hotkey_fine_data_collector
{
public:
    hotkey_fine_data_collector(const hotkey_collector *base);
    void capture_fine_data(const std::string &data);
    bool analyse_fine_data(std::string &result);

private:
    std::unordered_map<std::string, int> _fine_count;
    hotbucket_hash_func _hotbucket_hash;
    int _target_bucket;
    thread_queue_map _qmap;
    std::vector<moodycamel::ReaderWriterQueue<std::string>> rw_queues;
    uint64_t _hotkey_collector_data_fragmentation;
    uint64_t _data_variance_threshold;

    inline int get_queue_index();
};

} // namespace server
} // namespace pegasus
