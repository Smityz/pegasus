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
#include <dsn/utility/flags.h>
#include <dsn/dist/replication/replica_base.h>
#include "base/pegasus_utils.h"

namespace pegasus {
namespace server {

DSN_DECLARE_int32(data_capture_hash_bucket_num);

class hotkey_coarse_data_collector;
class hotkey_fine_data_collector;

enum class collector_state
{
    STOP,   // data has been cleared, ready to start
    COARSE, // is running coarse capture and analysis
    FINE,   // is running fine capture and analysis
    FINISH  // capture and analyse is done, ready to get result
};

// hotkey_collector is responsible to find the hot keys after the partition
// was detected to be hot. The two types of hotkey, READ & WRITE, are detected
// separately.
class hotkey_collector : public dsn::replication::replica_base
{
public:
    hotkey_collector(dsn::apps::hotkey_type::type hotkey_type,
                     dsn::replication::replica_base *r_base);

    void capture_blob_data(const ::dsn::blob &raw_key);
    void capture_msg_data(dsn::message_ex **requests_point, int count);
    void capture_multi_get_data(const ::dsn::apps::multi_get_request &request,
                                const ::dsn::apps::multi_get_response &resp);

    // analyse_data is a periodic task, only valid when _state == collector_state::COARSE
    // || collector_state::FINE
    void analyse_data();

    bool handle_operation(dsn::apps::hotkey_collector_operation::type op, std::string &err_hint);

    std::string get_status();
    // true: result = hotkey, false: can't find hotkey
    bool get_result(std::string &result) const;
    static int variance_calc(const std::vector<int> &data_samples, int threshold);
    static int get_bucket_id(dsn::string_view data);

private:
    // after receiving START RPC, start to capture and analyse
    // return true: start detecting successfully
    //        false: The query is in progress or a hot spot has been found
    bool start(/*out*/ std::string &err_hint);
    // after receiving collector_state::STOP RPC or timeout, clear historical data
    void stop();

    bool is_ready_to_detect() const
    {
        auto state = _state.load();
        return (state == collector_state::STOP || state == collector_state::FINISH);
    }

    void capture_hash_key(const dsn::blob &hash_key, int row_cnt = 1);

private:
    std::atomic<collector_state> _state;
    // By _collector_start_time && FLAGS_hotkey_collector_max_work_time it can stop automatically
    uint64_t _collector_start_time;
    std::unique_ptr<hotkey_coarse_data_collector> _coarse_data_collector;
    std::unique_ptr<hotkey_fine_data_collector> _fine_data_collector;
    std::string _fine_result;
    int _coarse_result;
    const dsn::apps::hotkey_type::type _hotkey_type;
};

// hotkey_coarse_data_collector handles the first procedure (COARSE) of hotkey detection.
// It captures the data without recording them, but simply divides the incoming requests
// into a number of buckets and counts the accessed times of each bucket.
// If the variance among the buckets exceeds the threshold, the most accessed bucket
// is regarded to contain the hotkey.
//
// This technique intends to reduce the load of data recording during FINE procedure,
// benefiting for both memory usage and performance.
//
class hotkey_coarse_data_collector : public dsn::replication::replica_base
{
public:
    explicit hotkey_coarse_data_collector(replica_base *base);

    // Counts `row_cnt` for the bucket of `hash_key`.
    void capture_data(const dsn::blob &hash_key, int row_cnt);

    // returns: id of the most accessed bucket.
    //          -1 if not hot bucket is found.
    int analyse_data();

private:
    std::vector<std::atomic<int>> _hash_buckets;
};

// hotkey_fine_data_collector handles the second procedure (FINE) of hotkey detection.
// It captures only the data mapping to the "hot" bucket.
class hotkey_fine_data_collector : public dsn::replication::replica_base
{
public:
    hotkey_fine_data_collector(replica_base *base,
                               int hot_bucket,
                               dsn::apps::hotkey_type::type hotkey_type);

    void capture_data(const dsn::blob &hash_key, int row_cnt);
    bool analyse_data(std::string &result);

private:
    typedef std::shared_ptr<std::unordered_map<int, int>> thread_queue_map;

    const int _target_bucket;
    thread_queue_map _thread_queue_map;
    std::vector<moodycamel::ReaderWriterQueue<std::pair<dsn::blob, int>>> _string_capture_queue;

    inline int get_queue_index();
};

} // namespace server
} // namespace pegasus
