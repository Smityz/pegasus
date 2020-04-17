// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/cpp/serverlet.h>
#include <dsn/utility/error_code.h>
#include <rrdb/rrdb_types.h>
#include <gtest/gtest_prod.h>
#include <dsn/utility/ringbuf.h>

namespace pegasus {
namespace server {

enum collector_status_set
{
    STOP = 0,
    COARSE,
    FINE,
    FINISH
};

class hotkey_collector
{
public:
    hotkey_collector() : _collector_state(STOP), _coarse_result(-1) {}

    bool init()
    {
        if (_collector_state.load(std::memory_order_seq_cst) == COARSE ||
            _collector_state.load(std::memory_order_seq_cst) == FINE) {
            return false;
        }
        if (_collector_state.load(std::memory_order_seq_cst) == FINISH) {
            clear();
        }
        _timestamp = dsn_now_s();
        _collector_state.store(COARSE, std::memory_order_seq_cst);
        return true;
    }

    void clear()
    {
        for (int i = 0; i < 103; i++) {
            _coarse_count[i].store(0, std::memory_order_seq_cst);
            _fine_capture_unit[i].mutex.lock();
            while (!_fine_capture_unit[i].queue.empty()) {
                _fine_capture_unit[i].queue.pop();
            }
            _fine_capture_unit[i].mutex.unlock();
        }
        _coarse_result.store(-1, std::memory_order_seq_cst);
        _fine_result = "";
        _fine_count.clear();
        _collector_state.store(STOP, std::memory_order_seq_cst);
    }

    void capture_blob_data(const ::dsn::blob &key);
    void capture_msg_data(dsn::message_ex **requests, const int count);
    void capture_str_data(const std::string &data);

    void analyse_data();

    std::string get_status()
    {
        collector_state_set status = _collector_state.load(std::memory_order_seq_cst);
        if (status == STOP)
            return "STOP";
        if (status == COARSE)
            return "COARSE";
        if (status == FINE)
            return "FINE";
        return "FINISH";
    }

    bool get_result(std::string &result)
    {
        if (_collector_state.load(std::memory_order_seq_cst) != FINISH)
            return false;
        result = _fine_result;
        return true;
    }

    enum collector_state_set
    {
        STOP = 0,
        COARSE,
        FINE,
        FINISH
    };

    enum collector_rpc_type
    {
        READ = 0,
        WRITE
    };

private:
    void capture_coarse_data(const std::string &data);
    void capture_fine_data(const std::string &data);
    const int analyse_coarse_data();
    bool analyse_fine_data();

    std::atomic<collector_state_set> _collector_state;
    std::atomic_uint _coarse_count[103];
    std::atomic<int> _coarse_result;
    struct fine_capture_unit_struct
    {
        dsn::utils::ringbuf<std::string, 500> queue;
        std::mutex mutex;
    } _fine_capture_unit[103];
    std::string _fine_result;
    std::unordered_map<std::string, int> _fine_count;
    uint64_t _timestamp;
    const int kMaxTime = 100;

    FRIEND_TEST(hotkey_detect_test, find_hotkey);
};

} // namespace server
} // namespace pegasus
