// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/cpp/serverlet.h>
#include <dsn/utility/error_code.h>
#include <rrdb/rrdb_types.h>

namespace pegasus {
namespace server {

class hotkey_collector
{
public:
    void capture_data(const ::dsn::blob &key) { capture_data(key.to_string()); }

    void capture_data(const dsn::message_ex **requests, const int count)
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

    void analyse_data();

    // hotkey_collector() : _collector_status(0), _coarse_result(-1) {}

    void init(::dsn::rpc_replier<::dsn::apps::hotkey_detect_response> &reply)
    {
        if (_collector_status.load(std::memory_order_seq_cst) != 0) {
            ::dsn::apps::hotkey_detect_response resp;
            resp.err = ERR_SERVICE_ALREADY_EXIST;
            reply(resp);
        };
        if (_collector_status.load(std::memory_order_seq_cst) == 0) {
            _reply = reply;
            _timestamp = dsn_now_s();
            _collector_status.store(1, std::memory_order_seq_cst);
        }
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
        _collector_status.store(0, std::memory_order_seq_cst);
    }

private:
    void capture_data(const std::string &data)
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
    const int analyse_coarse_data();
    void capture_coarse_data(const std::string &data);
    void capture_fine_data(const std::string &data);
    bool analyse_fine_data(const std::string &data);

    std::atomic_uint _coarse_count[103];
    // _collector_status 0:stop 1:coarse 2:fine 3:finish
    std::atomic<unsigned short> _collector_status;
    std::atomic<int> _coarse_result;
    struct fine_capture_unit_struct
    {
        std::queue<std::string> queue;
        std::mutex mutex;
    } _fine_capture_unit[103];
    std::string _fine_result;
    std::unordered_map<std::string, int> _fine_count;
    uint64_t _timestamp;
    const int kMaxTime = 100;
    ::dsn::rpc_replier<::dsn::apps::hotkey_detect_response> _reply;
};

} // namespace server
} // namespace pegasus
