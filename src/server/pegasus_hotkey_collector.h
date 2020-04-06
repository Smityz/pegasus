// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <rrdb/rrdb_types.h>

namespace pegasus {
namespace server {

typedef rpc_holder<hotkey_detect_request, hotkey_detect_response> hotkey_rpc;

class hotkey_collector
{
public:
    void capture_data(const ::dsn::blob &key) { capture_data(key.data()); }

    void capture_data(dsn::message_ex **requests) { capture_data(requests[1].data()); }

    void analyse_data();

    void init(hotkey_detect_request r)
    {
        if (_collector_status.load(std::memory_order_seq_cst) != 0) {
            r.response().err = ERR_SERVICE_ALREADY_EXIST;
        } else {
            rpc = r;
            _timestamp = dsn_now_s();
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
        _collector_status.store(0, std::memory_order_seq_cst);
        _coarse_result.store(-1, std::memory_order_seq_cst);
        _fine_result = "";
        _fine_count.clear();
    }

private:
    void capture_data(std::string data)
    {
        if (_collector_status.load(std::memory_order_seq_cst) == 1)
            capture_coarse_data(data);
        if (_collector_status.load(std::memory_order_seq_cst) == 2)
            capture_fine_data(data);
    }
    int analyse_coarse_data() {}
    void capture_coarse_data(const std::string &data);
    void capture_fine_data(const std::string &data);

    std::atomic_uint _coarse_count[103];
    // _collector_status 0:stop 1:coarse 2:fine 3:finish
    std::atomic_ushort _collector_status(0);
    std::atomic_int _coarse_result(-1);
    struct fine_capture_unit_struct
    {
        std::queue<std::string> queue;
        std::mutex mutex;
    } _fine_capture_unit[103];
    std::string _fine_result;
    std::unordered_map<std::string, int> _fine_count;
    uint64_t timestamp;
    hotkey_rpc rpc;
}

} // namespace server
} // namespace pegasus