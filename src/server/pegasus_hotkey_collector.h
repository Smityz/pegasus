// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

namespace pegasus {
namespace server {

class hotkey_collector
{
public:
    void hotkey_collector()
    {
        _collector_status.load(1);
        _timestamp = dsn_now_s();
    }

    void ~hotkey_collector() {}

    void capture_read_data(const ::dsn::blob &key) { capture_data(key.data()); }

    void capture_write_data(dsn::message_ex **requests) { capture_data(requests[1].data()); }

    void analyse_data() {}

private:
    void capture_data(std::string data)
    {
        if (_collector_status.load(std::memory_order_relaxed) == 1)
            capture_coarse_data(data);
        if (_collector_status.load(std::memory_order_relaxed) == 2)
            capture_fine_data(data);
    }

    int analyse_coarse_data() {}

    void capture_coarse_data(std::string data) { hash_table[hash(data)]++; }

    void capture_fine_data(std::string data) {}

    std::atomic_uint _coarse_count[103]();
    // _collector_status 0:stop 1:coarse 2:fine 3:finish
    std::atomic_ushort _collector_status(0);
    std::atomic_int _coarse_result(-1);

    struct fine_capture_unit_struct
    {
        std::queue<std::string> queue;
        std::mutex mutex;
    } _fine_capture_unit[103];

    std::string _app_paritition_info;
    std::string _fine_result;
    std::unordered_map<std::string, int> _fine_count;
    uint64_t timestamp;
}

} // namespace server
} // namespace pegasus