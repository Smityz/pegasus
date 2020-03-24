// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <algorithm>
#include <gtest/gtest_prod.h>
#include <math.h>

#include "hotspot_partition_data.h"
#include "info_collector.h"
#include <dsn/perf_counter/perf_counter.h>

namespace pegasus {
namespace server {

struct hotpartition_counter
{
    ::dsn::perf_counter_wrapper read_hotpartition_counter, write_hotpartition_counter;
};

class hotspot_policy
{
public:
    // hotspot_app_data store the historical data which related to hotspot
    // it uses rolling queue to save one app's data
    // vector is used to save the partitions' data of this app
    // hotspot_partition_data is used to save data of one partition
    virtual void analysis(const std::queue<std::vector<hotspot_partition_data>> &hotspot_app_data,
                          std::vector<hotpartition_counter> &perf_counters) = 0;
};

// hotspot_calculator is used to find the hotspot in Pegasus
class hotspot_calculator
{
public:
    hotspot_calculator(const std::string &app_name,
                       const int partition_num,
                       std::unique_ptr<hotspot_policy> policy,
                       const bool detect_hotkey)
        : _app_name(app_name),
          _points(partition_num),
          _policy(std::move(policy)),
          detect_hotkey,
          _auto_detect_hotkey(detect_hotkey)
    {
        init_perf_counter(partition_num);
        if (_auto_detect_hotkey) {
            _over_threshold_times(partition_num);
        }
    }
    void aggregate(const std::vector<row_data> &partitions);
    void start_alg();
    void init_perf_counter(const int perf_counter_count);
    void notice_replica(const std::string &app_name, const int partition_num);

private:
    const std::string _app_name;
    std::vector<hotpartition_counter> _points;
    std::vector<int> _over_threshold_times;
    std::queue<std::vector<hotspot_partition_data>> _app_data;
    std::unique_ptr<hotspot_policy> _policy;
    bool _auto_detect_hotkey;
    static const int kMaxQueueSize = 100;
    FRIEND_TEST(table_hotspot_policy, hotspot_algo_qps_variance);
};
} // namespace server
} // namespace pegasus
