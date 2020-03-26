// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <algorithm>
#include <math.h>

#include "hotspot_partition_data.h"
#include <dsn/perf_counter/perf_counter.h>
#include <dsn/dist/replication/duplication_common.h>
#include <gtest/gtest_prod.h>

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
private:
public:
    hotspot_calculator(const std::string &app_name,
                       const int partition_num,
                       std::unique_ptr<hotspot_policy> policy)
        : _app_name(app_name),
          _points(partition_num),
          _policy(std::move(policy)),
          kHotPartitionT(0),
          kHotRpcT(0)
    {
        init_perf_counter(partition_num);
        _hotkey_auto_detect =
            dsn_config_get_value_bool("pegasus.collector",
                                      "hotkey_auto_detect",
                                      true,
                                      "auto detect hot key in the hot paritition");
        if (_hotkey_auto_detect) {
            _over_threshold_times.resize(partition_num);
            kHotPartitionT = (uint32_t)dsn_config_get_value_uint64(
                "pegasus.collector", "kHotPartitionT", 4, "threshold of hotspot partition value");
            kHotRpcT = (uint32_t)dsn_config_get_value_uint64(
                "pegasus.collector", "kHotRpcT", 1, "threshold of send rpc to detect hotkey");
        }
    }
    void aggregate(const std::vector<row_data> &partitions);
    void start_alg();
    void init_perf_counter(const int perf_counter_count);
    static void notice_replica(const std::string &app_name, const int partition_num);

private:
    const std::string _app_name;
    std::vector<hotpartition_counter> _points;
    std::vector<int> _over_threshold_times;
    std::queue<std::vector<hotspot_partition_data>> _app_data;
    std::unique_ptr<hotspot_policy> _policy;
    bool _hotkey_auto_detect;
    const int kMaxQueueSize = 100;
    int kHotPartitionT, kHotRpcT;
    dsn::task_tracker _tracker;
    FRIEND_TEST(table_hotspot_policy, hotspot_algo_qps_variance);
};
} // namespace server
} // namespace pegasus
