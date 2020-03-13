// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "table_hotspot_policy.h"

#include <dsn/dist/fmt_logging.h>

namespace pegasus {
namespace server {

void hotspot_calculator::aggregate(const std::vector<row_data> &partitions)
{
    while (_app_data.size() > kMaxQueueSize - 1) {
        _app_data.pop();
    }
    std::vector<hotspot_partition_data> temp(partitions.size());
    for (int i = 0; i < partitions.size(); i++) {
        temp[i] = std::move(hotspot_partition_data(partitions[i]));
    }
    _app_data.emplace(temp);
}

void hotspot_calculator::init_perf_counter(const int perf_counter_count)
{
    std::string counter_name;
    std::string counter_desc;
    for (int i = 0; i < perf_counter_count; i++) {
        string paritition_desc = _app_name + '.' + std::to_string(i);
        counter_name = fmt::format("app.stat.hotspots@{}", paritition_desc);
        counter_desc = fmt::format("statistic the hotspots of app {}", paritition_desc);
        _points[i].init_app_counter(
            "app.pegasus", counter_name.c_str(), COUNTER_TYPE_NUMBER, counter_desc.c_str());
    }
}

// Read/write separation
vector<partition_id> hotspot_calculator::excpetion_read_check()
{
    vector<partition_id> suspected_read_partition;
    for (partition:suspected_read_partition){
        if (_read_points[i]>THRESHOLD_1){
            global_read_count[i]++;
            if (global_read_count[i]>THRESHOLD_2){
                suspected_read_partition.push(i);
            }
        }
    }
    //maybe we should clear global_count periodic
    return suspected_read_partition;
}

void send_read_check_rpc(suspected_partition){
    if (suspected_partition in read_watch_list){
        return;
    }
    result = send_to_relica();
    derror(result);
}

void hotspot_calculator::start_alg() {
    _policy->analysis(_app_data, _points);
    //we can also send_rpc manully
    if (auto_detect_on){
        this->send_read_check_rpc(excpetion_read_check());
        this->send_write_check_rpc(excpetion_write_check())
    }
}

} // namespace server
} // namespace pegasus
