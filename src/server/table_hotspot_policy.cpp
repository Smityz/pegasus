// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "table_hotspot_policy.h"
#include "info_collector.h"
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
    std::string read_counter_name, write_counter_name;
    std::string read_counter_desc, write_counter_desc;
    for (int i = 0; i < perf_counter_count; i++) {
        string read_paritition_desc = _app_name + '.' + "read." + std::to_string(i);
        read_counter_name = fmt::format("app.stat.hotspots@{}", read_paritition_desc);
        read_counter_desc = fmt::format("statistic the hotspots of app {}", read_paritition_desc);
        _points[i].read_hotpartition_counter.init_app_counter("app.pegasus",
                                                              read_counter_name.c_str(),
                                                              COUNTER_TYPE_NUMBER,
                                                              read_counter_desc.c_str());
        string write_paritition_desc = _app_name + '.' + "write." + std::to_string(i);
        write_counter_name = fmt::format("app.stat.hotspots@{}", write_paritition_desc);
        write_counter_desc = fmt::format("statistic the hotspots of app {}", write_paritition_desc);
        _points[i].write_hotpartition_counter.init_app_counter("app.pegasus",
                                                               write_counter_name.c_str(),
                                                               COUNTER_TYPE_NUMBER,
                                                               write_counter_desc.c_str());
    }
}

void static hotspot_calculator::notice_replica(const std::string &app_name, const int partition_num)
{
}

void hotspot_calculator::start_alg()
{
    _policy->analysis(_app_data, _points);
    if (_auto_detect_hotkey) {
        for (const auto &item : _points) {
            if (item.read_hotpartition_counter->get_value() >
                THRESHOLD_OF_HOTSPOT_PARTITION_VALUE) {
                int index = distance(_points.begin(), item);
                _over_threshold_times[index]++;
                if (_over_threshold_times[distance(_points.begin(), item)] >
                    THRESHOLD_OF_SEND_RPC_TO_DETECT_HOTKEY) {
                    notice_replica(this->_app_name, index);
                    _over_threshold_times[index] = 0;
                }
            }
        }
    }
}

} // namespace server
} // namespace pegasus
