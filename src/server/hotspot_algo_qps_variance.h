// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <algorithm>
#include <gtest/gtest_prod.h>
#include <math.h>

#include "hotspot_partition_data.h"
#include "table_hotspot_policy.h"
#include <dsn/perf_counter/perf_counter.h>

namespace pegasus {
namespace server {
// PauTa Criterion
class hotspot_algo_qps_variance : public hotspot_policy
{
public:
    void read_analysis(const std::queue<std::vector<hotspot_partition_data>> &hotspot_app_data,
                       std::vector<hotpartition_counter> &perf_counters)
    {
        dassert(hotspot_app_data.back().size() == perf_counters.size(),
                "partition counts error, please check");
        std::vector<double> data_samples;
        data_samples.reserve(hotspot_app_data.size() * perf_counters.size());
        auto temp_data = hotspot_app_data;
        double total = 0, sd = 0, avg = 0;
        int sample_count = 0;
        // avg: Average number
        // sd: Standard deviation
        // sample_count: Number of samples
        while (!temp_data.empty()) {
            for (auto partition_data : temp_data.front()) {
                if (partition_data.total_read_qps - 1.00 > 0) {
                    data_samples.push_back(partition_data.total_read_qps);
                    total += partition_data.total_read_qps;
                    sample_count++;
                }
            }
            temp_data.pop();
        }
        if (sample_count == 0) {
            ddebug("hotspot_app_data size == 0");
            return;
        }
        avg = total / sample_count;
        for (auto data_sample : data_samples) {
            sd += pow((data_sample - avg), 2);
        }
        sd = sqrt(sd / sample_count);
        const auto &anly_data = hotspot_app_data.back();
        for (int i = 0; i < perf_counters.size(); i++) {
            double hot_point = (anly_data[i].total_read_qps - avg) / sd;
            // perf_counter->set can only be unsigned __int64
            // use ceil to guarantee conversion results
            hot_point = ceil(std::max(hot_point, double(0)));
            perf_counters[i].read_hotpartition_counter->set(hot_point);
        }
    }

    void write_analysis(const std::queue<std::vector<hotspot_partition_data>> &hotspot_app_data,
                        std::vector<hotpartition_counter> &perf_counters)
    {
        dassert(hotspot_app_data.back().size() == perf_counters.size(),
                "partition counts error, please check");
        std::vector<double> data_samples;
        data_samples.reserve(hotspot_app_data.size() * perf_counters.size());
        auto temp_data = hotspot_app_data;
        double total = 0, sd = 0, avg = 0;
        int sample_count = 0;
        // avg: Average number
        // sd: Standard deviation
        // sample_count: Number of samples
        while (!temp_data.empty()) {
            for (auto partition_data : temp_data.front()) {
                if (partition_data.total_write_qps - 1.00 > 0) {
                    data_samples.push_back(partition_data.total_write_qps);
                    total += partition_data.total_write_qps;
                    sample_count++;
                }
            }
            temp_data.pop();
        }
        if (sample_count == 0) {
            ddebug("hotspot_app_data size == 0");
            return;
        }
        avg = total / sample_count;
        for (auto data_sample : data_samples) {
            sd += pow((data_sample - avg), 2);
        }
        sd = sqrt(sd / sample_count);
        const auto &anly_data = hotspot_app_data.back();
        for (int i = 0; i < perf_counters.size(); i++) {
            double hot_point = (anly_data[i].total_write_qps - avg) / sd;
            // perf_counter->set can only be unsigned __int64
            // use ceil to guarantee conversion results
            hot_point = ceil(std::max(hot_point, double(0)));
            perf_counters[i].write_hotpartition_counter->set(hot_point);
        }
    }

    void analysis(const std::queue<std::vector<hotspot_partition_data>> &hotspot_app_data,
                  std::vector<hotpartition_counter> &perf_counters)
    {
        read_analysis(hotspot_app_data, perf_counters);
        write_analysis(hotspot_app_data, perf_counters);
    }
};
} // namespace server
} // namespace pegasus
