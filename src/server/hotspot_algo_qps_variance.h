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
    void read_analysis(const partition_data_queue &hotspot_app_data,
                       std::vector<hotpartition_counter> &perf_counters);

    void write_analysis(const partition_data_queue &hotspot_app_data,
                        std::vector<hotpartition_counter> &perf_counters);

    void analysis(const partition_data_queue &hotspot_app_data,
                  std::vector<hotpartition_counter> &perf_counters);
};
} // namespace server
} // namespace pegasus
