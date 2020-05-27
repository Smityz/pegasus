// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "pegasus_hotkey_collector.h"

namespace pegasus {
namespace server {

hotkey_coarse_data_collector::hotkey_coarse_data_collector(const hotkey_collector *base)
    : _hotbucket_hash(base->hotbucket_hash),
      _hotkey_collector_data_fragmentation(base->hotkey_collector_data_fragmentation),
      _data_variance_threshold(base->data_variance_threshold),
      _coarse_count(_hotkey_collector_data_fragmentation)
{
    for (int i = 0; i < _hotkey_collector_data_fragmentation; i++)
        _coarse_count[i].store(0);
}

void hotkey_coarse_data_collector::capture_coarse_data(const std::string &data)
{
    int key_hash_val = _hotbucket_hash(data, _hotkey_collector_data_fragmentation);
    ++_coarse_count[key_hash_val];
}

const int hotkey_coarse_data_collector::analyse_coarse_data()
{
    std::vector<int> data_samples;
    std::vector<int> hot_values;
    data_samples.reserve(_hotkey_collector_data_fragmentation);
    hot_values.reserve(_hotkey_collector_data_fragmentation);
    for (int i = 0; i < _hotkey_collector_data_fragmentation; i++) {
        data_samples.push_back(_coarse_count[i].load());
        _coarse_count[i].store(0);
    }
    if (hotkey_collector::variance_cal(data_samples, hot_values, _data_variance_threshold)) {
        int hotkey_num = 0, hotkey_index = 0;
        for (int i = 0; i < _hotkey_collector_data_fragmentation; i++) {
            if (hot_values[i] >= _data_variance_threshold) {
                hotkey_num++;
                hotkey_index = i;
            }
        }
        if (hotkey_num == 1) {
            derror("Find a hot bucket in analyse_coarse_data(), index: %d", hotkey_index);
            return hotkey_index;
        }
        if (hotkey_num >= 2) {
            int hottest = -1, hottest_index = -1;
            for (int i = 0; i < _hotkey_collector_data_fragmentation; i++) {
                if (hottest < hot_values[i]) {
                    hottest = hot_values[i];
                    hottest_index = i;
                }
            }
            derror("Multiple hotkey_hash_bucket is hot in this app, select the hottest one to "
                   "detect, index: %d",
                   hottest_index);
            return hottest_index;
        }
    }

    derror("Can't find a hot bucket in analyse_coarse_data()");
    return -1;
}

} // namespace server
} // namespace pegasus
