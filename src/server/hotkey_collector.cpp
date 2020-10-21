// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "hotkey_collector.h"

#include <dsn/dist/replication/replication_enums.h>
#include <dsn/utility/smart_pointers.h>
#include <dsn/utility/flags.h>
#include <boost/functional/hash.hpp>
#include "base/pegasus_key_schema.h"
#include <dsn/dist/fmt_logging.h>

namespace pegasus {
namespace server {

DSN_DEFINE_int32("pegasus.server",
                 coarse_data_variance_threshold,
                 3,
                 "the threshold of variance calculate to find the outliers");

DSN_DEFINE_validator(coarse_data_variance_threshold,
                     [](int32_t threshold) -> bool { return (threshold >= 0); });

DSN_DEFINE_int32("pegasus.server",
                 data_capture_hash_bucket_num,
                 37,
                 "the number of data capture hash buckets");

DSN_DEFINE_validator(data_capture_hash_bucket_num, [](int32_t bucket_num) -> bool {
    if (bucket_num < 3) {
        return false;
    }
    // data_capture_hash_bucket_num should be a prime number
    for (int i = 2; i <= bucket_num / i; i++) {
        if (bucket_num % i == 0)
            return false;
    }
    return true;
});

hotkey_collector::hotkey_collector(dsn::replication::hotkey_type::type hotkey_type,
                                   dsn::replication::replica_base *r_base)
    : replica_base(r_base),
      _state(hotkey_collector_state::STOPPED),
      _hotkey_type(hotkey_type),
      _internal_collector(std::make_shared<hotkey_empty_data_collector>(this))
{
}

void hotkey_collector::handle_rpc(const dsn::replication::detect_hotkey_request &req,
                                  dsn::replication::detect_hotkey_response &resp)
{
    switch (req.action) {
    case dsn::replication::detect_action::START:
        on_start_detect(resp);
        return;
    case dsn::replication::detect_action::STOP:
        on_stop_detect(resp);
        return;
    default:
        std::string hint = fmt::format("{}: can't find this detect action", req.action);
        resp.err = dsn::ERR_INVALID_STATE;
        resp.__set_err_hint(hint);
        derror_replica(hint);
    }
}

void hotkey_collector::capture_raw_key(const dsn::blob &raw_key, int64_t weight)
{
    dsn::blob hash_key, sort_key;
    pegasus_restore_key(raw_key, hash_key, sort_key);
    capture_hash_key(hash_key, weight);
}

void hotkey_collector::capture_hash_key(const dsn::blob &hash_key, int64_t weight)
{
    // TODO: (Tangyanzhao) add a unit test to ensure data integrity
    _internal_collector->capture_data(hash_key, weight);
}

void hotkey_collector::analyse_data()
{
    switch (_state.load()) {
    case hotkey_collector_state::COARSE_DETECTING:
        _internal_collector->analyse_data(_result);
        if (_result.coarse_bucket_index != -1) {
            // TODO: (Tangyanzhao) reset _internal_collector to hotkey_fine_data_collector
            _state.store(hotkey_collector_state::FINE_DETECTING);
        }
        return;
    default:
        return;
    }
}

/*static*/ int hotkey_collector::get_bucket_id(dsn::string_view data)
{
    size_t hash_value = boost::hash_range(data.begin(), data.end());
    return static_cast<int>(hash_value % FLAGS_data_capture_hash_bucket_num);
}

void hotkey_collector::on_start_detect(dsn::replication::detect_hotkey_response &resp)
{
    auto now_state = _state.load();
    std::string hint;
    switch (now_state) {
    case hotkey_collector_state::COARSE_DETECTING:
    case hotkey_collector_state::FINE_DETECTING:
        resp.err = dsn::ERR_INVALID_STATE;
        hint = fmt::format("still detecting {} hotkey, state is {}",
                           dsn::enum_to_string(_hotkey_type),
                           enum_to_string(now_state));
        dwarn_replica(hint);
        return;
    case hotkey_collector_state::FINISHED:
        resp.err = dsn::ERR_INVALID_STATE;
        hint = fmt::format(
            "{} hotkey result has been found, you can send a stop rpc to restart hotkey detection",
            dsn::enum_to_string(_hotkey_type));
        dwarn_replica(hint);
        return;
    case hotkey_collector_state::STOPPED:
        _internal_collector.reset(new hotkey_coarse_data_collector(this));
        _state.store(hotkey_collector_state::COARSE_DETECTING);
        resp.err = dsn::ERR_OK;
        hint = fmt::format("starting to detect {} hotkey", dsn::enum_to_string(_hotkey_type));
        ddebug_replica(hint);
        return;
    default:
        hint = "invalid collector state";
        resp.err = dsn::ERR_INVALID_STATE;
        resp.__set_err_hint(hint);
        derror_replica(hint);
        dassert(false, "invalid collector state");
    }
}

void hotkey_collector::on_stop_detect(dsn::replication::detect_hotkey_response &resp)
{
    _state.store(hotkey_collector_state::STOPPED);
    _internal_collector.reset();
    resp.err = dsn::ERR_OK;
    std::string hint =
        fmt::format("{} hotkey stopped, cache cleared", dsn::enum_to_string(_hotkey_type));
    ddebug_replica(hint);
}

hotkey_coarse_data_collector::hotkey_coarse_data_collector(replica_base *base)
    : internal_collector_base(base), _hash_buckets(FLAGS_data_capture_hash_bucket_num)
{
    for (auto &bucket : _hash_buckets) {
        bucket.store(0);
    }
}

void hotkey_coarse_data_collector::capture_data(const dsn::blob &hash_key, uint64_t weight)
{
    _hash_buckets[hotkey_collector::get_bucket_id(hash_key)].fetch_add(weight);
}

void hotkey_coarse_data_collector::analyse_data(detect_hotkey_result &result)
{
    std::vector<uint64_t> buckets(FLAGS_data_capture_hash_bucket_num);
    for (int i = 0; i < buckets.size(); i++) {
        buckets[i] = _hash_buckets[i].load();
        _hash_buckets[i].store(0);
    }
    outlier_detection detection(buckets, FLAGS_coarse_data_variance_threshold);
    int hotindex = -1;
    if (detection.find_hotindex(hotindex)) {
        result.coarse_bucket_index = hotindex;
    }
}

outlier_detection::outlier_detection(const std::vector<uint64_t> &captured_keys, int threshold)
    : _data_size(captured_keys.size()), _threshold(threshold)

{
    calculate_data_count(captured_keys);
    calculate_standard_deviation(captured_keys);
}

void outlier_detection::calculate_data_count(const std::vector<uint64_t> &captured_keys)
{
    for (int i = 0; i < _data_size; i++) {
        _data_count += captured_keys[i];
        if (captured_keys[i] > _hot_value) {
            _hot_index = i;
            _hot_value = captured_keys[i];
        }
    }
}

void outlier_detection::calculate_standard_deviation(const std::vector<uint64_t> &captured_keys)
{
    dcheck_gt(captured_keys.size(), 2);
    _avg_count = (_data_count - captured_keys[_hot_index]) / (_data_size - 1);
    for (int i = 0; i < _data_size; i++) {
        if (i != _hot_index) {
            _standard_deviation += pow((captured_keys[i] - _avg_count), 2);
        }
    }
    _standard_deviation = sqrt(_standard_deviation / (_data_size - 2));
}

bool outlier_detection::find_hotindex(int &hot_index)
{
    double hot_point = (_hot_value - _avg_count) / _standard_deviation;
    hot_index = _hot_index;
    return hot_point >= _threshold;
}

} // namespace server
} // namespace pegasus
