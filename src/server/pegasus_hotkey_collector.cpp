// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "pegasus_hotkey_collector.h"

#include "base/pegasus_key_schema.h"
#include "base/pegasus_rpc_types.h"
#include <math.h>

namespace pegasus {
namespace server {

hotkey_collector::hotkey_collector(dsn::apps::hotkey_type::type hotkey_type)
    : hotkey_type(hotkey_type), _collector_state(STOP), _coarse_result(-1)
{
    hotkey_collector_max_work_time = dsn_config_get_value_uint64(
        "pegasus.server",
        "hotkey_collector_max_work_time",
        150,
        "hotkey_collector_max_work_time, after that the collection will stop automatically");

    hotkey_collector_data_fragmentation =
        dsn_config_get_value_uint64("pegasus.server",
                                    "hotkey_collector_data_fragmentation",
                                    37,
                                    "hotkey_collector_data_fragmentation");

    data_variance_threshold = dsn_config_get_value_uint64(
        "pegasus.server", "data_variance_threshold", 3, "data_variance_threshold");

    _timestamp = dsn_now_s();

    hotbucket_hash = [](const std::string &data, const int split_count) {
        return (int)(std::hash<std::string>{}(data) % split_count);
    };
}

bool hotkey_collector::init()
{
    switch (_collector_state.load()) {
    case COARSE:
        ddebug("Has been detecting %s hotkey, now state is %s",
               hotkey_type == dsn::apps::hotkey_type::READ ? "read" : "write",
               "COARSE");
        return false;
    case FINE:
        ddebug("Has been detecting %s hotkey, now state is %s",
               hotkey_type == dsn::apps::hotkey_type::READ ? "read" : "write",
               "FINE");
        return false;
    case FINISH:
        ddebug(
            "%s hotkey result has been found, you can send a stop rpc to restart hotkey detection",
            hotkey_type == dsn::apps::hotkey_type::READ ? "Read" : "Write");
        return false;
    case STOP:
        _timestamp = dsn_now_s();
        _coarse_data_collector.reset(new hotkey_coarse_data_collector(this));
        _collector_state.store(COARSE);
        ddebug("Is starting to detect %s hotkey",
               hotkey_type == dsn::apps::hotkey_type::READ ? "read" : "write");
        return true;
    }
    derror("wrong collector state");
    return false;
}

void hotkey_collector::clear()
{
    _collector_state.store(STOP);
    _coarse_data_collector.reset();
    _fine_data_collector.reset();
    ddebug("Already cleared %s hotkey cache",
           hotkey_type == dsn::apps::hotkey_type::READ ? "read" : "write");
}

std::string hotkey_collector::get_status()
{
    switch (_collector_state.load()) {
    case COARSE:
        return "COARSE";
    case FINE:
        return "FINE";
    case FINISH:
        return "FINISH";
    case STOP:
        return "STOP";
    }
    derror("wrong collector state");
    return "false";
}

bool hotkey_collector::get_result(std::string &result)
{
    if (_collector_state.load() != FINISH)
        return false;
    result = _fine_result;
    return true;
}

void hotkey_collector::capture_msg_data(dsn::message_ex **requests, const int count)
{
    if (_collector_state.load() == STOP || count == 0 || _collector_state.load() == FINISH) {
        return;
    }
    for (int i = 0; i < count; i++) {
        ::dsn::blob key;
        if (requests[i] != nullptr) {
            dsn::task_code rpc_code(requests[0]->rpc_code());
            if (rpc_code == dsn::apps::RPC_RRDB_RRDB_MULTI_PUT) {
                dsn::apps::multi_put_request thrift_request;
                unmarshall(requests[0], thrift_request);
                requests[0]->restore_read();
                key = thrift_request.hash_key;
                capture_str_data(key.to_string());
                if (thrift_request.kvs.size() > 1) {
                    for (int j = 0; j < thrift_request.kvs.size() - 2; j++) {
                        capture_str_data(key.to_string());
                    }
                }
                return;
            }
            if (rpc_code == dsn::apps::RPC_RRDB_RRDB_INCR) {
                dsn::apps::incr_request thrift_request;
                unmarshall(requests[0], thrift_request);
                requests[0]->restore_read();
                key = thrift_request.key;
                capture_blob_data(key);
                return;
            }
            if (rpc_code == dsn::apps::RPC_RRDB_RRDB_CHECK_AND_SET) {
                dsn::apps::check_and_set_request thrift_request;
                unmarshall(requests[0], thrift_request);
                requests[0]->restore_read();
                key = thrift_request.hash_key;
                capture_str_data(key.to_string());
                return;
            }
            if (rpc_code == dsn::apps::RPC_RRDB_RRDB_CHECK_AND_MUTATE) {
                dsn::apps::check_and_mutate_request thrift_request;
                unmarshall(requests[0], thrift_request);
                requests[0]->restore_read();
                key = thrift_request.hash_key;
                capture_str_data(key.to_string());
                return;
            }
            if (rpc_code == dsn::apps::RPC_RRDB_RRDB_PUT) {
                dsn::apps::update_request thrift_request;
                unmarshall(requests[i], thrift_request);
                requests[i]->restore_read();
                key = thrift_request.key;
                capture_blob_data(key);
                return;
            }
        }
    }
}

void hotkey_collector::capture_multi_get_data(const ::dsn::apps::multi_get_request &request,
                                              const ::dsn::apps::multi_get_response &resp)
{
    if (_collector_state.load() == STOP || _collector_state.load() == FINISH) {
        return;
    }
    if (resp.kvs.size() != 0) {
        for (const auto &iter : resp.kvs) {
            capture_blob_data(request.hash_key);
        }
    } else {
        capture_blob_data(request.hash_key);
    }
}

void hotkey_collector::capture_blob_data(const ::dsn::blob &key)
{
    if (_collector_state.load() == STOP || _collector_state.load() == FINISH) {
        return;
    }
    std::string hash_key, sort_key;
    if (key.length() < 2)
        return;
    pegasus_restore_key(key, hash_key, sort_key);
    capture_str_data(hash_key);
}

void hotkey_collector::capture_str_data(const std::string &data)
{
    if (_collector_state.load() == STOP || data.length() == 0 ||
        _collector_state.load() == FINISH) {
        return;
    }
    if (_collector_state.load() == COARSE && _coarse_data_collector != nullptr) {
        _coarse_data_collector->capture_coarse_data(data);
    }
    if (_collector_state.load() == FINE && _fine_data_collector != nullptr) {
        _fine_data_collector->capture_fine_data(data);
    }
}

void hotkey_collector::analyse_data()
{
    if (_collector_state.load() == STOP || _collector_state.load() == FINISH) {
        return;
    }
    if (dsn_now_s() - _timestamp >= hotkey_collector_max_work_time) {
        derror("ERR_NOT_FOUND_HOTKEY");
        clear();
        return;
    }
    if (_collector_state.load() == COARSE && _coarse_data_collector != nullptr) {
        _coarse_result = _coarse_data_collector->analyse_coarse_data();
        if (_coarse_result != -1) {
            _fine_data_collector.reset(new hotkey_fine_data_collector(this));
            _collector_state.store(FINE);
            _coarse_data_collector.reset();
        }
    }
    if (_collector_state.load() == FINE) {
        if (_fine_data_collector != nullptr &&
            _fine_data_collector->analyse_fine_data(_fine_result)) {
            derror("%s hotkey result: [%s]",
                   hotkey_type == dsn::apps::hotkey_type::READ ? "read" : "write",
                   _fine_result.c_str());
            _collector_state.store(FINISH);
            _fine_data_collector.reset();
        }
    }
}

bool hotkey_collector::variance_cal(const std::vector<int> &data_samples,
                                    std::vector<int> &hot_values,
                                    const int threshold)
{
    bool is_hotkey = false;
    int data_size = data_samples.size();
    double total = 0;
    for (const auto &data_sample : data_samples) {
        total += data_sample;
    }
    // in case of sample size too small
    if (data_size < 3 || total < data_size) {
        for (int i = 0; i < data_size; i++)
            hot_values.emplace_back(0);
        return false;
    }
    std::vector<double> avgs;
    std::vector<double> sds;
    for (int i = 0; i < data_size; i++) {
        double avg = (total - data_samples[i]) / (data_size - 1);
        double sd = 0;
        for (int j = 0; j < data_size; j++) {
            if (j != i) {
                sd += (data_samples[j] - avg) * (data_samples[j] - avg);
            }
        }
        sd = sqrt(sd / (data_size - 2));
        avgs.emplace_back(avg);
        sds.emplace_back(sd);
    }
    for (int i = 0; i < data_size; i++) {
        double hot_point = (data_samples[i] - avgs[i]) / sds[i];
        hot_point = ceil(std::max(hot_point, double(0)));
        hot_values.emplace_back(hot_point);
        if (hot_point >= threshold) {
            is_hotkey = true;
        }
    }
    return is_hotkey;
}

} // namespace server
} // namespace pegasus
