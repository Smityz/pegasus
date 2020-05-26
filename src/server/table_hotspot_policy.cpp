// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "table_hotspot_policy.h"

#include <dsn/dist/fmt_logging.h>
#include <dsn/tool-api/rpc_address.h>
#include <dsn/tool-api/group_address.h>
#include <dsn/utility/error_code.h>
#include <rrdb/rrdb_types.h>
#include "pegasus_hotkey_collector.h"

using namespace dsn;

namespace pegasus {
namespace server {

hotspot_calculator::hotspot_calculator(const std::string &app_name,
                                       const int partition_num,
                                       std::unique_ptr<hotspot_policy> policy)
    : _app_name(app_name),
      _points(partition_num),
      _policy(std::move(policy)),
      _hotpartition_threshold(0),
      _occurrence_threshold(0)
{
    init_perf_counter(partition_num);
    _enable_hotkey_auto_detect =
        dsn_config_get_value_bool("pegasus.collector",
                                  "hotkey_auto_detect",
                                  true,
                                  "auto detect hot key in the hot paritition");
    if (_enable_hotkey_auto_detect) {
        _over_threshold_times_read.resize(partition_num);
        _over_threshold_times_write.resize(partition_num);
        _hotpartition_threshold =
            (uint32_t)dsn_config_get_value_uint64("pegasus.collector",
                                                  "hotpartition_threshold",
                                                  3,
                                                  "threshold of hotspot partition value");
        _occurrence_threshold = (uint32_t)dsn_config_get_value_uint64(
            "pegasus.collector",
            "occurrence_threshold",
            1,
            "hot paritiotion occurrence times'threshold to send rpc to detect hotkey");
    }
}

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

/*static*/ void hotspot_calculator::notify_replica(const std::string &app_name,
                                                   const int partition_index,
                                                   const bool is_read_request)
{
    ddebug("start to notify_replica, %s.%d %s",
           app_name.c_str(),
           partition_index,
           is_read_request ? "read_data" : "write_data");
    std::vector<rpc_address> meta_servers;
    replica_helper::load_meta_servers(meta_servers);
    rpc_address meta_server;

    meta_server.assign_group("meta-servers");
    for (auto &ms : meta_servers) {
        meta_server.group_address()->add(ms);
    }
    auto cluster_name = replication::get_current_cluster_name();
    auto resolver = partition_resolver::get_resolver(cluster_name, meta_servers, app_name.c_str());
    ::dsn::apps::hotkey_detect_request req;
    req.type = is_read_request ? dsn::apps::hotkey_type::READ : dsn::apps::hotkey_type::WRITE;
    req.operation = dsn::apps::hotkey_collector_operation::START;
    resolver->call_op(
        RPC_DETECT_HOTKEY,
        req,
        nullptr,
        [app_name, partition_index, is_read_request](
            error_code err, dsn::message_ex *request, dsn::message_ex *resp) {
            if (err == ERR_OK) {
                ::dsn::apps::hotkey_detect_response response;
                ::dsn::unmarshall(resp, response);
                if (response.err == ERR_OK) {
                    ddebug("hotkey detect rpc sending successed");
                } else {
                    ddebug("hotkey detect rpc sending failed");
                }
            } else {
                ddebug("hotkey detect rpc sending failed, %s", err.to_string());
            }
        },
        std::chrono::seconds(10),
        partition_index,
        0);
}

void hotspot_calculator::start_alg()
{
    ddebug("start to detect hotspot partition");
    _policy->analysis(_app_data, _points);
    if (_enable_hotkey_auto_detect) {
        bool find_a_read_hotpartition = false;
        bool find_a_write_hotpartition = false;
        for (int i = 0; i < _points.size(); i++) {
            if (_points[i].read_hotpartition_counter->get_value() >= _hotpartition_threshold &&
                ++_over_threshold_times_read[i] > _occurrence_threshold) {
                notify_replica(this->_app_name, i, true);
                find_a_read_hotpartition = true;
            }
            if (_points[i].write_hotpartition_counter->get_value() >= _hotpartition_threshold &&
                ++_over_threshold_times_write[i] > _occurrence_threshold) {
                notify_replica(this->_app_name, i, false);
                find_a_write_hotpartition = true;
            }
        }
        if (find_a_read_hotpartition) {
            for (int i = 0; i < _points.size(); i++) {
                _over_threshold_times_read[i] = 0;
            }
            ddebug("Find a read hot partition");
        } else {
            ddebug("Not find a read hot partition");
        }
        if (find_a_write_hotpartition) {
            for (int i = 0; i < _points.size(); i++) {
                _over_threshold_times_write[i] = 0;
            }
            ddebug("Find a write hot partition");
        } else {
            ddebug("Not find a write hot partition");
        }
    }
}

} // namespace server
} // namespace pegasus
