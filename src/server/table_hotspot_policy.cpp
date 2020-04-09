// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "table_hotspot_policy.h"

#include <dsn/dist/fmt_logging.h>
#include <dsn/tool-api/rpc_address.h>
#include <dsn/tool-api/group_address.h>
#include <dsn/utility/error_code.h>
#include <rrdb/rrdb_types.h>

using namespace dsn;

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

inline void empty_rpc_handler(error_code, message_ex *, message_ex *) {}

/*static*/ void hotspot_calculator::notice_replica(const std::string &app_name,
                                                   const int partition_index)
{
    ddebug("start to notice_replica");
    std::vector<rpc_address> meta_servers;
    replica_helper::load_meta_servers(meta_servers);
    rpc_address meta_server;

    meta_server.assign_group("meta-servers");
    for (auto &ms : meta_servers) {
        meta_server.group_address()->add(ms);
    }
    auto cluster_name = replication::get_current_cluster_name();
    auto resolver = partition_resolver::get_resolver(cluster_name, meta_servers, app_name.c_str());
    hotkey_detect_request req;
    req.partition = partition_index;
    resolver->call_op(
        RPC_DETECT_HOTKEY,
        req,
        &_tracker,
        [this](error_code err, dsn::message_ex *request, dsn::message_ex *resp) {
            if (err == ERR_OK) {
                hotkey_detect_response response;
                ::dsn::unmarshall(resp, response);
                if (response.err == ERR_OK) {
                    ddebug("detect hotspot rpc sending succeed");
                    return;
                } else if (response.err == ERR_SERVICE_ALREADY_EXIST) {
                    ddebug("this hotspot rpc has been sending");
                }
            } else if (err == ERR_TIMEOUT) {
                notice_replica(app_name, partition_index);
            }
        },
        _hotkey_rpc_interval,
        partition_index,
        0);
}

void hotspot_calculator::start_alg()
{
    ddebug("start to start_alg");
    notice_replica(this->_app_name, 1);
    _policy->analysis(_app_data, _points);
    if (_hotkey_auto_detect) {
        for (int i = 0; i < _points.size(); i++) {
            if (_points[i].read_hotpartition_counter->get_value() > kHotPartitionT) {
                _over_threshold_times[i]++;
                if (_over_threshold_times[i] > kHotRpcT) {
                    notice_replica(this->_app_name, i);
                    _over_threshold_times[i] = 0;
                }
            }
        }
    }
}

} // namespace server
} // namespace pegasus
