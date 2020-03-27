// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "table_hotspot_policy.h"

#include <dsn/dist/fmt_logging.h>
#include <dsn/tool-api/rpc_address.h>
#include <dsn/tool-api/group_address.h>
#include <dsn/cpp/rpc_holder.h>
#include <dsn/cpp/serialization_helper/dsn.layer2_types.h>
#include <dsn/cpp/message_utils.h>

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

/*static*/ void hotspot_calculator::notice_replica(const std::string &app_name,
                                                   const int partition_num)
{
    std::vector< : rpc_address> meta_servers;
    replica_helper::load_meta_servers(meta_servers);
    rpc_address meta_server;

    meta_server.assign_group("meta-servers");
    for (auto &ms : meta_servers) {
        meta_server.group_address()->add(ms);
    }

    configuration_query_by_index_request req;
    req.app_name = app_name;
    auto cluster_name = replication::get_current_cluster_name();

    auto resolver = partition_resolver::get_resolver(cluster_name, meta_servers, app_name.c_str());

    typedef rpc_holder<configuration_query_by_index_request, configuration_query_by_index_response>
        configuration_query_by_index_rpc;
    configuration_query_by_index_request request;
    request.app_name = app_name;
    message_ex *msg = dsn::message_ex::create_request(RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX);
    marshall(msg, request);
}

void hotspot_calculator::start_alg()
{
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
