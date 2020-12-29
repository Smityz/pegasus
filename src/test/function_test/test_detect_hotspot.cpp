/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <libgen.h>

#include <dsn/utility/filesystem.h>
#include <dsn/dist/replication/replication_ddl_client.h>
#include <pegasus/client.h>
#include <gtest/gtest.h>
#include <boost/lexical_cast.hpp>
#include <dsn/utility/rand.h>

#include "base/pegasus_const.h"
#include "global_env.h"

using namespace ::dsn;
using namespace ::dsn::replication;
using namespace pegasus;

static std::string generate_hash_key_by_random(bool is_hotkey, int probability = 100)
{
    if (is_hotkey && (dsn::rand::next_u32(100) < probability)) {
        return "ThisisahotkeyThisisahotkey";
    }
    static const std::string chars("abcdefghijklmnopqrstuvwxyz"
                                   "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                   "1234567890"
                                   "!@#$%^&*()"
                                   "`~-_=+[{]{\\|;:'\",<.>/? ");
    std::string result;
    for (int i = 0; i < 20; i++) {
        result += chars[dsn::rand::next_u32(chars.size())];
    }
    return result;
}

class test_detect_hotspot : public testing::Test
{
public:
    virtual void SetUp() override
    {
        chdir(global_env::instance()._pegasus_root.c_str());
        system("pwd");

        system("./run.sh clear_onebox");
        system("cp src/server/config.min.ini config-server-test-hotspot.ini");
        system("sed -i \"/^\\s*enable_detect_hotkey/c enable_detect_hotkey = "
               "true\" config-server-test-hotspot.ini");
        system("./run.sh start_onebox -c -w --config_path config-server-test-hotspot.ini");
        std::this_thread::sleep_for(std::chrono::seconds(3));

        std::vector<dsn::rpc_address> meta_list;
        replica_helper::load_meta_servers(
            meta_list, PEGASUS_CLUSTER_SECTION_NAME.c_str(), "single_master_cluster");

        ddl_client = std::make_shared<replication_ddl_client>(meta_list);
        pg_client =
            pegasus::pegasus_client_factory::get_client("single_master_cluster", app_name.c_str());

        auto err = ddl_client->create_app(app_name.c_str(), "pegasus", 8, 3, {}, false);
        ASSERT_EQ(dsn::ERR_OK, err);
    }

    virtual void TearDown() override
    {
        chdir(global_env::instance()._pegasus_root.c_str());
        system("./run.sh clear_onebox");
        // system("./run.sh start_onebox -w");
        // chdir(global_env::instance()._working_dir.c_str());
    }

    void write_hotspot_data()
    {
        int64_t start = dsn_now_s();
        int err = PERR_OK;
        ASSERT_NE(pg_client, nullptr);

        const int64_t warmup_second = 30;
        for (int i = 0; dsn_now_s() - start < warmup_second; ++i %= 1000) {
            std::string index = std::to_string(i);
            std::string h_key = generate_hash_key_by_random(false, 0);
            std::string s_key = "sortkey_" + index;
            std::string value = "value_" + index;
            err = pg_client->set(h_key, s_key, value);
            ASSERT_EQ(err, PERR_OK);
        }

        for (int i = 0; dsn_now_s() - start < max_detection_second; ++i %= 1000) {
            std::string index = std::to_string(i);
            std::string h_key = generate_hash_key_by_random(true, 50);
            std::string s_key = "sortkey_" + index;
            std::string value = "value_" + index;
            err = pg_client->set(h_key, s_key, value);
            ASSERT_EQ(err, PERR_OK);
        }

        int32_t app_id;
        int32_t partition_count;
        std::vector<dsn::partition_configuration> partitions;
        ddl_client->list_app(app_name, app_id, partition_count, partitions);
        dsn::replication::detect_hotkey_response resp;
        dsn::replication::detect_hotkey_request req;
        req.type = dsn::replication::hotkey_type::type::WRITE;
        req.action = dsn::replication::detect_action::QUERY;
        bool find_hotkey = false;
        int partition_index;
        for (partition_index = 0; partition_index < partitions.size(); partition_index++) {
            req.pid = dsn::gpid(app_id, partition_index);
            auto errinfo =
                ddl_client->detect_hotkey(partitions[partition_index].primary, req, resp);
            ASSERT_EQ(errinfo, dsn::ERR_OK);
            if (!resp.hotkey_result.empty()) {
                find_hotkey = true;
                break;
            }
        }
        ASSERT_TRUE(find_hotkey);
        ASSERT_EQ(resp.err, dsn::ERR_OK);
        ASSERT_EQ(resp.hotkey_result, "ThisisahotkeyThisisahotkey");

        // Wait for collector sending the next start detecting command
        sleep(15);

        req.action = dsn::replication::detect_action::STOP;
        auto errinfo = ddl_client->detect_hotkey(partitions[partition_index].primary, req, resp);
        ASSERT_EQ(errinfo, dsn::ERR_OK);
        ASSERT_EQ(resp.err, dsn::ERR_OK);

        req.action = dsn::replication::detect_action::QUERY;
        for (partition_index = 0; partition_index < partitions.size(); partition_index++) {
            req.pid = dsn::gpid(app_id, partition_index);
            auto errinfo =
                ddl_client->detect_hotkey(partitions[partition_index].primary, req, resp);
            ASSERT_EQ(errinfo, dsn::ERR_OK);
            ASSERT_EQ(resp.err_hint,
                      "Can't get hotkey now, now state: hotkey_collector_state::STOPPED");
        }
    }

    void write_random_data()
    {
        int64_t start = dsn_now_s();
        int err = PERR_OK;
        ASSERT_NE(pg_client, nullptr);

        for (int i = 0; dsn_now_s() - start < max_detection_second; ++i %= 1000) {
            std::string index = std::to_string(i);
            std::string h_key = generate_hash_key_by_random(false, 0);
            std::string s_key = "sortkey_" + index;
            std::string value = "value_" + index;
            err = pg_client->set(h_key, s_key, value);
            ASSERT_EQ(err, PERR_OK);
        }

        int32_t app_id;
        int32_t partition_count;
        std::vector<dsn::partition_configuration> partitions;
        ddl_client->list_app(app_name, app_id, partition_count, partitions);
        dsn::replication::detect_hotkey_response resp;
        dsn::replication::detect_hotkey_request req;
        req.type = dsn::replication::hotkey_type::type::WRITE;
        req.action = dsn::replication::detect_action::QUERY;
        bool find_hotkey = false;
        int partition_index;
        for (partition_index = 0; partition_index < partitions.size(); partition_index++) {
            req.pid = dsn::gpid(app_id, partition_index);
            auto errinfo =
                ddl_client->detect_hotkey(partitions[partition_index].primary, req, resp);
            ASSERT_EQ(errinfo, dsn::ERR_OK);
            ASSERT_EQ(resp.err_hint,
                      "Can't get hotkey now, now state: hotkey_collector_state::STOPPED");
        }
    }

    void capture_until_maxtime()
    {
        int64_t start = dsn_now_s();
        dsn::replication::detect_hotkey_response resp;
        dsn::replication::detect_hotkey_request req;
        int target_partition = 2;
        int32_t app_id;
        int32_t partition_count;
        std::vector<dsn::partition_configuration> partitions;

        req.type = dsn::replication::hotkey_type::type::WRITE;
        req.action = dsn::replication::detect_action::START;
        ddl_client->list_app(app_name, app_id, partition_count, partitions);
        req.pid = dsn::gpid(app_id, target_partition);
        auto errinfo = ddl_client->detect_hotkey(partitions[target_partition].primary, req, resp);
        ASSERT_EQ(errinfo, dsn::ERR_OK);
        ASSERT_EQ(resp.err, dsn::ERR_OK);

        req.action = dsn::replication::detect_action::QUERY;
        errinfo = ddl_client->detect_hotkey(partitions[target_partition].primary, req, resp);
        ASSERT_EQ(resp.err_hint,
                  "Can't get hotkey now, now state: hotkey_collector_state::COARSE_DETECTING");

        // max_detection_second > max_seconds_to_detect_hotkey
        int max_seconds_to_detect_hotkey = 160;
        for (int i = 0; dsn_now_s() - start < max_seconds_to_detect_hotkey; ++i %= 1000) {
            std::string index = std::to_string(i);
            std::string h_key = generate_hash_key_by_random(false, 0);
            std::string s_key = "sortkey_" + index;
            std::string value = "value_" + index;
            auto err = pg_client->set(h_key, s_key, value);
            ASSERT_EQ(err, PERR_OK);
        }

        req.action = dsn::replication::detect_action::QUERY;
        errinfo = ddl_client->detect_hotkey(partitions[target_partition].primary, req, resp);
        ASSERT_EQ(resp.err_hint,
                  "Can't get hotkey now, now state: hotkey_collector_state::STOPPED");
    }

    void read_hotspot_data()
    {
        int64_t start = dsn_now_s();
        int err = PERR_OK;
        ASSERT_NE(pg_client, nullptr);

        const int64_t warmup_second = 30;
        for (int i = 0; dsn_now_s() - start < warmup_second; ++i %= 1000) {
            std::string index = std::to_string(i);
            std::string h_key = generate_hash_key_by_random(false, 0);
            std::string s_key = "sortkey_" + index;
            std::string value = "value_" + index;
            err = pg_client->get(h_key, s_key, value);
            ASSERT_TRUE((err == PERR_OK) || err == (PERR_NOT_FOUND));
        }

        for (int i = 0; dsn_now_s() - start < max_detection_second; ++i %= 1000) {
            std::string index = std::to_string(i);
            std::string h_key = generate_hash_key_by_random(true, 50);
            std::string s_key = "sortkey_" + index;
            std::string value;
            err = pg_client->get(h_key, s_key, value);
            ASSERT_TRUE((err == PERR_OK) || err == (PERR_NOT_FOUND));
        }

        int32_t app_id;
        int32_t partition_count;
        std::vector<dsn::partition_configuration> partitions;
        ddl_client->list_app(app_name, app_id, partition_count, partitions);
        dsn::replication::detect_hotkey_response resp;
        dsn::replication::detect_hotkey_request req;
        req.type = dsn::replication::hotkey_type::type::READ;
        req.action = dsn::replication::detect_action::QUERY;
        bool find_hotkey = false;
        int partition_index;
        for (partition_index = 0; partition_index < partitions.size(); partition_index++) {
            req.pid = dsn::gpid(app_id, partition_index);
            auto errinfo =
                ddl_client->detect_hotkey(partitions[partition_index].primary, req, resp);
            ASSERT_EQ(errinfo, dsn::ERR_OK);
            if (resp.hotkey_result.empty()) {
                find_hotkey = true;
                break;
            }
        }
        ASSERT_EQ(resp.err_hint,
                  "Can't get hotkey now, now state: hotkey_collector_state::FINISHED");
        ASSERT_TRUE(find_hotkey);
        ASSERT_EQ(resp.err, dsn::ERR_OK);

        ASSERT_EQ(resp.hotkey_result, "ThisisahotkeyThisisahotkey");

        // Wait for collector sending the next start detecting command
        sleep(15);

        req.action = dsn::replication::detect_action::STOP;
        auto errinfo = ddl_client->detect_hotkey(partitions[partition_index].primary, req, resp);
        ASSERT_EQ(errinfo, dsn::ERR_OK);
        ASSERT_EQ(resp.err, dsn::ERR_OK);
    }

    void read_random_data()
    {
        int64_t start = dsn_now_s();
        int err = PERR_OK;
        ASSERT_NE(pg_client, nullptr);

        for (int i = 0; dsn_now_s() - start < max_detection_second; ++i %= 1000) {
            std::string index = std::to_string(i);
            std::string h_key = generate_hash_key_by_random(false, 0);
            std::string s_key = "sortkey_" + index;
            std::string value;
            err = pg_client->get(h_key, s_key, value);
            ASSERT_TRUE((err == PERR_OK) || err == (PERR_NOT_FOUND));
        }

        int32_t app_id;
        int32_t partition_count;
        std::vector<dsn::partition_configuration> partitions;
        ddl_client->list_app(app_name, app_id, partition_count, partitions);
        dsn::replication::detect_hotkey_response resp;
        dsn::replication::detect_hotkey_request req;
        req.type = dsn::replication::hotkey_type::type::READ;
        req.action = dsn::replication::detect_action::QUERY;
        bool find_hotkey = false;
        int partition_index;
        for (partition_index = 0; partition_index < partitions.size(); partition_index++) {
            req.pid = dsn::gpid(app_id, partition_index);
            auto errinfo =
                ddl_client->detect_hotkey(partitions[partition_index].primary, req, resp);
            ASSERT_EQ(errinfo, dsn::ERR_OK);
            ASSERT_EQ(resp.err_hint,
                      "Can't get hotkey now, now state: hotkey_collector_state::STOPPED");
        }
    }

    const std::string app_name = "hotspot_test";
    const int64_t max_detection_second = 100;
    std::shared_ptr<replication_ddl_client> ddl_client;
    pegasus::pegasus_client *pg_client;
};

// TEST_F(test_detect_hotspot, write_hotspot_data)
// {
//     std::cout << "start testing write_hotspot_data..." << std::endl;
//     write_hotspot_data();
//     std::cout << "hotspot passed....." << std::endl;
// }

// TEST_F(test_detect_hotspot, write_random_data)
// {
//     std::cout << "start testing write_random_data..." << std::endl;
//     write_random_data();
//     std::cout << "hotspot passed....." << std::endl;
// }

// TEST_F(test_detect_hotspot, capture_until_maxtime)
// {
//     std::cout << "start testing capture_until_maxtime..." << std::endl;
//     capture_until_maxtime();
//     std::cout << "hotspot passed....." << std::endl;
// }

TEST_F(test_detect_hotspot, read_hotspot_data)
{
    std::cout << "start testing read_hotspot_data..." << std::endl;
    read_hotspot_data();
    std::cout << "hotspot passed....." << std::endl;
}

// TEST_F(test_detect_hotspot, read_random_data)
// {
//     std::cout << "start testing read_random_data..." << std::endl;
//     read_random_data();
//     std::cout << "hotspot passed....." << std::endl;
// }
