// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <server/pegasus_hotkey_collector.h>

#include "base/pegasus_key_schema.h"
#include "message_utils.h"
#include <gtest/gtest.h>
#include <stdlib.h>
#include <dsn/utility/rand.h>
#include <dsn/utility/defer.h>

namespace pegasus {
namespace server {

int position = 0;

std::string hotkey_generator(bool is_hotkey)
{
    if (is_hotkey && rand() % 2) {
        return "AAAAAAAAAAAAAAAAAAAA";
    } else {
        const char CCH[] = "_0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_";
        const int len = strlen(CCH);
        std::string result = "";
        int index;
        for (int i = 0; i < 20; i++) {
            index = rand() % len;
            result += CCH[index];
        }
        return result;
    }
}

TEST(hotkey_detect_test, find_hotkey)
{
    srand((unsigned)time(NULL));
    std::string result;
    std::unique_ptr<hotkey_collector> collector(new hotkey_collector);
    std::vector<std::thread> workers, workers1;

    clock_t time_start = clock();

    // test hotkey_collector::init()
    ASSERT_EQ(collector->get_status(), "STOP");
    ASSERT_TRUE(collector->init());
    ASSERT_FALSE(collector->init());

    // test capture 0 hotspot && blob data
    ASSERT_EQ(collector->get_status(), "COARSE");
    for (int i = 0; i < 3; i++) {
        workers.emplace_back(std::thread([&]() {
            dsn::blob key;
            for (int j = 0; j < 10000; j++) {
                std::string hashkey = hotkey_generator(false);
                pegasus_generate_key(key, hashkey, std::string("sortAAAAAAAAAAAAAAAA"));
                collector->capture_blob_data(key);
                if (i == 0 && j % 1000 == 0) {
                    collector->analyse_data();
                }
            }
        }));
    }
    std::for_each(workers.begin(), workers.end(), [](std::thread &t) { t.join(); });

    // test automatic destruction
    collector->kMaxTime_sec = 0;
    collector->analyse_data();
    ASSERT_EQ(collector->get_status(), "STOP");
    ASSERT_EQ(collector->get_result(result), false);
    collector->kMaxTime_sec = 45;
    ASSERT_TRUE(collector->init());

    // test one hotkey with random data
    ASSERT_EQ(collector->get_status(), "COARSE");
    for (int i = 0; i < 3; i++) {
        workers1.emplace_back(std::thread([&]() {
            dsn::blob key;
            for (int j = 0; j < 10000; j++) {
                std::string hashkey = hotkey_generator(false);
                pegasus_generate_key(key, hashkey, std::string("sortAAAAAAAAAAAAAAAA"));
                collector->capture_blob_data(key);
                if (i == 0 && j % 1000 == 0) {
                    collector->analyse_data();
                }
            }
        }));
    }
    std::for_each(workers1.begin(), workers1.end(), [](std::thread &t) { t.join(); });

    return;

    ASSERT_EQ(collector->get_status(), "FINISH");
    ASSERT_EQ(collector->get_result(result), true);
    ASSERT_EQ(result, "AAAAAAAAAAAAAAAAAAAA");

    ASSERT_TRUE(collector->init());
    ASSERT_EQ(collector->get_status(), "COARSE");

    // test only one key in the data sample
    for (int i = 0; i < 10000; i++) {
        dsn::blob key;
        pegasus_generate_key(
            key, std::string("hashAAAAAAAAAAAAAAAA"), std::string("sortAAAAAAAAAAAAAAAA"));
        dsn::apps::update_request req;
        req.key = key;
        req.value.assign("value", 0, 5);
        int put_rpc_cnt = dsn::rand::next_u32(1, 10);
        int remove_rpc_cnt = dsn::rand::next_u32(1, 10);
        int total_rpc_cnt = put_rpc_cnt + remove_rpc_cnt;
        auto writes = new dsn::message_ex *[total_rpc_cnt];
        for (int i = 0; i < put_rpc_cnt; i++) {
            writes[i] = pegasus::create_put_request(req);
        }
        for (int i = put_rpc_cnt; i < total_rpc_cnt; i++) {
            writes[i] = pegasus::create_remove_request(key);
        }
        auto cleanup = dsn::defer([=]() { delete[] writes; });
        collector->capture_msg_data(writes, total_rpc_cnt);
        if (i % 1000 == 0) {
            collector->analyse_data();
        }
    }
    ASSERT_EQ(collector->get_status(), "FINISH");
    ASSERT_EQ(collector->get_result(result), true);
    ASSERT_EQ(result, "hashAAAAAAAAAAAAAAAA");
    collector->clear();
    ASSERT_EQ(collector->get_status(), "STOP");

    clock_t time_end = clock();
    std::cout << "time use:" << 1000 * (time_end - time_start) / (double)CLOCKS_PER_SEC << "ms"
              << std::endl;
}

} // namespace server
} // namespace pegasus
