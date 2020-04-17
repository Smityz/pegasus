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
        return "AAAAAAAAAA";
    } else {
        const char CCH[] = "_0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_";
        const int len = strlen(CCH);
        std::string result = "";
        int index;
        for (int i = 0; i < 10; i++) {
            index = rand() % len;
            result += CCH[index];
        }
        return result;
    }
}

TEST(hotkey_detect_test, find_hotkey)
{
    srand(1);
    std::unique_ptr<hotkey_collector> collector(new hotkey_collector);

    // test hotkey_collector::init()
    ASSERT_EQ(collector->get_status(), "STOP");
    ASSERT_TRUE(collector->init());
    ASSERT_FALSE(collector->init());

    // test capture read data
    ASSERT_EQ(collector->get_status(), "COARSE");
    dsn::blob key;
    for (int i = 0; i < 1000000; i++) {
        pegasus_generate_key(key, hotkey_generator(false), std::string("sort"));
        collector->capture_blob_data(key);
    }
    ASSERT_EQ(collector->get_status(), "COARSE");
    collector->analyse_data();
    ASSERT_EQ(collector->get_status(), "COARSE");

    for (int i = 0; i < 1000000; i++) {
        pegasus_generate_key(key, hotkey_generator(true), std::string("sort"));
        collector->capture_blob_data(key);
        if (i % 10000 == 0) {
            collector->analyse_data();
        }
    }
    ASSERT_EQ(collector->get_status(), "FINISH");
    std::string result;
    ASSERT_EQ(collector->get_result(result), true);
    ASSERT_EQ(result, "AAAAAAAAAA");

    ASSERT_TRUE(collector->init());
    ASSERT_EQ(collector->get_status(), "COARSE");

    for (int i = 0; i < 100000; i++) {
        dsn::blob key;
        pegasus_generate_key(key, std::string("hash"), std::string("sort"));
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
        if (i % 10000 == 0) {
            collector->analyse_data();
        }
    }
    ASSERT_EQ(collector->get_status(), "FINISH");
    ASSERT_EQ(collector->get_result(result), true);
    ASSERT_EQ(result, "hash");
    collector->clear();
    ASSERT_EQ(collector->get_status(), "STOP");
}

} // namespace server
} // namespace pegasus
