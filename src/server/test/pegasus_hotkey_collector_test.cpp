// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <server/pegasus_hotkey_collector.h>

#include "base/pegasus_key_schema.h"
#include <gtest/gtest.h>
#include <stdlib.h>

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
        collector->capture_data(key);
    }
    ASSERT_EQ(collector->get_status(), "COARSE");
    collector->analyse_data();
    ASSERT_EQ(collector->get_status(), "COARSE");

    for (int i = 0; i < 1000000; i++) {
        pegasus_generate_key(key, hotkey_generator(true), std::string("sort"));
        collector->capture_data(key);
        if (i % 10000 == 0) {
            collector->analyse_data();
        }
    }
    ASSERT_EQ(collector->get_status(), "STOP");
    collector->clear();
}

} // namespace server
} // namespace pegasus
