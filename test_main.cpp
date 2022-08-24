#include "myrandom.hpp"
#include "mytree.hpp"
#include "command.hpp"
#include "database.hpp"

#include "fmt/format.h"
#include "nlohmann/json.hpp"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <iostream>
#include <string>
#include <regex>

using json = nlohmann::json;
using namespace fmt;
using namespace std;

void test1() {
    db = new DataBase();
    db->init();

    parseCommand(string("delete table test"));
    parseCommand(string("create table test"));
    parseCommand(string("use table test"));
    parseCommand(string("insert ../test_case/test1.json"));
    //parseCommand(string("create index example_id"));
    //parseCommand(string("create index example_str"));
    parseCommand(string("clear_b table test"));

    json btree_node_json;
    btree_node_json = db->get_btree_node_info("test", "_id", 0);
    btree_node_json = db->get_btree_node_info("test", "_id", 1);
    btree_node_json = db->get_btree_node_info("test", "_id", 2);

    btree_node_json = db->get_btree_node_info("test", "example_str", 0);
    btree_node_json = db->get_btree_node_info("test", "example_str", 1);
    btree_node_json = db->get_btree_node_info("test", "example_str", 2);

    btree_node_json = db->get_btree_node_info("test", "example_id", 0);
    btree_node_json = db->get_btree_node_info("test", "example_id", 1);
    btree_node_json = db->get_btree_node_info("test", "example_id", 2);

    delete db;
}

void test2() {
    db = new DataBase();
    db->init();

    parseCommand(string("delete table test2"));
    parseCommand(string("create table test2"));
    parseCommand(string("use table test2"));
    parseCommand(string("create index example_id"));
    parseCommand(string("create index example_str"));
    parseCommand(string("insert ../test_case/test2.json"));
    parseCommand(string("clear_b table test2"));

    delete db;
}

int main(int argc, char **argv) {
    test1();
    // test2();
}
