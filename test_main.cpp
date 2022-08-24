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

#define MAX_LENGTH 10000
#define DEFAULT_LENGTH 16
#define DEFAULT_RANGE 1000
#define DEFAULT_DEGREE 2

void test() {
    db = new DataBase();
    db->init();

    parseCommand("create table test");
    parseCommand("use table test");
    parseCommand("insert C:\\Users\\chaos\\projects\\BTree-Disk\\test_case\\test1.json");

    delete db;
}

void test2() {
    db = new DataBase();
    db->init();
    delete db;
}

int main(int argc, char *argv[])
{
    test();

    return 0;
}
