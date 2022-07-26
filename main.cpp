
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

int main(int argc, char *argv[])
{
    db = new DataBase();
    db->init();
    string input;
    while (true)
    {
        cout << "db >";
        getline(cin, input);

        if (input.size() == 0)
            continue;

        parseCommand(input);
    }

    delete db;
    return 0;
}
