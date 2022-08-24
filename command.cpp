#include <iostream>
#include <string>
#include <regex>

#include "command.hpp"
#include "database.hpp"

using namespace std;

const regex regexes[7] = {
        regex("^\\s*(create)\\s*table\\s*(\\w+)\\s*$"),
        regex("^\\s*(use)\\s*table\\s*(\\w+)\\s*$"),
        regex("^\\s*(set)\\s*index\\s*(\\w+)\\s*$"),
        regex("^\\s*(insert)\\s*(\\S+?)\\s*$"),
        regex("^\\s*(query)\\s*(\\w+)\\s*$"),
        regex("^\\s*(remove)\\s*(\\w+)\\s*$"),
        regex("^\\s*(quit)")};

void parseCommand(string input)
{
    smatch match;

    for (int i = 0; i < 7; i++)
    {
        if (!regex_match(input, match, regexes[i]))
            continue;
        auto firstMatch = match.str(1);

        if (firstMatch == "create")
        {
            auto name = match.str(2);
            cout << "Create command" << endl;
            cout << "Name: " << name << endl;
            db->createTable(match);
        }
        else if (firstMatch == "use")
        {
            auto name = match.str(2);
            cout << "Use command" << endl;
            cout << "Name: " << name << endl;
            db->useTable(match);
        }
        else if (firstMatch == "set")
        {
            auto name = match.str(2);
            cout << "Set command" << endl;
            cout << "Name: " << name << endl;
            db->setIndex(match);
        }
        else if (firstMatch == "insert")
        {
            auto name = match.str(2);
            cout << "Insert command" << endl;
            cout << "Name: " << name << endl;
            db->insert(match);
        }
        else if (firstMatch == "query")
        {
            auto expr = match.str(2);
            cout << "Query command" << endl;
            cout << "Expr: " << expr << endl;
            db->query(match);
        }
        else if (firstMatch == "remove")
        {
            auto expr = match.str(2);
            cout << "Remove command" << endl;
            cout << "Expr: " << expr << endl;
            db->remove(match);
        }
        else if (firstMatch == "quit") exit(0);
        else continue;

        return;
    }

    cerr << "Invalid command '" << input << "'" << endl;
}
