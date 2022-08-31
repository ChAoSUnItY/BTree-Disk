#ifndef DATABASE
#define DATABASE

#include "mytree.hpp"

#include <regex>
#include <string>
#include <filesystem>
#include <map>

#define DB_OUTPUT_DIR "./database"

using namespace std;

class DataBase;

extern DataBase* db;

class DataBase {
private:
    map<string, Table*> tables;
    Table* current_table;

public:
    int default_degree{-1};

    void init();

    void createTable(const smatch& match);
    void useTable(const smatch& match);
    void clearbTable(const smatch& match);
    void deleteTable(const smatch& match);
    json get_btree_node_info(const string& table_name, const string& index_name, const long& n);
    void setIndex(const smatch& match);
    void insert(const smatch& match);
    /*json*/void query(const smatch& match);
    /*remove*/void remove(const smatch& match);
};

#endif