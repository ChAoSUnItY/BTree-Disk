#include "database.hpp"

#include <exception>
#include <filesystem>
#include <iostream>
#include <exception>

#include "fmt/format.h"

namespace fs = std::filesystem;

DataBase* db = NULL;

void DataBase::init() {
    filesystem::path outputDir(DB_OUTPUT_DIR);
    if (!filesystem::is_directory(outputDir))
        filesystem::create_directory(outputDir);
}

void DataBase::createTable(const smatch& match) {
    string name = match[2].str();

    if (db->tables.find(name) != db->tables.end())
        throw runtime_error(fmt::format("Table {} is already defined.", name));

    db->tables[name] = new Table(name, DEFAULT_FIELD);
}
void DataBase::useTable(const smatch& match) {
    string name = match[2].str();

    if (db->tables.find(name) == db->tables.end()) {
        db->tables[name] = new Table(name);
    }

    db->current_table = db->tables[name];
}

void DataBase::setIndex(const smatch& match) {

}

void DataBase::insert(const smatch& match) {
    string file_name = match[2].str();
    fs::path file_path(file_name.c_str());

    if (!fs::exists(file_path)) {
        throw runtime_error(fmt::format("File {} does not exist", file_name));
    }

    ifstream data_file(file_name);

    if (!data_file.is_open()) {
        throw runtime_error(fmt::format("Cannot open file {}", file_name));
    }

    string line;
    while (getline(data_file, line)) {
        try {
            json line_json = json::parse(line);
            this->current_table->insert_data(line_json);
        } catch (exception &e) {
            cout << e.what() << endl;
        }
    }
}

/*json*/void DataBase::query(const smatch& match) {

}

/*remove*/void DataBase::remove(const smatch& match) {
    
}
