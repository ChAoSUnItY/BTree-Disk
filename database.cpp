#include "database.hpp"

#include <exception>
#include "fmt/format.h"

DataBase* db = new DataBase();

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

}

/*json*/void DataBase::query(const smatch& match) {

}

/*remove*/void DataBase::remove(const smatch& match) {
    
}
