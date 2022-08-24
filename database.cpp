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

    if (this->tables.find(name) != this->tables.end())
        throw runtime_error(fmt::format("Table {} is already defined.", name));

    this->tables[name] = new Table(name, DEFAULT_FIELD);
}
void DataBase::useTable(const smatch& match) {
    string name = match[2].str();

    if (this->tables.find(name) == this->tables.end()) {
        this->tables[name] = new Table(name);
    }

    this->current_table = this->tables[name];
}

void DataBase::clearbTable(const smatch& match) {
    string name = match[2].str();
    
    if (this->tables.find(name) == this->tables.end()) {
        auto table = this->tables[name];
        
        delete table;
        this->tables.erase(name);
    }
}

void DataBase::deleteTable(const smatch& match) {
    string name = match[2].str();

    if (this->tables.find(name) == this->tables.end()) {
        auto table = this->tables[name];

        std::filesystem::path folder_path = fmt::format("./database/{}", table->option->table_name);

        if (is_directory(folder_path)) {
            remove_all(folder_path);
        }

        delete table;
        this->tables.erase(name);
    }
}

json DataBase::get_btree_node_info(const string& table_name, const string& index_name, const long& n) {
    json holder;

    if (this->tables.find(table_name) == this->tables.end()) {
        auto table = this->tables[table_name];
        auto btree = table->get_index(index_name);
        auto node = new BTreeNode();
        btree->btree_page_mgr->get_node(n, *node, table->option);

        json header = {
                {"traversal_id", node->header.traversal_id},
                {"degree", node->header.degree},
                {"key_field_len", node->header.key_field_len},
                {"is_root", node->header.is_root},
                {"is_leaf", node->header.is_leaf},
                {"right", node->header.right},
                {"key_count", node->header.key_count}
        };

        holder["header"] = header;

        json keys;

        for (struct BtreeKey key : node->keys) {
            json json_key = {
                    {"data", key.data == nullptr ? "" : string(key.data)},
                    {"_id", key._id}
            };

            keys.push_back(json_key);
        }

        holder["keys"] = keys;

        json children;

        for (auto child : node->children) {
            children.push_back(children);
        }

        holder["children"] = children;
    }

    return holder;
}

void DataBase::setIndex(const smatch& match) {
    if (this->current_table == nullptr) {
        throw runtime_error("Current table does not exist");
    }

    this->current_table->create_index(match[2].str());
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
