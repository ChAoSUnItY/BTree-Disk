#include "mytree.hpp"
#include "database.hpp"
#include "common.hpp"

#include "fmt/format.h"
#include "nlohmann/json.hpp"

#include <filesystem>
#include <exception>
#include <fstream>

namespace fs = std::filesystem;
using json = nlohmann::json;

const regex FIELD_INFO_VALIDATION_REGEX("\\(\\s*(\\w+)\\s*(\\w+)\\s*(\\(\\d+\\)\\s*)?(,\\s*(\\w+)\\s*(\\w+)\\s*(\\(\\d+\\)\\s*)?)*\\)");
const regex PER_FIELD_INFO_REGEX("(\\w+)\\s*(\\w+)\\s*(\\((\\d+)\\))?");

Table::Table(const string& name, const string& fieldInfo) {
    this->option = new TableOption(name);

    // Analyze field info
    smatch match;
    ssub_match sm;

    if (!regex_match(fieldInfo, match, FIELD_INFO_VALIDATION_REGEX))
        throw runtime_error("Invalid field info syntax");

    int previousPos = 0;

    while (regex_search(fieldInfo.begin() + previousPos, fieldInfo.end(), match, PER_FIELD_INFO_REGEX)) {
        string fieldName = match.str(1);
        string dataType = match.str(2);
        int size;

        if (dataType == "int") {
            size = 4;
        } else {
            size = stoi(match.str(4));
        }

        this->option->field_info.push_back(FieldTypeInfo(fieldName, dataType, size));

        previousPos = (int) (match[0].second - fieldInfo.begin());
    }

    // Create table folder & table's metainfo
    fs::path tableFolder(fmt::format("./database/{}", name));

    if (!fs::is_directory(tableFolder))
        fs::create_directory(tableFolder);

    json metaInfo;

    this->create_primary_index(DEFAULT_PK);
    metaInfo["pk"] = this->pk;

    json fieldInfoJson;

    for (auto info : this->option->field_info) {
        json field;
        field["type"] = get<1>(info);
        field["size"] = get<2>(info);
        fieldInfoJson[get<0>(info)] = field;
    }

    metaInfo["fieldInfo"] = fieldInfoJson;

    json index;

    index.push_back(this->pk);

    metaInfo["index"] = index;

    fstream output_stream(fmt::format("./database/{}/meta_info.json", name), ios_base::out);
    output_stream << metaInfo.dump(4) << '\n';
    output_stream.close();
}

Table::Table(const string& name) {
    this->option = new TableOption(name);

    fs::path tableFolder(fmt::format("./database/{}", name));

    fstream input_stream(fmt::format("./database/{}/meta_info.json", name), ios_base::in);

    json metaInfo = json::parse(input_stream);

    json fieldInfo = metaInfo["fieldInfo"];

    for (auto& [key, value] : fieldInfo.items()) {
        string type = value["type"].get<string>();
        int size = value["type"].get<int>();
        this->option->field_info.push_back(FieldTypeInfo(key, type, size));
    }

    this->pk = metaInfo["pk"];

    json index = metaInfo["index"];

    for (auto& [key, value] : index.items()) {
        // this->IndexMap[value.get<string>()] = new BTree(value.get<string>(), this->data_page_mgr, this->option);
    }
}

void Table::create_primary_index(const string& pk) {
    this->pk = pk;
    int degree = (DEFAULT_PAGE_SIZE - 100) / (sizeof(int) + sizeof(long)) / 2 ;
    this->IndexMap[pk] = new BTree(pk, degree, 0, this->data_page_mgr, this->option);
}

BTree::BTree(const string& index_name, int degree, int key_field_len, shared_ptr <DataPageMgr> data_page_mgr, TableOption* table_option) {
    this->table_option = table_option;
    this->header.degree = degree;
    this->header.key_field_len = key_field_len;
    this->index_name = index_name;

    this->root = new BTreeNode(0, degree, key_field_len, true, true);
    this->header.count++;

    this->btree_page_mgr = std::make_shared<BtreePageMgr>(fmt::format("./database/{}/{}_index/btree_file", this->table_option->table_name, this->index_name));
    this->btree_page_mgr->save_header(this->header);
    this->btree_page_mgr->save_node(0, *this->root);
}

BTree::BTree(const string& index_name, shared_ptr <DataPageMgr> data_page_mgr, TableOption* table_option) {
    this->table_option = table_option;
    this->index_name = index_name;
    this->btree_page_mgr = std::make_shared<BtreePageMgr>(fmt::format("./database/{}/{}_index/btree_file", this->table_option->table_name, this->index_name));
    this->btree_page_mgr->get_header(this->header);
    this->btree_page_mgr->get_node(this->header.root_id, this->root);
}

BTreeNode::BTreeNode(long id, int degree, int key_field_len, bool is_root, bool is_leaf) {
    this->header.traversal_id = id;
    this->header.degree = degree;
    this->header.key_field_len = key_field_len;
    this->header.is_root = is_root;
    this->header.is_leaf = is_leaf;
}

BtreePageMgr::BtreePageMgr(const string& filename, bool trunc) : fstream(filename.data(), ios::in | ios::out | ios::binary) {
    this->empty = false;
    this->filename = filename;
    if (!this->good() || trunc) {
        this->empty = true;
        this->open(filename.data(), std::ios::in | std::ios::out | std::ios::trunc | std::ios::binary);
    }
}

BtreePageMgr::~BtreePageMgr() {
    this->close();
}

template <class header_data>
void BtreePageMgr::save_header(header_data &header) {
    this->clear();
    this->seekp(0, ios::beg);
    this->write(reinterpret_cast<char *>(&header), sizeof(header));
    this->header_prefix = sizeof(header_data);
}

template <class header_data>
bool BtreePageMgr::get_header(header_data &header) {
    this->clear();
    this->seekg(0, ios::beg);
    this->read(reinterpret_cast<char *>(&header), sizeof(header));
    return this->gcount() > 0;
}

template <class btree_node>
void BtreePageMgr::save_node(const long &n, btree_node &node) {
    if (node.header.is_leaf) {
        this->seekp(n, ios::beg);
    } else {

    }
}

template <class btree_node>
bool BtreePageMgr::get_node(const long &n, btree_node &node) {
    return false;
}

template <class header_data>
void DataPageMgr::save_header(header_data &header) {
    this->clear();
    this->seekp(0, ios::beg);
    this->write(reinterpret_cast<char *>(&header), sizeof(header));
    this->header_prefix = sizeof(header_data);
}

template <class header_data>
bool DataPageMgr::get_header(header_data &header) {
    this->clear();
    this->seekg(0, ios::beg);
    this->read(reinterpret_cast<char *>(&header), sizeof(header));
    return this->gcount() > 0;
}

DataPageMgr::DataPageMgr(const string& filename, bool trunc) : fstream(filename.data(), ios::in | ios::out | ios::binary) {
    this->empty = false;
    this->filename = filename;
    if (!this->good() || trunc) {
        this->empty = true;
        this->open(filename.data(), std::ios::in | std::ios::out | std::ios::trunc | std::ios::binary);
    }
}

DataPageMgr::~DataPageMgr() {
    this->close();
}
