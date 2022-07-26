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

const regex FIELD_INFO_VALIDATION_REGEX(
        "\\(\\s*(\\w+)\\s*(\\w+)\\s*(\\(\\d+\\)\\s*)?(,\\s*(\\w+)\\s*(\\w+)\\s*(\\(\\d+\\)\\s*)?)*\\)");
const regex PER_FIELD_INFO_REGEX("(\\w+)\\s*(\\w+)\\s*(\\((\\d+)\\))?");

Table::Table(const string &name, const string &fieldInfo, int default_degree) {
    this->option = new TableOption(name);

    // Analyze field info
    smatch match;
    ssub_match sm;

    if (!regex_match(fieldInfo, match, FIELD_INFO_VALIDATION_REGEX))
        throw runtime_error("Invalid field info syntax");

    this->default_degree = default_degree;

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

    this->data_page_mgr = std::make_shared<DataPageMgr>(fmt::format("./database/{}/table_data.bin", name));

    json metaInfo;

    this->create_primary_index(DEFAULT_PK);
    metaInfo["pk"] = this->pk;

    json fieldInfoJson;

    for (auto info: this->option->field_info) {
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

Table::Table(const string &name, int default_degree) {
    this->option = new TableOption(name);
    this->default_degree = default_degree;

    fs::path tableFolder(fmt::format("./database/{}", name));

    fstream input_stream(fmt::format("./database/{}/meta_info.json", name), ios_base::in);
    this->data_page_mgr = std::make_shared<DataPageMgr>(fmt::format("./database/{}/table_data.bin", name));
    json metaInfo = json::parse(input_stream);

    json fieldInfo = metaInfo["fieldInfo"];

    for (auto &[key, value]: fieldInfo.items()) {
        string type = value["type"].get<string>();
        int size = value["size"].get<int>();
        this->option->field_info.emplace_back(key, type, size);
    }

    this->pk = metaInfo["pk"];

    json index = metaInfo["index"];

    for (auto &[key, value]: index.items()) {
        this->IndexMap[value.get<string>()] = new BTree(value.get<string>(), this->data_page_mgr, this->option);
    }
}

void Table::create_primary_index(const string &pk) {
    this->pk = pk;
    int degree, page_size;
    if (this->default_degree == -1) {
        page_size = DEFAULT_PAGE_SIZE;
        degree = (page_size - 100) / (sizeof(int) + sizeof(long)) / 2;
    } else {
        degree = this->default_degree;
        page_size = degree * 2 * (sizeof(int) + sizeof(long)) + 100;
    }
    this->IndexMap[pk] = new BTree(pk, degree, page_size, FieldType::INTEGER, 0, this->data_page_mgr, this->option);
}

void Table::insert_data(json &json_data) {
    this->data_header.count++;
    this->data_page_mgr->save_header(this->data_header);
    this->data_page_mgr->save_node(this->data_header.count - 1, json_data, this->option->field_info);

    if (this->pk == DEFAULT_PK) {
        json_data[this->pk] = this->data_header.count;
    }

    int pk = json_data[this->pk];

    for (auto &elem: json_data.items()) { /// todo 萬一出現 exception, data_header 要有 rollback 機制
        string json_key = elem.key();

        if (json_key == this->pk) {
            struct BtreeKey btree_key{nullptr, pk};
            this->IndexMap[this->pk]->insert_key(btree_key, this->data_header.count - 1);
        } else if (this->IndexMap.find(json_key) != this->IndexMap.end()) {
            struct BtreeKey btree_key{nullptr, pk};
            string json_value = elem.value().get<string>();

            int field_size;
            for (auto &item: this->option->field_info) {
                if (json_key == get<0>(item)) {
                    field_size = get<2>(item);
                }
            }

            btree_key.data = new char[field_size]();
            strncpy(btree_key.data, json_value.c_str(), field_size);
            this->IndexMap[json_key]->insert_key(btree_key, this->data_header.count - 1);
        }
    }
}

void Table::create_index(const string &index_name) {
    if (this->IndexMap.contains(index_name)) {
        throw runtime_error(
                fmt::format("Current table {} already contains index {}", this->option->table_name, index_name));
    }

    bool create_new_index = false;
    int key_data_size;
    string key_data_type;
    for (auto &item: this->option->field_info) {
        string field_name = std::get<0>(item);
        string field_type = std::get<1>(item);
        int field_size = std::get<2>(item);

        if (index_name == field_name) {
            int key_field_len = field_size;
            key_data_size = field_size;
            key_data_type = field_type;
            FieldType key_field_type;

            if (field_type == "int") {
                key_field_type = FieldType::INTEGER;
            } else if (field_type == "char") {
                key_field_type = FieldType::CHAR;
            }

            int degree, page_size;
            if (this->default_degree == -1) {
                page_size = DEFAULT_PAGE_SIZE;
                degree = (page_size - 100) / (sizeof(int) + key_field_len + sizeof(long)) / 2;
            } else {
                degree = this->default_degree;
                page_size = degree * 2 * (sizeof(int) + key_field_len + sizeof(long)) + 100;
            }
            this->IndexMap[index_name] = new BTree(index_name, degree, page_size, key_field_type, key_field_len,
                                                   this->data_page_mgr, this->option);
            create_new_index = true;
            break;
        }
    }

    if (!create_new_index) {
        throw runtime_error(fmt::format("index {} does not match", index_name));
    }

    for (long i = 0; i < this->data_header.count; ++i) {
        json json_data;
        this->data_page_mgr->get_node(i, json_data, this->option->field_info);

//        if (!json_data["exist"]) {
//            continue;
//        }
//
//        json_data.erase("exist");

        struct BtreeKey btree_key{nullptr, i};

        btree_key.data = new char[key_data_size]();
        if (key_data_type == "int") {
            int json_raw_value = json_data[index_name].get<int>();
            strncpy(btree_key.data, reinterpret_cast<char *>(&json_raw_value), key_data_size);
        } else if (key_data_type == "char") {
            string json_raw_value = json_data[index_name].get<string>();
            strncpy(btree_key.data, json_raw_value.c_str(), key_data_size);
        }
        this->IndexMap[index_name]->insert_key(btree_key, i);
    }
}

BTree::BTree(const string &index_name, int degree, int page_size, FieldType type, int key_field_len,
             shared_ptr<DataPageMgr> data_page_mgr,
             TableOption *table_option) {
    this->table_option = table_option;
    this->header.fieldType = type;
    this->header.degree = degree;
    this->header.page_size = page_size;
    this->header.key_field_len = key_field_len;
    this->index_name = index_name;

    this->root = new BTreeNode(0, degree, page_size, key_field_len, true, true);
    this->header.count++;

    if (index_name == "_id") {
        this->header.is_pk = true;
    }

    std::filesystem::path folder_path = fmt::format("./database/{}/{}_index", this->table_option->table_name,
                                                    this->index_name);

    if (!is_directory(folder_path)) {
        create_directories(folder_path);
    }

    this->btree_page_mgr = std::make_shared<BtreePageMgr>(
            fmt::format("./database/{}/{}_index/btree_file", this->table_option->table_name, this->index_name));
    this->btree_page_mgr->save_header(this->header);
    this->btree_page_mgr->save_node(0, *this->root, this->table_option);
}

BTree::BTree(const string &index_name, shared_ptr<DataPageMgr> data_page_mgr, TableOption *table_option) {
    this->table_option = table_option;
    this->index_name = index_name;
    this->root = new BTreeNode();
    this->btree_page_mgr = std::make_shared<BtreePageMgr>(
            fmt::format("./database/{}/{}_index/btree_file", this->table_option->table_name, this->index_name));
    this->btree_page_mgr->get_header(this->header);
    this->btree_page_mgr->get_node(this->header.root_id, *this->root, this->table_option);
}

void BTree::insert_key(struct BtreeKey &key, long pos) {
    BTreeNode *now_node = this->root;
    vector<pair<long, int>> traversal_node_record;

    while (true) {
        vector<struct BtreeKey>::iterator key_iter = now_node->keys.begin();
        vector<long>::iterator children_iter = now_node->children.begin();

        if (now_node->header.is_leaf) {
            for (; key_iter != now_node->keys.end(); ++key_iter, ++children_iter) {
                int cmp_result = this->compare_key(key, *key_iter);

                if (cmp_result > 0) {
                    continue;
                } else if (cmp_result < 0) {
                    break;
                }
            }

            if (key_iter == now_node->keys.end()) {
                now_node->keys.push_back(key);
                now_node->children.push_back(pos);
            } else {
                now_node->keys.insert(key_iter, key);
                now_node->children.insert(children_iter, pos);
            }

            now_node->header.key_count++;

            if (now_node->is_full()) {
                this->split_child(now_node, traversal_node_record);
            }

            break;
        }

        for (; key_iter != now_node->keys.end(); ++key_iter, ++children_iter) {
            int cmp_result = this->compare_key(key, *key_iter);

            if (cmp_result > 0) {
                continue;
            } else if (cmp_result < 0) {
                break;
            }
        }

        if (key_iter != now_node->keys.begin()) {
            --key_iter;
            --children_iter;

            strncpy(key_iter->data, key.data, this->header.key_field_len);
            key_iter->_id = key._id;
        }

        traversal_node_record.emplace_back(make_pair(now_node->header.traversal_id, key_iter - now_node->keys.begin()));

        if (this->NodeMap.find(*children_iter) != this->NodeMap.end()) {
            now_node = this->NodeMap[*children_iter];
        } else {
            now_node = new BTreeNode();

            this->btree_page_mgr->get_node(*children_iter, *now_node, this->table_option);

            this->NodeMap[*children_iter] = now_node;
            this->node_buffer.push(now_node);
        }
    }
}

int BTree::compare_key(const struct BtreeKey &key1, const struct BtreeKey &key2) {
    switch (this->header.fieldType) {
        case FieldType::CHAR: {
            int id1 = key1._id, id2 = key2._id, cmp = this->header.is_pk ? 0 : strncmp(key1.data, key2.data, 1);

            if (cmp != 0) {
                return cmp;
            } else {
                if (id1 > id2) {
                    return 1;
                } else if (id1 < id2) {
                    return -1;
                } else {
                    return 0;
                }
            }
        }
        case FieldType::INTEGER: {
            int k1, k2, id1 = key1._id, id2 = key2._id;
            if (this->header.is_pk) {
                k1 = k2 = 0;
            } else {
                strncpy(reinterpret_cast<char *>(&k1), key1.data, 4);
                strncpy(reinterpret_cast<char *>(&k2), key2.data, 4);
            }

            if (k1 > k2) {
                return 1;
            } else if (k1 < k2) {
                return -1;
            } else {
                if (id1 > id2) {
                    return 1;
                } else if (id1 < id2) {
                    return -1;
                } else {
                    return 0;
                }
            }
        }
    }
}

void BTree::split_child(BTreeNode *to_split, vector<pair<long, int>> &traversal_record) {
    while (true) {
        this->header.count++;

        auto *right = new BTreeNode(this->header.count - 1,
                                    this->header.degree,
                                    this->header.page_size,
                                    this->header.key_field_len,
                                    false,
                                    to_split->header.is_leaf);

        if (to_split->header.is_root) {
            this->header.count++;

            auto *new_root = new BTreeNode(this->header.count - 1,
                                           this->header.degree,
                                           this->header.page_size,
                                           this->header.key_field_len,
                                           true,
                                           false);

            to_split->header.is_root = false;

            this->header.root_id = new_root->header.traversal_id;

            new_root->keys.push_back(to_split->key_copy(to_split->header.key_count / 2 - 1));
            new_root->keys.push_back(to_split->key_copy(to_split->header.key_count - 1));
            new_root->children.push_back(to_split->header.traversal_id);
            new_root->children.push_back(right->header.traversal_id);
            new_root->header.key_count = 2;

            auto right_keys_iter = to_split->keys.begin() + to_split->header.key_count / 2;
            auto right_children_iter = to_split->children.begin() + to_split->header.key_count / 2;

            for (; right_keys_iter != to_split->keys.end(); right_keys_iter++) {
                right->keys.emplace_back(*right_keys_iter);
                right->children.emplace_back(*right_children_iter);
            }
            right->header.key_count = right->keys.size();

            to_split->keys.resize(to_split->header.key_count / 2);
            to_split->children.resize(to_split->header.key_count / 2);
            to_split->header.key_count = to_split->keys.size();

            this->btree_page_mgr->save_header(this->header);
            this->btree_page_mgr->save_node(to_split->header.traversal_id, *to_split, this->table_option);
            this->btree_page_mgr->save_node(new_root->header.traversal_id, *new_root, this->table_option);
            this->btree_page_mgr->save_node(right->header.traversal_id, *right, this->table_option);
        } else {
            auto right_keys_iter = to_split->keys.begin() + to_split->header.key_count / 2;
            auto right_children_iter = to_split->children.begin() + to_split->header.key_count / 2;

            for (; right_keys_iter != to_split->keys.end(); right_keys_iter++) {
                right->keys.emplace_back(*right_keys_iter);
                right->children.emplace_back(*right_children_iter);
            }

            auto pair = traversal_record.back();

            traversal_record.pop_back();

            auto parent_node = this->NodeMap[pair.first];

            parent_node->keys.insert(parent_node->keys.begin() + pair.second,
                                     to_split->key_copy(to_split->header.key_count / 2 - 1));
            parent_node->children.insert(parent_node->children.begin() + pair.second, to_split->header.traversal_id);
            parent_node->children[pair.second + 1] = right->header.traversal_id;

            to_split->keys.resize(to_split->header.key_count / 2);
            to_split->children.resize(to_split->header.key_count / 2);
            to_split->header.key_count /= 2;

            this->btree_page_mgr->save_header(this->header);
            this->btree_page_mgr->save_node(to_split->header.traversal_id, *to_split, this->table_option);
            this->btree_page_mgr->save_node(right->header.traversal_id, *right, this->table_option);

            if (parent_node->is_full()) {
                to_split = parent_node;
                continue;
            }
        }

        break;
    }
}

BTreeNode::BTreeNode(long id, int degree, int page_size, int key_field_len, bool is_root, bool is_leaf) {
    this->header.traversal_id = id;
    this->header.degree = degree;
    this->header.page_size = page_size;
    this->header.key_field_len = key_field_len;
    this->header.is_root = is_root;
    this->header.is_leaf = is_leaf;
}

BTreeNode::BTreeNode() {
}

bool BTreeNode::is_full() {
    return this->keys.size() >= this->header.degree * 2;
}

struct BtreeKey BTreeNode::key_copy(size_t index) {
    struct BtreeKey key{
            nullptr,
            0
    };
    auto original = this->keys[index];
    strncpy(key.data, original.data, this->header.key_field_len);
    key._id = original._id;
    return key;
}

BtreePageMgr::BtreePageMgr(const string &filename, bool trunc) : fstream(filename.data(),
                                                                         ios::in | ios::out | ios::binary) {
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

template<class header_data>
void BtreePageMgr::save_header(header_data &header) {
    this->clear();
    this->seekp(0, ios::beg);
    this->write(reinterpret_cast<char *>(&header), sizeof(header));
    this->header_prefix = sizeof(header_data);
}

template<class header_data>
bool BtreePageMgr::get_header(header_data &header) {
    this->clear();
    this->seekg(0, ios::beg);
    this->read(reinterpret_cast<char *>(&header), sizeof(header));
    return this->gcount() > 0;
}

template<class btree_node>
void BtreePageMgr::save_node(const long &n, btree_node &node, TableOption *option) {
    this->clear();
    this->seekp(this->header_prefix + n * node.header.page_size, ios::beg);
    this->write(reinterpret_cast<char *>(&(node.header)), sizeof(node.header));

    for (int i = 0; i < node.header.key_count; i++) {
        int children_data = node.children[i];
        int id = node.keys[i]._id;
        char* data = node.keys[i].data;
        this->write(reinterpret_cast<char *>(&children_data), sizeof(long));
        this->write(reinterpret_cast<char *>(&id), sizeof(int));
        this->write(reinterpret_cast<char *>(data), node.header.key_field_len * sizeof(char));
    }
}

template<class btree_node>
bool BtreePageMgr::get_node(const long &n, btree_node &node, TableOption *option) {
    this->clear();
    this->seekp(this->header_prefix + n * node.header.page_size, ios::beg);
    this->read(reinterpret_cast<char *>(&node.header), sizeof(node.header));

    for (int i = 0; i < node.header.key_count; i++) {
        long child;
        this->read(reinterpret_cast<char *>(&child), sizeof(long));
        node.children.push_back(child);
        struct BtreeKey key{
                (char *) malloc(node.header.key_field_len),
                0
        };
        this->read(reinterpret_cast<char *>(&key._id), sizeof(int));
        this->read(reinterpret_cast<char *>(&key.data), node.header.key_field_len * sizeof(char));
        node.keys.push_back(key);
    }

    return this->gcount() > 0;
}

template<class header_data>
void DataPageMgr::save_header(header_data &header) {
    this->clear();
    this->seekp(0, ios::beg);
    this->write(reinterpret_cast<char *>(&header), sizeof(header));
    this->header_prefix = sizeof(header_data);
}

template<class header_data>
bool DataPageMgr::get_header(header_data &header) {
    this->clear();
    this->seekg(0, ios::beg);
    this->read(reinterpret_cast<char *>(&header), sizeof(header));
    return this->gcount() > 0;
}

void DataPageMgr::save_node(const long &n, const json &node, vector<FieldTypeInfo > &field_info) {
    this->clear();
    int total_size = 0, acc_size = 0;

    for (auto &type_info: field_info) {
        total_size += std::get<2>(type_info);
    }

    this->seekp(this->header_prefix + n * total_size, ios::beg);

    char *data = new char[total_size]();

    for (auto &type_info: field_info) {
        std::string type_name = std::get<0>(type_info);
        std::string type_type = std::get<1>(type_info);
        int type_size = std::get<2>(type_info);

        if (type_type == "int") {
            int read_data = node[type_name].get<int>();
            strncpy(data + acc_size, reinterpret_cast<char *>(&read_data), type_size);
        } else if (type_type == "char") {
            strncpy(data + acc_size, node[type_name].get<std::string>().c_str(), type_size);
        }

        acc_size += type_size;
    }

    this->write(data, total_size);
    delete[] data;
}

bool DataPageMgr::get_node(const long &n, json &node, vector<FieldTypeInfo > &field_info) {
    this->clear();
    int total_size = 0;

    for (auto &type_info: field_info) {
        total_size += std::get<2>(type_info);
    }

    this->seekp(this->header_prefix + n * total_size, ios::beg);

    for (auto &type_info: field_info) {
        std::string type_name = std::get<0>(type_info);
        std::string type_type = std::get<1>(type_info);
        int type_size = std::get<2>(type_info);

        if (type_type == "int") {
            int data = 0;
            this->read(reinterpret_cast<char *>(&data), type_size);
            node[type_name] = data;
        } else if (type_type == "char") {
            char *data = new char[type_size]();
            this->read(reinterpret_cast<char *>(&data), type_size);
            node[type_name] = string(data);
            delete[] data;
        }
    }

    return this->gcount() > 0;
}

DataPageMgr::DataPageMgr(const string &filename, bool trunc) : fstream(filename.data(),
                                                                       ios::in | ios::out | ios::binary) {
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
