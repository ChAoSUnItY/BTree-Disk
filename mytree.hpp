#ifndef MYTREE
#define MYTREE

#include <map>
#include <regex>
#include <string>
#include <vector>
#include <queue>
#include <memory>
#include <fstream>

#include "common.hpp"

#include "nlohmann/json.hpp"

using namespace std;
using json = nlohmann::json;

// typedef struct btree {
//     int key_len;
//     int *keys;
//     int children_len;
//     struct btree **children;
//     bool is_root, is_leaf;
//     int degree;
//     struct btree *parent;
//     int parent_child;
//     int traversal_id;
// } BTree;

#define DEFAULT_FIELD "(example_id int, example_str char(8), example_str2 char(12))"

class Table;
class TableOption;
class DbSystem;
class BtreePageMgr;
class DataPageMgr;
enum class FieldType;
class BTree;
class BTreeNode;
struct BtreeKey;

class Table {
    vector<BTree*> btrees;
public:
    TableOption* option;

    Table(const string& name, const string& field_info);
    Table(const string& name);

    void create_primary_index(const string& pk);
    void create_index(const string& index_name);
    void insert_data(json& json);
    BTree* get_index(const string& index) {
        if (this->IndexMap.contains(index)) {
            return this->IndexMap[index];
        } else {
            return nullptr;
        }
    };

    private:
        string pk;
        map<string, BTree*> IndexMap;
        shared_ptr <DataPageMgr> data_page_mgr;

        struct MetaData {
            long count{0};
        } data_header;
};

class TableOption {
    public:
        TableOption(const string& table_name):
            table_name(table_name),
            page_size(DEFAULT_PAGE_SIZE),
            field_max(DEFAULT_FIELD_MAX),
            value_max(DEFAULT_VALUE_MAX),
            btree_node_buffer_len(BTREE_NODE_BUFFER_LEN){};
        TableOption(const string& table_name, int page_size, int field_max, int value_max, int btree_node_buffer_len):
            table_name(table_name),
            page_size(page_size),
            field_max(field_max),
            value_max(value_max),
            btree_node_buffer_len(btree_node_buffer_len){};
        ~TableOption() {};
        string table_name;
        int page_size;
        int field_max;
        int value_max;
        int btree_node_buffer_len;
        vector<FieldTypeInfo> field_info; // 欄位名稱, 資料型態, 欄位值 size
};

class Buffer {

};

enum class FieldType {
    INTEGER = 0,
    CHAR = 1
};

class BTree {
    public:
        BTree(const string& index_name, int degree, FieldType type, int key_field_len, shared_ptr <DataPageMgr> data_page_mgr, TableOption* table_option);
        BTree(const string& index_name, shared_ptr <DataPageMgr> data_page_mgr, TableOption* table_option);
        ~BTree();
        TableOption* table_option;
        struct meta_data {
            FieldType fieldType;
            bool is_pk{false};
            long count{0};
            long root_id{0};
            int degree;
            int key_field_len;
        } header;
        string index_name;
        shared_ptr <BtreePageMgr> btree_page_mgr;
        shared_ptr <DataPageMgr> data_page_mgr;
        queue<BTreeNode*> node_buffer;
        map<long, BTreeNode*> NodeMap;
        BTreeNode* root;

        void insert_key(struct BtreeKey& key, long pos);
        int compare_key(const struct BtreeKey& key1, const struct BtreeKey& key2);
        void split_child(BTreeNode* to_split, vector<pair<long, int>>& traversal_record);
};

class BTreeNode {
    public:
        BTreeNode(long id, int degree, int key_field_len, bool is_root, bool is_leaf);
        BTreeNode();
        ~BTreeNode();

        struct meta_data {
            long traversal_id;
            int degree;
            int key_field_len;
            bool is_root;
            bool is_leaf;
            long right{-1};
            int key_count{0};
        } header;

        //BtreeKey key[2*degree-1];
        //long children[2*degree];
        vector<struct BtreeKey> keys;
        vector<long> children;

        bool is_full();
        struct BtreeKey key_copy(size_t index);
};

struct BtreeKey {
    char *data;
    int _id;
};

class BtreePageMgr : protected fstream {
    public:
        BtreePageMgr(const string& filename, bool trunc = false);
        ~BtreePageMgr();

        template <class header_data>
        void save_header(header_data &header);

        template <class header_data>
        bool get_header(header_data &header);

        template <class btree_node>
        void save_node(const long &n, btree_node &node, TableOption *option);

        template <class btree_node>
        bool get_node(const long &n, btree_node &node, TableOption *option);

    private:
        bool empty;
        string filename;
        long header_prefix{0};
};

class DataPageMgr : protected fstream {
    public:
        DataPageMgr(const string& filename, bool trunc = false);
        ~DataPageMgr();

        template <class header_data>
        void save_header(header_data &header);

        template <class header_data>
        bool get_header(header_data &header);

        void save_node(const long &n, const json &node, vector<FieldTypeInfo> &field_info);

        bool get_node(const long &n, json &node, vector<FieldTypeInfo> &field_info);

    private:
        bool empty;
        string filename;
        long header_prefix{0};
};



// void insert(BTree**, BTree*, int);
// void split_child(BTree**, BTree*);
// void delete_node(BTree**, BTree*, int);
// void resolve(BTree**, BTree*);
// void borrow(BTree**, BTree*);
// void merge_child(BTree**, BTree*);
// void traversal(BTree*);
// void print_tree(BTree*);
// void tree_init(BTree**, int);
// void tree_free(BTree*, bool);
// void validate(BTree*);
// void check_valid(BTree*, int);

#endif

