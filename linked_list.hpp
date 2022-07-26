#include <stdlib.h>
#include "mytree.hpp"

typedef struct node {
    BTree *value;
    struct node *next;
} Node;

static Node* new_node(BTree *value) {
    Node *node = (Node*) calloc(1, sizeof(Node));
    node->value = value;
    return node;
}

static Node* append_node(Node *node, BTree *value) {
    Node *new_node = (Node*) calloc(1, sizeof(Node));
    new_node->value = value;
    node->next = new_node;
    return new_node;
}

static void free_node(Node *node) {
    while (node) {
        Node *prev = node;
        node = node->next;
        free(prev);
    }
}