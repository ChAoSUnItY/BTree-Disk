#include <stdio.h>
#include "mytree.hpp"
#include "linked_list.hpp"

void _check_valid(BTree*, int, int*);

void check_valid(BTree* t, int is_root) {
    int tree_height = -1;

    int traversal_id = 0;
    Node *head, *tail, *now;
    head = (Node *) calloc(1, sizeof(Node));
    tail = head;
    now = head;

    head->value = t;
    head->value->traversal_id = traversal_id++;

    while (now) {
        BTree* now_node = now->value;

        for (int i = 0 ; i < now_node->children_len ; i++) {
            Node *_new = (Node *) calloc(1, sizeof(Node));
            _new->value = now_node->children[i];
            _new->value->traversal_id = traversal_id++;
            tail->next = _new;
            tail = _new;
        }
        if (now == head) {
            _check_valid(now_node, is_root, &tree_height);
        } else {
            _check_valid(now_node, 0, &tree_height);
        }
        /*print_tree(now_node);*/

        now = now->next;
    }
    /*printf("==============================\n");*/

    now = head;
    while (now) {
        Node* to_delete = now;
        now = now->next;
        free(to_delete);
    }
}

void _check_valid(BTree* t, int is_root, int* tree_height) {
    if (!t) {
        return;
    }
    if (is_root) {
        if (!t->is_root) {
            printf("root is_root != 1\n");
            print_tree(t);
            exit(-1);
        }
        if (t->key_len < 1 && t->children_len > 0) {
            printf("root key_len < 1\n");
            print_tree(t);
            exit(-1);
        }

        if (!t->is_leaf) {
            if (t->children_len < 2) {
                printf("root children_len < 2\n");
                print_tree(t);
                exit(-1);
            }
        } else {
            if (t->children_len != 0) {
                printf("root children_len(%d) != 0\n", t->children_len);
                print_tree(t);
                exit(-1);
            }
        }
    } else {
        if (t->is_root) {
            printf("is_root == 1\n");
            print_tree(t);
            exit(-1);
        }
        if (t->key_len < t->degree-1) {
            printf("key_len(%d) < degree-1(%d)\n", t->key_len, t->degree-1);
            print_tree(t);
            exit(-1);
        }

        if (!t->is_leaf) {
            if (t->children_len < t->degree) {
                printf("children_len(%d) < degree(%d)\n", t->children_len, t->degree);
                print_tree(t);
                exit(-1);
            }
        } else {
            if (t->children_len != 0) {
                printf("root children_len(%d) != 0\n", t->children_len);
                print_tree(t);
                exit(-1);
            }
        }
    }

    if (!t->is_leaf) {
        if (t->key_len + 1 != t->children_len) {
            printf("t->key_len + 1(%d) != t->children_len(%d)\n", t->key_len + 1, t->children_len);
            print_tree(t);
            exit(-1);
        }
    }

    if (t->is_leaf) {
        if (t->children_len > 0) {
            printf("t->children_len(%d) > 0\n", t->children_len);
            print_tree(t);
            exit(-1);
        }
    } else {
        for (int i = 0 ; i < t->children_len ; i++) {
            if (!t->children[i]) {
                printf("children[%d] is NULL\n", i);
                print_tree(t);
                exit(-1);
            }
            if (t->children[i]->parent != t) {
                if (!t->children[i]->parent) {
                    printf("children[%d](%d)->parent is NULL\n",
                            i,
                            t->children[i]->traversal_id
                        );
                } else {
                    printf("children[%d](%d)->parent(%d) != t(%d)\n",
                            i,
                            t->children[i]->traversal_id,
                            t->children[i]->parent->traversal_id,
                            t->traversal_id
                        );
                }
                print_tree(t);
                exit(-1);
            }
            if (t->children[i]->parent_child != i) {
                printf("children[%d](%d)->parent_child(%d) != i(%d)\n",
                        i,
                        t->children[i]->traversal_id,
                        t->children[i]->parent_child,
                        i
                    );
                print_tree(t);
                exit(-1);
            }
        }
    }

    for (int i = 1 ; i < t->key_len ; i++) {
        if (t->keys[i] < t->keys[i-1]) {
            printf("keys[%d](%d) < keys[%d](%d)\n",
                    i,
                    t->keys[i],
                    i-1,
                    t->keys[i-1]
              );
            print_tree(t);
            exit(-1);
        }
    }

    if (t->is_root) {
        if (t->parent) {
            printf("root parent is not NULL (%d)\n",
                    t->parent->traversal_id
                );
            print_tree(t);
            exit(-1);
        }

        if (t->parent_child != -1) {
            printf("root parent_child != -1 (%d)\n",
                    t->parent_child
                );
            print_tree(t);
            exit(-1);
        }
    } else {
        if (!t->parent) {
            printf("parent is NULL\n");
            print_tree(t);
            exit(-1);
        }
        if (t->parent_child == -1) {
            printf("parent_child != -1\n");
            print_tree(t);
            exit(-1);
        }
    }

    if (t->parent && t->parent_child > 0 && t->keys[0] < t->parent->keys[t->parent_child-1]) {
        printf("keys[0](%d) < parent->keys[parent_child-1(%d)](%d)\n",
                t->keys[0],
                t->parent_child-1,
                t->parent->keys[t->parent_child-1]
            );
        print_tree(t);
        exit(-1);
    }

    if (t->parent && t->parent_child < t->parent->key_len && t->keys[t->key_len-1] > t->parent->keys[t->parent_child]) {
        printf("keys[key_len-1(%d)](%d) > parent->keys[parent_child(%d)](%d)\n",
                t->key_len-1,
                t->keys[t->key_len-1],
                t->parent_child,
                t->parent->keys[t->parent_child]
            );
        print_tree(t);
        exit(-1);
    }

    // if (t->is_leaf) {
    //     if (*tree_height == -1) {
    //         *tree_height = t->traversal_height;
    //         /*printf("ensure tree_height(%d) in node %d\n",*/
    //                 /**tree_height,*/
    //                 /*t->traversal_id*/
    //             /*);*/
    //     } else if (*tree_height != t->traversal_height) {
    //         printf("tree_height(%d) != traversal_height(%d)\n",
    //                 *tree_height,
    //                 t->traversal_height
    //             );
    //         print_tree(t);
    //         exit(-1);
    //     }
    // }
}