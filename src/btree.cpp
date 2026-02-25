#include "btree.h"
#include "row.h"
#include <cstring>

uint32_t *leaf_node_num_cells(void *node){
    return (uint32_t*)((char*)node+COMMON_NODE_HEADER_SIZE);
}

void *leaf_node_cell(void *node,uint32_t cell_num){
    return (char*)node+LEAF_NODE_HEADER_SIZE+cell_num*LEAF_NODE_CELL_SIZE;
}

void initialize_leaf_node(void *node){
    *(uint8_t*)node=(uint8_t)NodeType::LEAF;
    *leaf_node_num_cells(node)=0;
}