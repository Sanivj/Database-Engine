#ifndef BTREE_H
#define BTREE_H

#include <cstdint>
#include "pager.h"
#include "row.h"

const uint32_t NODE_TYPE_SIZE=sizeof(uint8_t);
const uint32_t IS_ROOT_SIZE=sizeof(uint8_t);
const uint32_t PARENT_POINTER_SIZE=sizeof(uint32_t);

const uint32_t COMMON_NODE_HEADER_SIZE=NODE_TYPE_SIZE+IS_ROOT_SIZE+PARENT_POINTER_SIZE;

enum class NodeType{
    INTERNAL,
    LEAF
};

const uint32_t LEAF_NODE_NUM_CELLS_SIZE=sizeof(uint32_t);
const uint32_t LEAF_NODE_HEADER_SIZE=COMMON_NODE_HEADER_SIZE+LEAF_NODE_NUM_CELLS_SIZE;

const uint32_t LEAF_NODE_KEY_SIZE=sizeof(uint32_t);
const uint32_t LEAF_NODE_VALUE_SIZE=sizeof(Row);
const uint32_t LEAF_NODE_CELL_SIZE=LEAF_NODE_KEY_SIZE+LEAF_NODE_VALUE_SIZE;

const uint32_t LEAF_NODE_SPACE_FOR_CELLS=PAGE_SIZE-LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS=LEAF_NODE_SPACE_FOR_CELLS/LEAF_NODE_CELL_SIZE;

void initialize_leaf_node(void *node);
uint32_t *leaf_node_num_cells(void *node);
void *leaf_node_cell(void *node,uint32_t cell_num);

#endif