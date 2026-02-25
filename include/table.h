#ifndef TABLE_H
#define TABLE_H

#include "pager.h"
#include "row.h"
using namespace std;

class Table{
    public:
        Table(const string &filename);
        ~Table();

        void insert_row(const Row &row);
        void select_all();
        void select_by_id(uint32_t id);
        bool id_exists(uint32_t id);
    private:
        Pager pager;
        uint32_t num_rows;
};
#endif