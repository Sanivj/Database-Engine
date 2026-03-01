#ifndef TABLE_H
#define TABLE_H

#include <vector>
#include "pager.h"
#include "schema.h"
#include "value.h"

using namespace std;

class Table{
    public:
        Table(const string &filename,const Schema &schema);

        void insert_row(const vector<Value>& values);
        void select_all();
        void select_where(const string &column,const string &value) const;
    private:
        uint32_t compute_row_size() const;
        void serialize_row(const vector<Value> &values,void *destination) const;
        void deserialize_row(void *source,vector<Value> &values) const;

        Pager pager;
        Schema schema;
        uint32_t num_rows;
};
#endif