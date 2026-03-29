#ifndef TABLE_H
#define TABLE_H

#include <vector>
#include "pager.h"
#include "schema.h"
#include "value.h"
#include "statement.h"

using namespace std;

class Table{
    public:
        Table(const string &filename,const Schema &schema);

        void insert_row(const vector<Value>& values);
        void select_all();
        void select_where(const string &column,const string &op,const string &value);
        void select_columns(const vector<string>&columns,const Schema &schema);
        void select_columns_where(const vector<string>&columns,const Schema &schema,const string &where_column,const string &where_operator,const string &where_value);
        void delete_where(const string &column,const string &op,const string &value,bool print_result=true);
        void update_where(const string &target_column,const string &new_value,const string &where_column,const string &where_operator,const string &where_value);
        vector<vector<Value>>get_all_rows();
        vector<vector<Value>>filter_rows(const Statement &statement);
        void print_rows(const vector<vector<Value>>&rows,bool distinct=false)const;
        void print_rows_with_schema(const vector<vector<Value>>&rows,const Schema &schema,bool distinct=false)const;
        void print_selected_columns(const vector<vector<Value>>&rows,const vector<string>&columns,const Schema &schema,bool distinct=false,const vector<string>&aliases={})const;
        void sort_rows(vector<vector<Value>>&rows,const string &order_by_column,bool descending)const;
        void aggregate(const vector<vector<Value>>&rows,const Statement &statement);
        void group_by_aggregate(const vector<vector<Value>> &rows,const Statement &statement);
        vector<vector<Value>>inner_join(Table *other,const string &col1,const string &col2);
        vector<vector<Value>>left_join(Table *other,const string &col1,const string &col2,const Schema &other_schema);
        vector<vector<Value>>right_join(Table *other,const string &col1,const string &col2,const Schema &other_schema);
        vector<vector<Value>>full_outer_join(Table *other,const string &col1,const string &col2,const Schema &other_schema);
        vector<vector<Value>>filter_joined_rows(const vector<vector<Value>>&rows,const Schema &combined_schema,const Statement &statement);
        void sort_joined_rows(vector<vector<Value>>&rows,const Schema &combined_schema,const string &order_by_column,bool descending)const;
    private:
        uint32_t compute_row_size() const;
        void serialize_row(const vector<Value> &values,void *destination) const;
        void deserialize_row(void *source,vector<Value> &values) const;
        bool primary_key_exists(const Value &pk_value);
        Pager pager;
        Schema schema;
        uint32_t num_rows;
};
#endif