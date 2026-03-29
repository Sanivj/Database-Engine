#ifndef DATABASE_H
#define DATABASE_H

#include <string>
#include <unordered_map>
#include <memory>
#include "schema.h"
#include "table.h"
#include "index.h"
using namespace std;
class Database{
    public:
        Database();

        bool create_table(const Schema &schema);
        void drop_table(const string &table_name);
        const Schema &get_schema(const string &table_name) const;
        Table *get_table(const string &table_name);
        bool table_exists(const string &table_name) const;
        void load_catalog();

        //INDEX MANAGEMENT
        bool create_index(const string &index_name,const string &table_name,const string &column_name);
        void drop_index(const string &index_name);
        bool index_exists(const string &index_name)const;
        HashIndex *get_index_for_column(const string &table_name,const string &column_name);
        void list_indexes()const;
    private:
        unordered_map<string,Schema>schemas;
        unordered_map<string,unique_ptr<Table>>tables;
        //index_name->HashIndex
        unordered_map<string,HashIndex>indexes;
        //table:column->index_name for last lookup
        unordered_map<string,string>column_index_map;
};

#endif