#ifndef DATABASE_H
#define DATABASE_H

#include <string>
#include <unordered_map>
#include <memory>
#include "schema.h"
#include "table.h"
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
    private:
        unordered_map<string,Schema>schemas;
        unordered_map<string,unique_ptr<Table>>tables;
};

#endif