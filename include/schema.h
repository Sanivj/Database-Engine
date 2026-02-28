#ifndef SCHEMA_H
#define SCHEMA_H

#include <string>
#include <vector>
using namespace std;
enum class DataType{
    INT,
    TEXT
};

struct Column{
    string name;
    DataType type;
    bool is_primary_key;
};

class Schema{
    public:
        Schema()=default;

        void set_table_name(const string &name);
        const string &get_table_name() const;

        void add_column(const string &name,DataType type,bool is_primary_key);
        const vector<Column>&get_columns() const;
        int get_primary_key_index() const;
    private:
        string table_name;
        vector<Column>columns;
};

#endif