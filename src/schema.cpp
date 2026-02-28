#include "schema.h"
using namespace std;
void Schema::set_table_name(const string &name){
    table_name=name;
}

const string &Schema::get_table_name() const{
    return table_name;
}

void Schema::add_column(const string &name,DataType type,bool is_primary_key){
    columns.push_back({name,type,is_primary_key});
}

const vector<Column> &Schema::get_columns() const{
    return columns;
}

int Schema::get_primary_key_index() const{
    for(size_t i=0;i<columns.size();i++){
        if(columns[i].is_primary_key){
            return i;
        }
    }
    return -1;
}