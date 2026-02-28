#include "database.h"
#include "schema.h"
#include <iostream>
using namespace std;

bool Database::create_table(const Schema &schema){
    string name=schema.get_table_name();

    if(schemas.find(name)!=schemas.end()){
        cout<<"Error: Table already exists.\n";
        return false;
    }

    schemas[name]=schema;
    string filename="data/"+name+".db";
    tables[name]=make_unique<Table>(filename);

    cout<<"Table '"<<name<<"' created successfully.\n";
    return true;
}

bool Database::drop_table(const string &table_name){
    if(!table_exists(table_name)){
        cout<<"Error: Table does not exist.\n";
        return false;
    }
    tables.erase(table_name);
    schemas.erase(table_name);
    cout<<"Table '"<<table_name<<"' dropped.\n";
    return true;
}

Table *Database::get_table(const string &table_name){
    if(!table_exists(table_name))return nullptr;

    return tables[table_name].get();
}

bool Database::table_exists(const string &table_name) const {
    return schemas.find(table_name)!=schemas.end();
}

const Schema& Database::get_schema(const string &table_name) const{
    return schemas.at(table_name);
}