#include "database.h"
#include "schema.h"
#include <iostream>
#include <filesystem>
using namespace std;

Database::Database(){
    if(!filesystem::exists("data")){
        filesystem::create_directory("data");
    }
    
    Schema tables_schema;
    tables_schema.set_table_name("sys_tables");
    tables_schema.add_column("table_name",DataType::TEXT,true);

    tables["sys_tables"]=make_unique<Table>("data/sys_tables.db",tables_schema);
    schemas["sys_tables"]=tables_schema;
    
    Schema columns_schema;
    columns_schema.set_table_name("sys_columns");
    columns_schema.add_column("table_name",DataType::TEXT,false);
    columns_schema.add_column("column_name",DataType::TEXT,false);
    columns_schema.add_column("type",DataType::TEXT,false);
    columns_schema.add_column("is_primary",DataType::INT,false);

    tables["sys_columns"]=make_unique<Table>("data/sys_columns.db",columns_schema);
    schemas["sys_columns"]=columns_schema;
    
    load_catalog();
}

bool Database::create_table(const Schema &schema){
    string name=schema.get_table_name();

    if(schemas.find(name)!=schemas.end()){
        cout<<"Error: Table already exists.\n";
        return false;
    }

    schemas[name]=schema;
    string filename="data/"+name+".db";
    tables[name]=make_unique<Table>(filename,schema);

    Table *sys_tables=get_table("sys_tables");
    vector<Value>table_row;
    table_row.push_back(Value::from_text(schema.get_table_name()));
    sys_tables->insert_row(table_row);

    Table *sys_columns=get_table("sys_columns");
    for(const auto &col:schema.get_columns()){
        vector<Value>col_row;
        col_row.push_back(Value::from_text(schema.get_table_name()));
        col_row.push_back(Value::from_text(col.name));

        if(col.type==DataType::INT){
            col_row.push_back(Value::from_text("INT"));
        }else{
            col_row.push_back(Value::from_text("TEXT"));
        }
        col_row.push_back(Value::from_int(col.is_primary_key?1:0));
        sys_columns->insert_row(col_row);
    }

    cout<<"Table '"<<name<<"' created successfully.\n";
    return true;
}

void Database::drop_table(const string &table_name){
    if(table_name=="sys_tables"||table_name=="sys_columns"){
        cout<<"Error: Cannot drop system tables.\n";
        return;
    }

    if(!table_exists(table_name)){
        cout<<"Error: Table does not exist.\n";
        return;
    }

    Table *sys_tables=get_table("sys_tables");
    Table *sys_columns=get_table("sys_columns");

    if(sys_tables){
        sys_tables->delete_where("table_name","=",table_name,false);
    }

    if(sys_columns){
        sys_columns->delete_where("table_name","=",table_name,false);
    }

    string filename="data/"+table_name+".db";
    filesystem::remove(filename);

    tables.erase(table_name);
    schemas.erase(table_name);

    cout<<"Table "<<table_name<<" dropped successfully.\n";
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

void Database::load_catalog(){
    Table *sys_tables=get_table("sys_tables");
    Table *sys_columns=get_table("sys_columns");

    if(!sys_tables||!sys_columns){
        return;
    }
    auto table_rows=sys_tables->get_all_rows();

    for(const auto &row:table_rows){
        string table_name=row[0].as_text();
        if(table_name=="sys_tables"||table_name=="sys_columns"){
            continue;
        }
        Schema schema;
        schema.set_table_name(table_name);

        auto column_rows=sys_columns->get_all_rows();
        for(const auto &col_row:column_rows){
            if(col_row[0].as_text()!=table_name)continue;
            string col_name=col_row[1].as_text();
            string type_str=col_row[2].as_text();
            bool is_pk=col_row[3].as_int();

            DataType type;
            if(type_str=="INT"){
                type=DataType::INT;
            }else{
                type=DataType::TEXT;
            }
            schema.add_column(col_name,type,is_pk);
        }
        schemas[table_name]=schema;
        string filename="data/"+table_name+".db";
        tables[table_name]=make_unique<Table>(filename,schema);
    }
}