#include "value.h"
#include "statement.h"
#include "database.h"
#include <iostream>
#include <string>
#include <cstring>
using namespace std;

void print_prompt(){
    cout<<"db>";
}

void execute_statement(const Statement &statement, Database &db){

    if(statement.type==StatementType::CREATE_TABLE){
        db.create_table(statement.schema_to_create);
    }

    else if(statement.type==StatementType::DROP_TABLE){
        db.drop_table(statement.table_name);
    }
    
    else if(statement.type==StatementType::INSERT){
        if(!db.table_exists(statement.table_name)){
            cout<<"Error: Table does not exist.\n";
            return;
        }
        Table *table=db.get_table(statement.table_name);
        const Schema &schema=db.get_schema(statement.table_name);
        if(statement.insert_values.size()!=schema.get_columns().size()){
            cout<<"Error: Column count mismatch.\n";
            return;
        }
        vector<Value> values;
        for(size_t i=0;i<schema.get_columns().size();i++){
            if(schema.get_columns()[i].type==DataType::INT){
                values.push_back(Value::from_int(stoi(statement.insert_values[i])));
            }else{
                values.push_back(Value::from_text(statement.insert_values[i]));
            }
        }
        table->insert_row(values);
        cout<<"Executed\n";
    }

    else if(statement.type==StatementType::SELECT_ALL){
        if(!db.table_exists(statement.table_name)){
            cout<<"Error: Tabel does not exist.\n";
            return;
        }
        Table *table =db.get_table(statement.table_name);
        const Schema &schema=db.get_schema(statement.table_name);

        if(!statement.has_where_clause){
            if(statement.select_all_columns){
                table->select_all();
            }else{
                table->select_columns(statement.select_columns,schema);
            }
        }else{
            if(statement.select_all_columns){
                table->select_where(statement.where_column,statement.where_value);
            }else{
                table->select_columns_where(
                    statement.select_columns,
                    schema,
                    statement.where_column,
                    statement.where_value
                );
            }
        }
    }

    else{
        cout<<"Unsupported Statement.\n";
    }
}

int main(){
    Database db;
    string input;
    while(true){
        print_prompt();
        getline(cin,input);

        if(input==".exit"){
            cout<<"Exiting MiniDB...\n";
            break;
        }
        Statement statement;
        if(!prepare_statement(input,statement)){
            cout<<"Unrecognized command\n";
            continue;
        }
        execute_statement(statement,db);
    }
    return 0;
}