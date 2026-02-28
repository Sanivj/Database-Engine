#include "table.h"
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

    if(statement.type==StatementType::DROP_TABLE){
        db.drop_table(statement.table_name);
    }
    
    if(statement.type==StatementType::INSERT){
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
        Row row;
        row.id=stoi(statement.insert_values[0]);
        strncpy(row.username,statement.insert_values[1].c_str(),COLUMN_USERNAME_SIZE);
        strncpy(row.email,statement.insert_values[2].c_str(),COLUMN_EMAIL_SIZE);
        table->insert_row(row);
    }

    if(statement.type==StatementType::SELECT_ALL){
        if(!db.table_exists(statement.table_name)){
            cout<<"Error: Tabel does not exist.\n";
            return;
        }
        Table *table =db.get_table(statement.table_name);
        const Schema &schema=db.get_schema(statement.table_name);

        if(!statement.has_where_clause){
            table->select_all();
        }else{
            if(statement.where_column==schema.get_columns()[schema.get_primary_key_index()].name){
                uint32_t id=stoi(statement.where_value);
                table->select_by_id(id);
            }else{
                cout<<"WHERE only supports primary key for now.\n";
            }
        }
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