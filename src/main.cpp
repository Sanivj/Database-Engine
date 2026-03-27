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
            cout<<"Error: Table does not exist.\n";
            return;
        }
        Table *table =db.get_table(statement.table_name);
        const Schema &schema=db.get_schema(statement.table_name);

        vector<vector<Value>>rows;

        if(statement.has_join){
            if(!db.table_exists(statement.join_table)){
                cout<<"Error: Join table does not exist.\n";
                return;
            }

            Table *table2=db.get_table(statement.join_table);

            vector<vector<Value>>rows=table->inner_join(table2,statement.join_left_column,statement.join_right_column);

            table->print_rows(rows);
            return;
        }

        if(statement.has_where_clause){
            rows=table->filter_rows(
                statement
            );
        }else{
            rows=table->get_all_rows();
        }

        if(statement.has_order_by){
            table->sort_rows(rows,statement.order_by_column,statement.order_desc);
        }

        if(statement.has_offset){
            if(statement.offset_count<rows.size()){
                rows.erase(rows.begin(),rows.begin()+statement.offset_count);
            }else{
                rows.clear();
            }
        }

        if(statement.has_limit){
            if(statement.limit_count<rows.size()){
                rows.resize(statement.limit_count);
            }
        }

        if(statement.aggregate_type!=AggregateType::NONE){
            if(statement.has_group_by){
                table->group_by_aggregate(rows,statement);
            }else{
                table->aggregate(rows,statement);
            }
            return;
        }

        if(statement.select_all_columns){
            table->print_rows(rows);
        }else{
            table->print_selected_columns(rows,statement.select_columns,schema);
        }
    }

    else if(statement.type==StatementType::DELETE_ROWS){
        if(!db.table_exists(statement.table_name)){
            cout<<"Error: Table does not exist.\n";
            return;
        }

        Table *table=db.get_table(statement.table_name);

        table->delete_where(statement.where_column,statement.where_operator,statement.where_value);
    }

    else if(statement.type==StatementType::UPDATE_ROWS){
        if(!db.table_exists(statement.table_name)){
            cout<<"Error: Table does not exist.\n";
            return;
        }
        Table *table=db.get_table(statement.table_name);

        table->update_where(statement.update_column,statement.update_value,statement.where_column,statement.where_operator,statement.where_value);
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
        Statement statement={};
        if(!prepare_statement(input,statement)){
            cout<<"Unrecognized command\n";
            continue;
        }
        execute_statement(statement,db);
    }
    return 0;
}