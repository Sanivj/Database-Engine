#include "value.h"
#include "statement.h"
#include "database.h"
#include <iostream>
#include <string>
#include <cstring>
#include <vector>
#include <algorithm>
#include "index.h"
using namespace std;

static vector<string>history;
static int history_pos=-1;
static string trim_copy(const string &s){
    size_t start=s.find_first_not_of(" \t");
    if(start==string::npos)return "";
    size_t end=s.find_last_not_of(" \t");
    return s.substr(start,end-start+1);
}

void add_to_history(const string &cmd){
    if(cmd.empty())return;
    if(!history.empty()&&history.back()==cmd)return;
    history.push_back(cmd);
    history_pos=history.size();
}

void print_prompt(bool continuation=false,bool in_txn=false){
    if(continuation){
        if(in_txn)cout<<"txn...> ";
        else cout<<"...> "; 
    }else{
        if(in_txn)cout<<"txn> ";
        else cout<<"db> ";
    }     
}

bool read_statement(string &out,bool in_txn=false){
    out.clear();
    string line;
    bool first=true;

    while(true){
        print_prompt(!first,in_txn);
        if(!getline(cin,line)){
            return false;
        }

        size_t end=line.find_last_not_of(" \t\r\n");
        if(end!=string::npos)line=line.substr(0,end+1);

        if(line.empty()){
            if(out.empty())continue;
            continue;
        }

        if(out.empty()){
            out=line;
        }else{
            out+=" "+line;
        }

        if(!out.empty()&&out.back()==';')break;
        if(!out.empty()&&out[0]=='.')break;

        first=false;
    }
    return true;
}

bool handle_meta_command(const string &input,Database &db){
    if(input==".exit"||input==".quit"){
        cout<<"Existing MiniDB...\n";
        return false;
    }

    if(input==".help"){
        cout<<"\n";
        cout<<"  Supported SQL commands:\n";
        cout<<"  ─────────────────────────────────────────────────────\n";
        cout<<"  CREATE TABLE name (col type [PRIMARY KEY], ...);\n";
        cout<<"  DROP TABLE name;\n";
        cout<<"  INSERT INTO name VALUES (v1, v2, ...);\n";
        cout<<"  SELECT [DISTINCT] */cols FROM table\n";
        cout<<"         [JOIN table2 ON t1.col = t2.col]\n";
        cout<<"         [LEFT|RIGHT|FULL OUTER JOIN ...]\n";
        cout<<"         [WHERE col op val [AND|OR col op val]]\n";
        cout<<"         [GROUP BY col]\n";
        cout<<"         [ORDER BY col [ASC|DESC]]\n";
        cout<<"         [LIMIT n] [OFFSET n];\n";
        cout<<"  UPDATE table SET col = val WHERE col op val;\n";
        cout<<"  DELETE FROM table WHERE col op val;\n";
        cout<<"  SELECT COUNT(*)|SUM(col)|AVG(col)|MIN(col)|MAX(col)\n";
        cout<<"         FROM table [WHERE ...];\n";
        cout<<"\n";
        cout<<"  Meta commands:\n";
        cout<<"  ─────────────────────────────────────────────────────\n";
        cout<<"  .help              Show this help message\n";
        cout<<"  .tables            List all user tables\n";
        cout<<"  .schema <table>    Show columns of a table\n";
        cout<<"  .exit / .quit      Exit MiniDB\n";
        cout<<"\n";
        return true;
    }

    if(input==".tables"){
        Table *sys=db.get_table("sys_tables");
        if(!sys){
            cout<<"No tables found.\n";
            return true;
        }
        auto rows=sys->get_all_rows();
        if(rows.empty()){
            cout<<"No users tables.\n";
            return true;
        }
        cout<<"\n Tables:\n";
        cout<<"  ──────────────────\n";
        for(const auto &row:rows){
            string name=row[0].as_text();
            if(name!="sys_tables"&&name!="sys_columns"){
                cout<<" "<<name<<"\n";
            }
        }
        cout<<"\n";
        return true;
    }

    if(input.substr(0,7)==".schema"){
        string table_name=trim_copy(input.substr(7));
        if(table_name.empty()){
            cout<<"Usage: .schema <table_name>\n";
            return true;
        }
        if(!db.table_exists(table_name)){
            cout<<"Error: Table '"<<table_name<<"' does not exist.\n";
            return true;
        }
        const Schema &schema=db.get_schema(table_name);
        const auto &cols=schema.get_columns();
        cout<<"\n  Schema for '"<<table_name<<"':\n";
        cout<<"  ──────────────────────────────────\n";
        for(const auto &col:cols){
            string type=(col.type==DataType::INT)?"INT":"TEXT";
            string pk=col.is_primary_key?" (PRIMARY KEY)":"";
            cout<<" "<<col.name<<" "<<type<<pk<<"\n";
        }
        cout<<"\n";
        return true;
    }

    if(input==".indexes"){
        db.list_indexes();
        return true;
    }

    if(!input.empty()&&input[0]=='.'){
        cout<<"Unknown command: "<<input<<". Type .help for help.\n";
        return true;
    }
    return false;
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
            if(statement.insert_values[i]=="__NULL__"){
                values.push_back(Value::make_null());
            }else if(schema.get_columns()[i].type==DataType::INT){
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
            const Schema &schema2=db.get_schema(statement.join_table);

            Schema combined_schema;
            combined_schema.set_table_name(statement.table_name+"_"+statement.join_table);
            for(const auto &col:schema.get_columns()){
                combined_schema.add_column(col.name,col.type,col.is_primary_key);
            }
            for(const auto &col:schema2.get_columns()){
                combined_schema.add_column(col.name,col.type,col.is_primary_key);
            }
            
            if(statement.is_left_join){
                rows=table->left_join(table2,statement.join_left_column,statement.join_right_column,schema2);
            }else if(statement.is_right_join){
                rows=table->right_join(table2,statement.join_left_column,statement.join_right_column,schema2);
            }else if(statement.is_full_outer_join){
                rows=table->full_outer_join(table2,statement.join_left_column,statement.join_right_column,schema2);
            }else{
                rows=table->inner_join(table2,statement.join_left_column,statement.join_right_column);
            }

            if(statement.has_where_clause){
                rows=table->filter_joined_rows(rows,combined_schema,statement);               
            }

            if(statement.has_order_by){
                table->sort_joined_rows(rows,combined_schema,statement.order_by_column,statement.order_desc);
            }

            if(statement.has_offset){
                if((size_t)statement.offset_count<rows.size()){
                    rows.erase(rows.begin(),rows.begin()+statement.offset_count);
                }else{
                    rows.clear();
                }
            }

            if(statement.has_limit){
                if((size_t)statement.limit_count<rows.size()){
                    rows.resize(statement.limit_count);
                }
            }

            if(statement.select_all_columns){
                table->print_rows_with_schema(rows,combined_schema,statement.has_distinct);
            }else{
                table->print_selected_columns(rows,statement.select_columns,combined_schema,statement.has_distinct,statement.select_aliases);
            }
            return;
        }

        
            HashIndex *idx=nullptr;
            if(statement.has_where_clause){
                idx=db.get_index_for_column(statement.table_name,statement.where_column);
            }
            rows=table->scan_rows(statement,idx);

            if(statement.has_order_by){
                table->sort_rows(rows,statement.order_by_column,statement.order_desc);
                if(statement.has_offset){
                    if(statement.offset_count<(int)rows.size()){
                        rows.erase(rows.begin(),rows.begin()+statement.offset_count);
                    }else{
                        rows.clear();
                    }
                }
                if(statement.has_limit){
                    if(statement.limit_count<(int)rows.size()){
                        rows.resize(statement.limit_count);
                    }
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
            table->print_rows_with_schema(rows,schema,statement.has_distinct);
        }else{
            table->print_selected_columns(rows,statement.select_columns,schema,statement.has_distinct,statement.select_aliases);
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

    else if(statement.type==StatementType::CREATE_INDEX){
        db.create_index(statement.index_name,statement.table_name,statement.index_column);
    }

    else if(statement.type==StatementType::DROP_INDEX){
        db.drop_index(statement.index_name);
    }

    else if(statement.type==StatementType::BEGIN_TXN){
        db.begin_transaction();
    }

    else if(statement.type==StatementType::COMMIT_TXN){
        db.commit_transaction();
    }

    else if(statement.type==StatementType::ROLLBACK_TXN){
        db.rollback_transaction();
    }

    else{
        cout<<"Unsupported Statement.\n";
    }
}

int main(){
    Database db;
     cout<<"\n";
    cout<<"  ╔══════════════════════════════════╗\n";
    cout<<"  ║         MiniDB v1.0              ║\n";
    cout<<"  ║  Type .help for available cmds  ║\n";
    cout<<"  ║  Type .exit to quit             ║\n";
    cout<<"  ╚══════════════════════════════════╝\n";
    cout<<"\n";
    string input;
    while(true){
        if(!read_statement(input,db.in_transaction()))break;
        string trimmed=input;
        if(!trimmed.empty()&&trimmed.back()==';'){
            trimmed.pop_back();
        }
        trimmed=trim_copy(trimmed);
        
        if(trimmed.empty())continue;

        if(trimmed[0]=='.'){
            if(!handle_meta_command(trimmed,db))break;
            continue;
        }

        add_to_history(input);

        Statement statement={};

        if(!prepare_statement(input,statement)){
            cout<<"Error: Unrecognized command. Type .help for help.\n";
        }
        execute_statement(statement,db);
    }
    return 0;
}