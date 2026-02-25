#include "table.h"
#include "statement.h"
#include <iostream>
#include <string>
using namespace std;

void print_prompt(){
    cout<<"db>";
}

void execute_statement(const Statement &statement, Table&table){
    if(statement.type==StatementType::INSERT){
        table.insert_row(statement.row_to_insert);
    }

    if(statement.type==StatementType::SELECT_ALL){
        table.select_all();
    }

    if(statement.type==StatementType::SELECT_BY_ID){
        table.select_by_id(statement.id_to_select);
    }
}

int main(){
    Table table("data/database.db");
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
        execute_statement(statement,table);
    }
    return 0;
}