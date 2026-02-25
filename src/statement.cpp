#include "statement.h"
#include <sstream>
#include <iostream>

using namespace std;

bool prepare_statement(const string &input,Statement &statement){
    stringstream ss(input);
    string command;
    ss>>command;

    if(command =="insert"){
        statement.type=StatementType::INSERT;

        ss>>statement.row_to_insert.id;
        ss>>statement.row_to_insert.username;
        ss>>statement.row_to_insert.email;

        return true;
    }

    if(command=="select"){
        uint32_t id;
        if(ss>>id){
            statement.type=StatementType::SELECT_BY_ID;
            statement.id_to_select=id;
        }else{
            statement.type=StatementType::SELECT_ALL;
        }
        return true;
    }

    return false;
}