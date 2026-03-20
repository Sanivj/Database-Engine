#ifndef STATEMENT_H
#define STATEMENT_H

#include "row.h"
#include "schema.h"
#include <string>
#include <vector>
using namespace std;

enum class StatementType{
    INSERT,
    SELECT_ALL,
    CREATE_TABLE,
    DROP_TABLE,
    DELETE_ROWS,
    UPDATE_ROWS
};

struct Statement{
    StatementType type;

    string table_name;
    //INSERT
    vector<string>insert_values;
    //SELECT
    vector<string>select_columns;
    bool select_all_columns=false;
    bool has_where_clause=false;
    string where_column;
    string where_operator;
    string where_value;
    //CREATE
    Schema schema_to_create;
    //UPDATE
    string update_column;
    string update_value;
};

bool prepare_statement(const string &input,Statement &statement);
#endif