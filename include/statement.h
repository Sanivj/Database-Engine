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

enum class AggregateType{
    NONE,
    COUNT,
    SUM,
    AVG,
    MIN,
    MAX
};

struct Statement{
    StatementType type;

    string table_name;
    //INSERT
    vector<string>insert_values;
    //SELECT
    vector<string>select_columns;
    bool select_all_columns=false;
    bool has_distinct=false;
    bool has_where_clause=false;
    string where_column;
    string where_operator;
    string where_value;
    //CREATE
    Schema schema_to_create;
    //UPDATE
    string update_column;
    string update_value;
    //ORDER BY
    bool has_order_by=false;
    string order_by_column;
    bool order_desc=false;
    //LIMIT
    bool has_limit=false;
    int limit_count=0;
    //AND/OR
    bool has_second_condition=false;
    string where_column2;
    string where_operator2;
    string where_value2;
    string logical_operator;
    //OFFSET
    bool has_offset=false;
    int offset_count=0;
    //AGGREGATE
    AggregateType aggregate_type=AggregateType::NONE;
    string aggregate_column;
    bool has_group_by=false;
    string group_by_column;
    //JOINS
    bool has_join=false;
    bool is_left_join=false;
    bool is_right_join=false;
    bool is_full_outer_join=false;
    string join_table;
    string join_left_column;
    string join_right_column;
};

bool prepare_statement(const string &input,Statement &statement);
#endif