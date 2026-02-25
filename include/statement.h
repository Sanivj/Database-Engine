#ifndef STATEMENT_H
#define STATEMENT_H

#include "row.h"
#include <string>
using namespace std;

enum class StatementType{
    INSERT,
    SELECT_ALL,
    SELECT_BY_ID
};

struct Statement{
    StatementType type;
    Row row_to_insert;
    uint32_t id_to_select;
};

bool prepare_statement(const string &input,Statement &statement);
#endif