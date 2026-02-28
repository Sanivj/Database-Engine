#include "statement.h"
#include <sstream>
#include <iostream>
#include <algorithm>
using namespace std;

static string string_to_upper(string s){
    transform(s.begin(),s.end(),s.begin(),::toupper);
    return s;
}

bool prepare_statement(const string &input,Statement &statement){
    string trimmed=input;
    if(!trimmed.empty()&&trimmed.back()==';')trimmed.pop_back();

    stringstream ss(trimmed);
    string first_word;
    ss>>first_word;

    string upper_first=string_to_upper(first_word);

    if(upper_first=="CREATE"){
        string second_word;
        ss>>second_word;
        if(string_to_upper(second_word)!="TABLE"){
            cout<<"Syntax Error. Expected TABLE.\n";
            return false;
        }
        string table_name;
        ss>>table_name;
        statement.type=StatementType::CREATE_TABLE;
        statement.schema_to_create=Schema();
        statement.schema_to_create.set_table_name(table_name);

        string rest;
        getline(ss,rest);

        size_t open_paren=rest.find('(');
        size_t close_paren=rest.find(')');

        if(open_paren==string::npos||close_paren==string::npos){
            cout<<"Syntax error in column definition.\n";
            return false;
        }

        string columns_str=rest.substr(open_paren+1,close_paren-open_paren-1);
        stringstream column_stream(columns_str);
        string column_def;

        while(getline(column_stream,column_def,',')){
            
            stringstream col_stream(column_def);

            string col_name;
            string col_type;
            string maybe_primary;
            string maybe_key;

            col_stream>>col_name;
            col_stream>>col_type;
            bool is_primary=false;
            col_stream>>maybe_primary>>maybe_key;
            if(string_to_upper(maybe_primary)=="PRIMARY"&&string_to_upper(maybe_key)=="KEY"){
                is_primary=true;
            }

            DataType type;
            if(string_to_upper(col_type)=="INT"){
                type=DataType::INT;
            }else if(string_to_upper(col_type)=="TEXT"){
                type=DataType::TEXT;
            }else{
                cout<<"Unsupported data type: "<<col_type<<"\n";
                return false;
            }
            statement.schema_to_create.add_column(
                col_name,
                type,
                is_primary
            );
        }
        return true;
    }

    if(upper_first=="INSERT"){
        string into_word;
        ss>>into_word;

        if(string_to_upper(into_word)!="INTO"){
            cout<<"Syntax error. Expected INTO.\n";
            return false;
        }
        ss>>statement.table_name;

        string values_word;
        ss>>values_word;

        if(string_to_upper(values_word)!="VALUES"){
            cout<<"Syntax error. Expected VALUES.\n";
            return false;
        }

        string rest;
        getline(ss,rest);

        size_t open_paren=rest.find('(');
        size_t close_paren=rest.find(')');

        if(open_paren==string::npos||close_paren==string::npos){
            cout<<"Syntax error in VALUES.\n";
            return false;
        }

        string values_str=rest.substr(open_paren+1,close_paren-open_paren-1);
        stringstream values_stream(values_str);
        string value;
        while(getline(values_stream,value,',')){
            value.erase(0,value.find_first_not_of("\t"));
            value.erase(value.find_last_not_of("\t")+1);

            if(!value.empty()&&value.front()=='\''&&value.back()=='\''){
                value=value.substr(1,value.size()-2);
            }
            statement.insert_values.push_back(value);
        }
        statement.type=StatementType::INSERT;
        return true;
    }

    if(upper_first=="SELECT"){
        statement.type=StatementType::SELECT_ALL;
        string columns_part;
        ss>>columns_part;

        if(columns_part=="*"){
            statement.select_all_columns=true;
        }else{
            cout<<"Only '*' supported for now.\n";
            return false;
        }

        string from_word;
        ss>>from_word;
        if(string_to_upper(from_word)!="FROM"){
            cout<<"Syntax error. Expected FROM.\n";
            return false;
        }

        ss>>statement.table_name;

        string maybe_where;
        ss>>maybe_where;

        if(string_to_upper(maybe_where)=="WHERE"){
            statement.has_where_clause=true;

            ss>>statement.where_column;

            string equals_sign;
            ss>>equals_sign;

            ss>>statement.where_value;

            if(!statement.where_value.empty()&&statement.where_value.front()=='\''&&statement.where_value.back()=='\''){
                statement.where_value=statement.where_value.substr(1,statement.where_value.size()-2);
            }
        }
        return true;
    }
    return false;
}