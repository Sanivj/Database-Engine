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
        statement.insert_values.clear();
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
            value.erase(0,value.find_first_not_of(" \t"));
            value.erase(value.find_last_not_of(" \t")+1);

            if(!value.empty()&&value.front()=='\''){
                value.erase(0,1);
            }
            if(!value.empty()&&value.back()=='\''){
                value.pop_back();
            }
            statement.insert_values.push_back(value);
        }
        statement.type=StatementType::INSERT;
        return true;
    }

    if(upper_first=="SELECT"){
        statement.type=StatementType::SELECT_ALL;
        statement.select_columns.clear();

        string rest;
        getline(ss,rest);

        rest.erase(0,rest.find_first_not_of(" \t"));

        size_t from_pos=rest.find("FROM");

        if(from_pos==string::npos){
            cout<<"Syntax error. Expected FROM.\n";
            return false;
        }

        string column_part=rest.substr(0,from_pos);
        string after_from=rest.substr(from_pos+4);

        column_part.erase(0,column_part.find_first_not_of(" \t"));
        column_part.erase(column_part.find_last_not_of(" \t")+1);

        if(column_part=="*"){
            statement.select_all_columns=true;
        }else{
            statement.select_all_columns=false;
            stringstream col_stream(column_part);
            string col;
            while(getline(col_stream,col,',')){
                col.erase(0,col.find_first_not_of(" \t"));
                col.erase(col.find_last_not_of(" \t")+1);
                statement.select_columns.push_back(col);
            }
        }
        stringstream from_stream(after_from);
        from_stream>>statement.table_name;
        string maybe_where;
        from_stream>>maybe_where;
        if(string_to_upper(maybe_where)=="WHERE"){
            statement.has_where_clause=true;
            from_stream>>statement.where_column;
            string equals_sign;
            from_stream>>equals_sign;

            string raw_value;
            from_stream>>raw_value;

            if(!raw_value.empty()&&raw_value.back()==';'){
                raw_value.pop_back();
            }

            if(!raw_value.empty()&&raw_value.front()=='\''&&raw_value.back()=='\''){
                raw_value=raw_value.substr(1,raw_value.size()-2);
            }
            statement.where_value=raw_value;
        }
        return true;
    }
    return false;
}