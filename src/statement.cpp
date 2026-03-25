#include "statement.h"
#include <sstream>
#include <iostream>
#include <algorithm>
using namespace std;

static string string_to_upper(string s){
    transform(s.begin(),s.end(),s.begin(),::toupper);
    return s;
}

static string trim_copy(const string& s){
    size_t start = s.find_first_not_of(" \t");
    if(start == string::npos){
        return "";
    }

    size_t end = s.find_last_not_of(" \t");
    return s.substr(start, end - start + 1);
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
        statement.has_where_clause=false;
        statement.has_order_by=false;
        statement.order_desc=false;
        statement.has_limit=false;
        statement.limit_count=0;
        statement.has_second_condition=false;
        statement.logical_operator="";

        string rest;
        getline(ss,rest);
        rest=trim_copy(rest);

        size_t from_pos=rest.find("FROM");
        if(from_pos==string::npos){
            cout<<"Syntax error. Expected FROM.\n";
            return false;
        }

        string column_part=trim_copy(rest.substr(0,from_pos));
        string after_from=trim_copy(rest.substr(from_pos+4));

        if(column_part=="*"){
            statement.select_all_columns=true;
        }else{
            statement.select_all_columns=false;
            stringstream col_stream(column_part);
            string col;

            while(getline(col_stream,col,',')){
                col=trim_copy(col);
                if(!col.empty()&&col.back()==';'){
                    col.pop_back();
                }
                col=trim_copy(col);
                statement.select_columns.push_back(col);
            }
        }
        size_t where_pos=after_from.find("WHERE");
        size_t order_pos=after_from.find("ORDER BY");
        size_t limit_pos=after_from.find("LIMIT");

        if(where_pos==string::npos&&order_pos==string::npos&&limit_pos==string::npos){
            statement.table_name=trim_copy(after_from);
        }

        else if(where_pos==string::npos&&order_pos==string::npos&&limit_pos!=string::npos){
            statement.table_name=trim_copy(after_from.substr(0,limit_pos));
        }

        else if(where_pos!=string::npos&&order_pos==string::npos){
            statement.table_name=trim_copy(after_from.substr(0,where_pos));
            string where_part;
            if(limit_pos!=string::npos){
                where_part=trim_copy(after_from.substr(where_pos+5,limit_pos-(where_pos+5)));
            }else{
                where_part=trim_copy(after_from.substr(where_pos+5));
            }
            statement.has_where_clause=true;
            stringstream where_stream(where_part);
            where_stream>>statement.where_column;
            where_stream>>statement.where_operator;
            where_stream>>statement.where_value;

            statement.where_value=trim_copy(statement.where_value);

            if(!statement.where_value.empty()&&statement.where_value.front()=='\''&&statement.where_value.back()=='\''){
                statement.where_value=statement.where_value.substr(1,statement.where_value.size()-2);
            }

            string maybe_logical;
            where_stream>>maybe_logical;
            maybe_logical=string_to_upper(trim_copy(maybe_logical));

            if(maybe_logical=="AND"||maybe_logical=="OR"){
                statement.has_second_condition=true;
                statement.logical_operator=maybe_logical;

                where_stream>>statement.where_column2;
                where_stream>>statement.where_operator2;
                where_stream>>statement.where_value2;

                statement.where_value2=trim_copy(statement.where_value2);

                if(!statement.where_value2.empty()&&statement.where_value2.front()=='\''&&statement.where_value2.back()=='\''){
                    statement.where_value2=statement.where_value2.substr(1,statement.where_value2.size()-2);
                }
            }
        }

        else if(where_pos==string::npos&&order_pos!=string::npos){
            statement.table_name=trim_copy(after_from.substr(0,order_pos));
        }

        else{
            statement.table_name=trim_copy(after_from.substr(0,where_pos));
            size_t where_end;
            if(order_pos!=string::npos){
                where_end=order_pos;
            }else if(limit_pos!=string::npos){
                where_end=limit_pos;
            }else{
                where_end=after_from.length();
            }
            string where_part=trim_copy(after_from.substr(where_pos+5,where_end-(where_pos+5)));
            statement.has_where_clause=true;
            stringstream where_stream(where_part);
            where_stream>>statement.where_column;
            where_stream>>statement.where_operator;
            where_stream>>statement.where_value;

            statement.where_value=trim_copy(statement.where_value);

            if(!statement.where_value.empty()&&statement.where_value.front()=='\''&&statement.where_value.back()=='\''){
                statement.where_value=statement.where_value.substr(1,statement.where_value.size()-2);
            }

            string maybe_logical;
            where_stream>>maybe_logical;
            maybe_logical=string_to_upper(trim_copy(maybe_logical));

            if(maybe_logical=="AND"||maybe_logical=="OR"){
                statement.has_second_condition=true;
                statement.logical_operator=maybe_logical;

                where_stream>>statement.where_column2;
                where_stream>>statement.where_operator2;
                where_stream>>statement.where_value2;

                statement.where_value2=trim_copy(statement.where_value2);

                if(!statement.where_value2.empty()&&statement.where_value2.front()=='\''&&statement.where_value2.back()=='\''){
                    statement.where_value2=statement.where_value2.substr(1,statement.where_value2.size()-2);
                }
            }
        }

        if(order_pos!=string::npos){
            string order_part=trim_copy(after_from.substr(order_pos+9));
            statement.has_order_by=true;
            stringstream order_stream(order_part);
            order_stream>>statement.order_by_column;

            statement.order_by_column=trim_copy(statement.order_by_column);
            if(!statement.order_by_column.empty()&&statement.order_by_column.back()==';'){
                statement.order_by_column.pop_back();
            }
            string maybe_dir;
            order_stream>>maybe_dir;
            maybe_dir=trim_copy(maybe_dir);

            if(!maybe_dir.empty()&&maybe_dir.back()==';'){
                maybe_dir.pop_back();
            }

            if(string_to_upper(maybe_dir)=="DESC"){
                statement.order_desc=true;
            }
        }

        if(limit_pos!=string::npos){
            statement.has_limit=true;
            string limit_part;
            if(order_pos!=string::npos&&order_pos>limit_pos){
                limit_part=trim_copy(after_from.substr(limit_pos+5,order_pos-(limit_pos+5)));
            }else{
                limit_part=trim_copy(after_from.substr(limit_pos+5));
            }
            stringstream limit_stream(limit_part);
            limit_stream>>statement.limit_count;
        }
        
        return true;
    }

    if(upper_first=="DELETE"){
        string from_word;
        ss>>from_word;

        if(string_to_upper(from_word)!="FROM"){
            cout<<"Syntax error. Expected FROM.\n";
            return false;
        }

        ss>>statement.table_name;

        string where_word;
        ss>>where_word;

        if(string_to_upper(where_word)!="WHERE"){
            cout<<"Syntax error. Expected WHERE.\n";
            return false;
        }

        statement.type=StatementType::DELETE_ROWS;
        statement.has_where_clause=true;

        ss>>statement.where_column;
        ss>>statement.where_operator;
        ss>>statement.where_value;

        if(!statement.where_value.empty()&&statement.where_value.back()==';'){
            statement.where_value.pop_back();
        }

        if(!statement.where_value.empty()&&statement.where_value.front()=='\''&&statement.where_value.back()=='\''){
            statement.where_value=statement.where_value.substr(1,statement.where_value.size()-2);
        }
        return true;
    }

    if(upper_first=="DROP"){
        string second_word;
        ss>>second_word;

        if(string_to_upper(second_word)!="TABLE"){
            cout<<"Syntax error. Expected TABLE.\n";
            return false;
        }
        ss>>statement.table_name;

        if(!statement.table_name.empty()&&statement.table_name.back()==';'){
            statement.table_name.pop_back();
        }

        statement.type=StatementType::DROP_TABLE;
        return true;
    }

    if(upper_first=="UPDATE"){
        statement.type=StatementType::UPDATE_ROWS;
        statement.has_where_clause=true;
        ss>>statement.table_name;

        string set_word;
        ss>>set_word;

        if(string_to_upper(set_word)!="SET"){
            cout<<"Syntax Error. Expected SET.\n";
            return false;
        }

        string rest;
        getline(ss,rest);
        rest=trim_copy(rest);

        rest.erase(0,rest.find_first_not_of(" \t"));

        size_t where_pos=rest.find("WHERE");
        if(where_pos==string::npos){
            cout<<"Syntax error. Expected WHERE.\n";
            return false;
        }

        string set_part=trim_copy(rest.substr(0,where_pos));
        string where_part=trim_copy(rest.substr(where_pos+5));
        
        size_t eq_pos=set_part.find('=');
        if(eq_pos==string::npos){
            cout<<"Syntax error in SET clause.\n";
            return false;
        }

        statement.update_column=trim_copy(set_part.substr(0,eq_pos));
        statement.update_value=trim_copy(set_part.substr(eq_pos+1));

        if(!statement.update_value.empty()&&statement.update_value.front()=='\''&&statement.update_value.back()=='\''){
            statement.update_value=statement.update_value.substr(1,statement.update_value.size()-2);
        }

        statement.has_where_clause=true;

        stringstream where_stream(where_part);
        where_stream>>statement.where_column;
        where_stream>>statement.where_operator;
        where_stream>>statement.where_value;

        statement.where_value=trim_copy(statement.where_value);

        if(!statement.where_value.empty()&&statement.where_value.front()=='\''&&statement.where_value.back()=='\''){
            statement.where_value=statement.where_value.substr(1,statement.where_value.size()-2);
        }

        string maybe_logical;
        where_stream>>maybe_logical;
        maybe_logical=string_to_upper(trim_copy(maybe_logical));

        if(maybe_logical=="AND"||maybe_logical=="OR"){
            statement.has_second_condition=true;
            statement.logical_operator=maybe_logical;

            where_stream>>statement.where_column2;
            where_stream>>statement.where_operator2;
            where_stream>>statement.where_value2;

            statement.where_value2=trim_copy(statement.where_value2);

            if(!statement.where_value2.empty()&&statement.where_value2.front()=='\''&&statement.where_value2.back()=='\''){
                statement.where_value2=statement.where_value2.substr(1,statement.where_value2.size()-2);
            }
        }

        statement.where_column = trim_copy(statement.where_column);
        statement.where_operator = trim_copy(statement.where_operator);
        statement.where_value = trim_copy(statement.where_value);

        if(!statement.where_value.empty()&&statement.where_value.back()==';'){
            statement.where_value.pop_back();
        }

        if(!statement.where_value.empty()&&statement.where_value.front()=='\''&&statement.where_value.back()=='\''){
            statement.where_value=statement.where_value.substr(1,statement.where_value.size()-2);
        }
        return true;
    }
    return false;
}