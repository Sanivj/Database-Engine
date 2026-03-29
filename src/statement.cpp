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

        if(string_to_upper(second_word)=="INDEX"){
            string index_name;
            ss>>index_name;
            string on_word;
            ss>>on_word;

            if(string_to_upper(on_word)!="ON"){
                cout<<"Syntax error.Expected ON.\n";
                return false;
            }
            string rest;
            getline(ss,rest);
            rest=trim_copy(rest);
            if(!rest.empty()&&rest.back()==';')rest.pop_back();
            rest=trim_copy(rest);

            size_t open=rest.find('(');
            size_t close=rest.find(')');
            if(open==string::npos||close==string::npos){
                cout<<"Syntax error. Expected table (column).\n";
                return false;
            }
            string table_name=trim_copy(rest.substr(0,open));
            string column_name=trim_copy(rest.substr(open+1,close-open-1));

            statement.type=StatementType::CREATE_INDEX;
            statement.index_name=index_name;
            statement.table_name=table_name;
            statement.index_column=column_name;
            return true;
        }

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
            
            if(string_to_upper(maybe_primary)=="PRIMARY"&&string_to_upper(maybe_key)=="KEY"){
                is_primary=true;
            }
            if(col_stream >> maybe_primary){
                if(string_to_upper(maybe_primary) == "PRIMARY"){
                    col_stream >> maybe_key;
                    if(string_to_upper(maybe_key) == "KEY"){
                        is_primary = true;
                    }
                }
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
        size_t close_paren=rest.rfind(')');

        if(open_paren==string::npos||close_paren==string::npos){
            cout<<"Syntax error in VALUES.\n";
            return false;
        }

        if(open_paren >= close_paren){
            cout<<"Syntax error in column definition.\n";
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
            string upper_val=value;
            transform(upper_val.begin(),upper_val.end(),upper_val.begin(),::toupper);
            if(upper_val=="NULL")value="__NULL__";
            statement.insert_values.push_back(value);
        }
        statement.type=StatementType::INSERT;
        return true;
    }

    if(upper_first=="SELECT"){
        statement.type=StatementType::SELECT_ALL;
        statement.select_columns.clear();
        statement.select_aliases.clear();
        statement.has_where_clause=false;
        statement.has_order_by=false;
        statement.order_desc=false;
        statement.has_limit=false;
        statement.limit_count=0;
        statement.has_second_condition=false;
        statement.logical_operator="";
        statement.has_offset=false;
        statement.offset_count=0;
        statement.aggregate_type=AggregateType::NONE;
        statement.aggregate_column="";
        statement.has_group_by=false;
        statement.group_by_column="";
        statement.has_distinct=false;
        statement.join_table_alias="";
        statement.table_alias="";

        string rest;
        getline(ss,rest);
        rest=trim_copy(rest);

        if(string_to_upper(rest).substr(0,8)=="DISTINCT"){
            statement.has_distinct=true;
            rest=trim_copy(rest.substr(8));
        }

        size_t from_pos=string_to_upper(rest).find("FROM");
        if(from_pos==string::npos){
            cout<<"Syntax error. Expected FROM.\n";
            return false;
        }

        string columns_part=trim_copy(rest.substr(0,from_pos));
        string after_from=trim_copy(rest.substr(from_pos+4));

        string upper_col=string_to_upper(columns_part);
        if(columns_part=="*"){
            statement.select_all_columns=true;
        }else{
            stringstream col_stream(columns_part);
            string col;

            while(getline(col_stream,col,',')){
                col=trim_copy(col);
                string upper=string_to_upper(col);
                if(upper.find("COUNT(")==0){
                    statement.aggregate_type=AggregateType::COUNT;

                    size_t open=col.find('(');
                    size_t close=col.find(')');
                    if(open!=string::npos&&close!=string::npos){
                        statement.aggregate_column=trim_copy(col.substr(open+1,close-open-1));
                    }
                }else if(upper.find("SUM(")==0){
                    statement.aggregate_type=AggregateType::SUM;

                    size_t open=col.find('(');
                    size_t close=col.find(')');
                    if(open!=string::npos&&close!=string::npos){
                        statement.aggregate_column=trim_copy(col.substr(open+1,close-open-1));
                    }
                }else if(upper.find("AVG(")==0){
                    statement.aggregate_type=AggregateType::AVG;

                    size_t open=col.find('(');
                    size_t close=col.find(')');
                    if(open!=string::npos&&close!=string::npos){
                        statement.aggregate_column=trim_copy(col.substr(open+1,close-open-1));
                    }
                }else if(upper.find("MIN(")==0){
                    statement.aggregate_type=AggregateType::MIN;

                    size_t open=col.find('(');
                    size_t close=col.find(')');
                    if(open!=string::npos&&close!=string::npos){
                        statement.aggregate_column=trim_copy(col.substr(open+1,close-open-1));
                    }
                }else if(upper.find("MAX(")==0){
                    statement.aggregate_type=AggregateType::MAX;

                    size_t open=col.find('(');
                    size_t close=col.find(')');
                    if(open!=string::npos&&close!=string::npos){
                        statement.aggregate_column=trim_copy(col.substr(open+1,close-open-1));
                    }
                }else{
                    size_t dot=col.find('.');
                    if(dot!=string::npos)col=col.substr(dot+1);

                    string upper_col=string_to_upper(col);
                    size_t as_pos=upper_col.find(" AS ");
                    if(as_pos!=string::npos){
                        string alias=trim_copy(col.substr(as_pos+4));
                        col=trim_copy(col.substr(0,as_pos));
                        statement.select_columns.push_back(col);
                        statement.select_aliases.push_back(alias);
                    }else{
                        statement.select_columns.push_back(col);
                        statement.select_aliases.push_back(col);
                    }
                }
            }
        }
        size_t where_pos=string_to_upper(after_from).find("WHERE");
        size_t order_pos=string_to_upper(after_from).find("ORDER BY");
        size_t limit_pos=string_to_upper(after_from).find("LIMIT");
        size_t offset_pos=string_to_upper(after_from).find("OFFSET");
        size_t group_pos=string_to_upper(after_from).find("GROUP BY");
        size_t end_pos=string_to_upper(after_from).length();
        size_t join_pos=string_to_upper(after_from).find("JOIN");
        size_t left_join_pos=string_to_upper(after_from).find("LEFT JOIN");
        size_t right_join_pos=string_to_upper(after_from).find("RIGHT JOIN");
        size_t full_outer_join_pos=string_to_upper(after_from).find("FULL OUTER JOIN");

        bool found_left_join=false;
        bool found_right_join=false;
        bool found_full_outer_join=false;
        if(full_outer_join_pos!=string::npos){
            found_full_outer_join=true;
            join_pos=full_outer_join_pos;
        }else if(left_join_pos!=string::npos){
            found_left_join=true;
            join_pos=left_join_pos;
        }else if(right_join_pos!=string::npos){
            found_right_join=true;
            join_pos=right_join_pos;
        }

        if(join_pos!=string::npos)end_pos=min(end_pos,join_pos);
        size_t actual_join_pos=string_to_upper(after_from).find("JOIN");
        if(where_pos!=string::npos)end_pos=min(end_pos,where_pos);
        if(group_pos!=string::npos)end_pos=min(end_pos,group_pos);
        if(order_pos!=string::npos)end_pos=min(end_pos,order_pos);
        if(limit_pos!=string::npos)end_pos=min(end_pos,limit_pos);
        if(offset_pos!=string::npos)end_pos=min(end_pos,offset_pos);



        string table_part=trim_copy(after_from.substr(0,end_pos));
        {
            stringstream tss(table_part);
            string tname,talias;
            tss>>tname;
            statement.table_name=tname;
            if(tss>>talias){
                statement.table_alias=talias;
            }else{
                statement.table_alias=tname;
            }
        }

        if(join_pos!=string::npos){
            statement.has_join=true;
            statement.is_left_join=found_left_join;
            statement.is_right_join=found_right_join;
            statement.is_full_outer_join=found_full_outer_join;
            
            string join_part=trim_copy(after_from.substr(actual_join_pos+4));

            stringstream join_stream(join_part);

            string jt;
            join_stream>>jt;
            statement.join_table=jt;

            string peek;
            streampos before=join_stream.tellg();
            join_stream>>peek;
            if(string_to_upper(peek)!="ON"){
                statement.join_table_alias=peek;
                join_stream>>peek;
            }else{
                statement.join_table_alias=jt;
            }
            string on_word=peek;

            if(string_to_upper(on_word)!="ON"){
                cout<<"Syntax Error. Expected ON.\n";
                return false;
            }

            string left_expr,eq,right_expr;

            join_stream>>left_expr;
            join_stream>>eq;
            join_stream>>right_expr;

            size_t dot1=left_expr.find('.');
            size_t dot2=right_expr.find('.');

            if(dot1==string::npos||dot2==string::npos){
                cout<<"Syntax error in JOIN condition.\n";
                return false;
            }

            statement.join_left_column=left_expr.substr(dot1+1);
            statement.join_right_column=right_expr.substr(dot2+1);
        }

        if(where_pos!=string::npos){
            statement.has_where_clause=true;

            string where_part;

            size_t end_where=after_from.length();

            if(group_pos!=string::npos&&group_pos>where_pos)end_where=group_pos;
            else if(order_pos!=string::npos&&order_pos>where_pos)end_where=order_pos;
            else if(limit_pos!=string::npos&&limit_pos>where_pos)end_where=limit_pos;
            else if(offset_pos!=string::npos&&offset_pos>where_pos)end_where=offset_pos;

            where_part=trim_copy(after_from.substr(where_pos+5,end_where-(where_pos+5)));

            stringstream where_stream(where_part);

            where_stream>>statement.where_column;
            where_stream>>statement.where_operator;
            where_stream>>statement.where_value;

            size_t dot1=statement.where_column.find('.');
            if(dot1!=string::npos) statement.where_column=statement.where_column.substr(dot1+1);

            statement.where_value=trim_copy(statement.where_value);

            if(!statement.where_value.empty()&&statement.where_value.back()==';')statement.where_value.pop_back();

            if(!statement.where_value.empty()&&statement.where_value.front()=='\''&&statement.where_value.back()=='\'')statement.where_value=statement.where_value.substr(1,statement.where_value.size()-2);

            string maybe_logical;

            where_stream>>maybe_logical;

            maybe_logical=string_to_upper(trim_copy(maybe_logical));

            if(maybe_logical=="AND"||maybe_logical=="OR"){
                statement.has_second_condition=true;
                statement.logical_operator=maybe_logical;

                where_stream>>statement.where_column2;
                where_stream>>statement.where_operator2;
                where_stream>>statement.where_value2;

                size_t dot2=statement.where_column2.find('.');
                if(dot2!=string::npos) statement.where_column2=statement.where_column2.substr(dot2+1);
                
                statement.where_value2=trim_copy(statement.where_value2);

                if(!statement.where_value2.empty()&&statement.where_value2.back()==';')statement.where_value2.pop_back();

                if(!statement.where_value2.empty()&&statement.where_value2.front()=='\''&&statement.where_value2.back()=='\'')statement.where_value2=statement.where_value2.substr(1,statement.where_value2.size()-2);
            }
        }

        if(group_pos!=string::npos){
            statement.has_group_by=true;

            string group_part;

            size_t end_pos=after_from.length();

            if(order_pos!=string::npos&&order_pos>group_pos){
                end_pos=order_pos;
            }else if(limit_pos!=string::npos&&limit_pos>group_pos){
                end_pos=limit_pos;
            }else if(offset_pos!=string::npos&&offset_pos>group_pos){
                end_pos=offset_pos;
            }
            group_part=trim_copy(after_from.substr(group_pos+8,end_pos-(group_pos+8)));
            stringstream group_stream(group_part);
            group_stream>>statement.group_by_column;
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

            size_t dot=statement.order_by_column.find('.');
            if(dot!=string::npos) statement.order_by_column=statement.order_by_column.substr(dot+1);

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

        if(offset_pos!=string::npos){
            statement.has_offset=true;

            string offset_part;
            offset_part=trim_copy(after_from.substr(offset_pos+6));
            stringstream offset_stream(offset_part);
            offset_stream>>statement.offset_count;
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

        if(string_to_upper(second_word)=="INDEX"){
            ss>>statement.index_name;
            if(!statement.index_name.empty()&&statement.index_name.back()==';')statement.index_name.pop_back();
            statement.type=StatementType::DROP_INDEX;
            return true;
        }

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