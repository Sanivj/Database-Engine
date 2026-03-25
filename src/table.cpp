#include "table.h"
#include <iostream>
#include <cstring>
#include <algorithm>

static const uint32_t HEADER_SIZE=sizeof(uint32_t);
static const uint32_t FIXED_TEXT_SIZE=256;
static string trim_copy(string s){
    size_t start = s.find_first_not_of(" \t");
    if(start == string::npos) return "";

    size_t end = s.find_last_not_of(" \t");
    return s.substr(start, end - start + 1);
}

Table::Table(const string &filename,const Schema &schema):pager(filename),schema(schema){
    if(pager.file_length>=HEADER_SIZE){
        void *page=pager.get_page(0);
        memcpy(&num_rows,page,HEADER_SIZE);
    }else{
        num_rows=0;
    }
}

uint32_t Table::compute_row_size() const{
    uint32_t size=0;
    for(const auto& col: schema.get_columns()){
        if(col.type==DataType::INT)size+=sizeof(int);
        else if(col.type==DataType::TEXT)size+=FIXED_TEXT_SIZE;
    }
    return size;
}

void Table::serialize_row(const vector<Value> &values,void *destination) const{
    char *ptr=(char*)destination;

    for(size_t i=0;i<values.size();i++){
        if(schema.get_columns()[i].type==DataType::INT){
            int v=values[i].as_int();
            memcpy(ptr,&v,sizeof(int));
            ptr+=sizeof(int);
        }else{
            string text=values[i].as_text();
            char buffer[FIXED_TEXT_SIZE]={};
            strncpy(buffer,text.c_str(),FIXED_TEXT_SIZE-1);
            memcpy(ptr,buffer,FIXED_TEXT_SIZE);
            ptr+=FIXED_TEXT_SIZE;
        }
    }
}

void Table::deserialize_row(void *source,vector<Value> &values) const{
    char *ptr=(char*)source;

    for(const auto &col:schema.get_columns()){
        if(col.type==DataType::INT){
            int v;
            memcpy(&v,ptr,sizeof(int));
            values.push_back(Value::from_int(v));
            ptr+=sizeof(int);
        }else{
            char buffer[FIXED_TEXT_SIZE+1]={};
            memcpy(buffer,ptr,FIXED_TEXT_SIZE);
            values.push_back(Value::from_text(buffer));
            ptr+=FIXED_TEXT_SIZE;
        }
    }
}

void Table::insert_row(const vector<Value> &values){
    int pk_index=schema.get_primary_key_index();
    if(pk_index>=0){
        if(primary_key_exists(values[pk_index])){
            cout<<"Error: Duplicate primary key.\n";
            return;
        }
    }
    uint32_t row_size=compute_row_size();
    uint32_t offset=HEADER_SIZE+num_rows*row_size;
    uint32_t page_num=offset/PAGE_SIZE;
    uint32_t page_offset=offset%PAGE_SIZE;

    void *page=pager.get_page(page_num);
    void *dest=(char*)page+page_offset;

    serialize_row(values,dest);

    num_rows++;

    void *first_page=pager.get_page(0);
    memcpy(first_page,&num_rows,HEADER_SIZE);
}

void Table::select_all(){
    uint32_t row_size=compute_row_size();
    for(uint32_t i=0;i<num_rows;i++){
        uint32_t offset=HEADER_SIZE+i*row_size;
        uint32_t page_num=offset/PAGE_SIZE;
        uint32_t page_offset=offset%PAGE_SIZE;

        void *page=pager.get_page(page_num);
        void *source=(char*)page+page_offset;

        vector<Value> values;
        deserialize_row(source,values);

        cout<<"(";
        for(size_t j=0;j<values.size();j++){
            if(values[j].get_type()==DataType::INT){
                cout<<values[j].as_int();
            }else{
                cout<<values[j].as_text();
            }

            if(j!=values.size()-1){
                cout<<", ";
            }
        }
        cout<<")\n";
    }
}

bool Table::primary_key_exists(const Value &pk_value){
    int pk_index=schema.get_primary_key_index();

    if(pk_index<0)return false;

    uint32_t row_size=compute_row_size();

    for(uint32_t i=0;i<num_rows;i++){
        uint32_t offset=HEADER_SIZE+i*row_size;
        uint32_t page_num=offset/PAGE_SIZE;
        uint32_t page_offset=offset%PAGE_SIZE;

        void *page=pager.get_page(page_num);
        void *source=(char*)page+page_offset;

        vector<Value> values;
        deserialize_row(source,values);

        if(values[pk_index].get_type()==DataType::INT){
            if(values[pk_index].as_int()==pk_value.as_int()){
                return true;
            }
        }
    }
    return false;
}

void Table::select_where(const string &column,const string &op,const string &value){
    string clean_column=trim_copy(column);
    string clean_value=trim_copy(value);
    string clean_op=trim_copy(op);

    if(!clean_column.empty()&&clean_column.back()==';'){
        clean_column.pop_back();
    }
    if(!clean_value.empty()&&clean_value.back()==';'){
        clean_value.pop_back();
    }
    if(!clean_op.empty()&&clean_op.back()==';'){
        clean_op.pop_back();
    }
    int column_index=-1;
    const auto &cols=schema.get_columns();
    for(size_t i=0;i<cols.size();i++){
        if(cols[i].name==clean_column){
            column_index=i;
            break;
        }
    }
    if(column_index<0){
        cout<<"Error: Column does not exist.\n";
        return;
    }
    uint32_t row_size=compute_row_size();
    for(uint32_t i=0;i<num_rows;i++){
        uint32_t offset=HEADER_SIZE+i*row_size;
        uint32_t page_num=offset/PAGE_SIZE;
        uint32_t page_offset=offset%PAGE_SIZE;

        void *page=pager.get_page(page_num);
        void *source=(char*)page+page_offset;
        vector<Value> values;
        deserialize_row(source,values);
        const Value &cell=values[column_index];
        bool match=false;
        if(cell.get_type()==DataType::INT){
            int left=cell.as_int();
            int right=stoi(clean_value);
            if(clean_op=="=")match=(left==right);
            else if(clean_op==">")match=(left>right);
            else if(clean_op=="<")match=(left<right);
            else if(clean_op==">=")match=(left>=right);
            else if(clean_op=="<=")match=(left<=right);
            else if(clean_op=="!=")match=(left!=right);
        }else{
            string left=cell.as_text();
            string right=clean_value;

            if(clean_op=="=")match=(left==right);
            else if(clean_op=="!=")match=(left!=right);
        }

        if(match){
            cout<<"(";
            for(size_t j=0;j<values.size();j++){
                if(values[j].get_type()==DataType::INT){
                    cout<<values[j].as_int();
                }else{
                    cout<<values[j].as_text();
                }

                if(j!=values.size()-1){
                    cout<<", ";
                }
            }
            cout<<")\n";
        }
    }
}

vector<vector<Value>>Table::get_all_rows(){
    vector<vector<Value>>rows;
    uint32_t row_size=compute_row_size();
    for(uint32_t i=0;i<num_rows;i++){
        uint32_t offset=HEADER_SIZE+i*row_size;
        uint32_t page_num=offset/PAGE_SIZE;
        uint32_t page_offset=offset%PAGE_SIZE;

        void *page =pager.get_page(page_num);
        void *source=(char*)page+page_offset;

        vector<Value>values;
        deserialize_row(source,values);
        rows.push_back(values);
    }
    return rows;
}

void Table::select_columns(const vector<string>&columns,const Schema &schema){
    vector<int>column_indexes;

    const auto &schema_cols=schema.get_columns();
    for(const auto &name:columns){
        for(size_t i=0;i<schema_cols.size();i++){
            if(schema_cols[i].name==name){
                column_indexes.push_back(i);
                break;
            }
        }
    }
    uint32_t row_size=compute_row_size();
    for(uint32_t i=0;i<num_rows;i++){
        uint32_t offset=HEADER_SIZE+i*row_size;
        uint32_t page_num=offset/PAGE_SIZE;
        uint32_t page_offset=offset%PAGE_SIZE;

        void *page=pager.get_page(page_num);
        void *source=(char*)page+page_offset;
        vector<Value>values;
        deserialize_row(source,values);
        cout<<"(";
        for(size_t j=0;j<column_indexes.size();j++){
            int idx=column_indexes[j];
            if(values[idx].get_type()==DataType::INT){
                cout<<values[idx].as_int();
            }else{
                cout<<values[idx].as_text();
            }
            if(j!=column_indexes.size()-1){
                cout<<", ";
            }
        }
        cout<<")\n";
    }
}

void Table::select_columns_where(const vector<string>&columns,const Schema &schema,const string &where_column,const string &where_operator,const string &where_value){
    string clean_column=trim_copy(where_column);
    string clean_value=trim_copy(where_value);
    string clean_op=trim_copy(where_operator);

    if(!clean_column.empty()&&clean_column.back()==';'){
        clean_column.pop_back();
    }

    if(!clean_value.empty()&&clean_value.back()==';'){
        clean_value.pop_back();
    }

    if(!clean_op.empty()&&clean_op.back()==';'){
        clean_op.pop_back();
    }

    vector<int>column_indexes;
    const auto &schema_cols=schema.get_columns();
    for(const auto &name:columns){
        for(size_t i=0;i<schema_cols.size();i++){
            if(schema_cols[i].name==name){
                column_indexes.push_back(i);
                break;
            }
        }
    }
    int where_index=-1;
    for(size_t i=0;i<schema_cols.size();i++){
        if(schema_cols[i].name==clean_column){
            where_index=i;
            break;
        }
    }
    if(where_index==-1){
        cout<<"Error: Column does not exist.\n";
        return;
    }
    uint32_t row_size=compute_row_size();
    for(uint32_t i=0;i<num_rows;i++){
        uint32_t offset=HEADER_SIZE+i*row_size;
        uint32_t page_num=offset/PAGE_SIZE;
        uint32_t page_offset=offset%PAGE_SIZE;

        void *page=pager.get_page(page_num);
        void *source=(char*)page+page_offset;
        vector<Value>values;
        deserialize_row(source,values);
        bool match=false;
        string op=clean_op;
        if(values[where_index].get_type()==DataType::INT){
            int left=values[where_index].as_int();
            int right=stoi(clean_value);

            if(op=="=")match=(left==right);
            else if(op==">")match=(left>right);
            else if(op=="<")match=(left<right);
            else if(op==">=")match=(left>=right);
            else if(op=="<=")match=(left<=right);
            else if(op=="!=")match=(left!=right);
        }else{
            string left=values[where_index].as_text();
            string right=clean_value;
            if(op=="=")match=(left==right);
            else if(op=="!=")match=(left!=right);
        }
        if(match){
            cout<<"(";
            for(size_t j=0;j<column_indexes.size();j++){
                int idx=column_indexes[j];
                if(values[idx].get_type()==DataType::INT){
                    cout<<values[idx].as_int();
                }else{
                    cout<<values[idx].as_text();
                }
                if(j!=column_indexes.size()-1){
                    cout<<", ";
                }
            }
            cout<<")\n";
        }
    }
}

void Table::delete_where(const string &column,const string &op,const string &value,bool print_result){
    string clean_column=trim_copy(column);
    string clean_op=trim_copy(op);
    string clean_value=trim_copy(value);

    if(!clean_column.empty()&&clean_column.back()==';'){
        clean_column.pop_back();
    }
    if(!clean_op.empty()&&clean_op.back()==';'){
        clean_op.pop_back();
    }
    if(!clean_value.empty()&&clean_value.back()==';'){
        clean_value.pop_back();
    }
    int column_index=-1;
    const auto &cols=schema.get_columns();
    for(size_t i=0;i<cols.size();i++){
        if(cols[i].name==clean_column){
            column_index=i;
            break;
        }
    }
    if(column_index<0){
        cout<<"Error: Column does not exist.\n";
        return;
    }

    vector<vector<Value>>all_rows=get_all_rows();
    vector<vector<Value>> kept_rows;
    
    for(const auto &row:all_rows){
        const Value &cell=row[column_index];
        bool match=false;
        if(cell.get_type()==DataType::INT){
            int left=cell.as_int();
            int right=stoi(clean_value);

            if(clean_op=="=")match=(left==right);
            else if(clean_op==">")match=(left>right);
            else if(clean_op=="<")match=(left<right);
            else if(clean_op==">=")match=(left>=right);
            else if(clean_op=="<=")match=(left<=right);
            else if(clean_op=="!=")match=(left!=right);
        }else{
            string left=cell.as_text();
            
            if(clean_op=="=")match=(left==clean_value);
            else if(clean_op=="!=")match=(left!=clean_value);
        }

        if(!match){
            kept_rows.push_back(row);
        }
    }
    num_rows=0;
    void *first_page=pager.get_page(0);
    memset(first_page,0,PAGE_SIZE);
    memcpy(first_page,&num_rows,HEADER_SIZE);

    uint32_t rows_size=compute_row_size();

    for(const auto &row:kept_rows){
        uint32_t offset=HEADER_SIZE+num_rows*rows_size;
        uint32_t page_num=offset/PAGE_SIZE;
        uint32_t page_offset=offset%PAGE_SIZE;

        void *page=pager.get_page(page_num);
        void *dest=(char*)page+page_offset;

        serialize_row(row,dest);
        num_rows++;
    }
    first_page=pager.get_page(0);
    memcpy(first_page,&num_rows,HEADER_SIZE);

    if(print_result){
        cout<<"Deleted "<<(all_rows.size()-kept_rows.size())<<" rows(s).\n";
    }
}

void Table::update_where(const string &target_column,const string &new_value,const string &where_column,const string &where_operator,const string &where_value){
    string clean_target=trim_copy(target_column);
    string clean_new_value=trim_copy(new_value);
    string clean_where_column=trim_copy(where_column);
    string clean_where_op=trim_copy(where_operator);
    string clean_where_value=trim_copy(where_value);

    if(!clean_target.empty()&&clean_target.back()==';')clean_target.pop_back();
    if(!clean_new_value.empty()&&clean_new_value.back()==';')clean_new_value.pop_back();
    if(!clean_where_column.empty()&&clean_where_column.back()==';')clean_where_column.pop_back();
    if(!clean_where_op.empty()&&clean_where_op.back()==';')clean_where_op.pop_back();
    if(!clean_where_value.empty()&&clean_where_value.back()==';')clean_where_value.pop_back();

    const auto &cols=schema.get_columns();

    int target_index=-1;
    int where_index=-1;

    for(size_t i=0;i<cols.size();i++){
        if(cols[i].name==clean_target)target_index=i;
        if(cols[i].name==clean_where_column)where_index=i;
    }

    if(target_index==-1||where_index==-1){
        cout<<"Error: Column does not exist.\n";
        return;
    }

    vector<vector<Value>>all_rows=get_all_rows();
    int updated_count=0;

    for(auto &row:all_rows){
        const Value &cell=row[where_index];
        bool match=false;

        if(cell.get_type()==DataType::INT){
            int left=cell.as_int();
            int right=stoi(clean_where_value);

            if(clean_where_op=="=")match=(left==right);
            else if(clean_where_op==">")match=(left>right);
            else if(clean_where_op=="<")match=(left<right);
            else if(clean_where_op==">=")match=(left>=right);
            else if(clean_where_op=="<=")match=(left<=right);
            else if(clean_where_op=="!=")match=(left!=right);
        }else{
            string left=cell.as_text();

            if(clean_where_op=="=")match=(left==clean_where_value);
            else if(clean_where_op=="!=")match=(left!=clean_where_value);
        }

        if(match){
            if(cols[target_index].type==DataType::INT){
                row[target_index]=Value::from_int(stoi(clean_new_value));
            }else{
                row[target_index]=Value::from_text(clean_new_value);
            }
            updated_count++;
        }
    }
    num_rows=0;
    
    void *first_page=pager.get_page(0);
    memset(first_page,0,PAGE_SIZE);
    memcpy(first_page,&num_rows,HEADER_SIZE);

    uint32_t row_size=compute_row_size();

    for(const auto &row:all_rows){
        uint32_t offset=HEADER_SIZE+num_rows*row_size;
        uint32_t page_num=offset/PAGE_SIZE;
        uint32_t page_offset=offset%PAGE_SIZE;

        void *page=pager.get_page(page_num);
        void *dest=(char*)page+page_offset;

        serialize_row(row,dest);
        num_rows++;
    }

    first_page=pager.get_page(0);
    memcpy(first_page,&num_rows,HEADER_SIZE);

    cout<<"Updated "<<updated_count<<" row(s).\n";
}

vector<vector<Value>>Table::filter_rows(const Statement &statement){
    vector<vector<Value>>result;

    vector<vector<Value>>all_rows=get_all_rows();
    const auto &cols=schema.get_columns();

    int idx1=-1;
    for(size_t i=0;i<cols.size();i++){
        if(cols[i].name==statement.where_column){
            idx1=i;
            break;
        }
    }

    if(idx1==-1){
        cout<<"Error: Column does not exist.\n";
        return result;
    }

    int idx2=-1;
    if(statement.has_second_condition){
        for(size_t i=0;i<cols.size();i++){
            if(cols[i].name==statement.where_column2){
                idx2=i;
                break;
            }
        }

        if(idx2==-1){
            cout<<"Error: Column does not exist.\n";
            return result;
        }
    }

    for(const auto &row:all_rows){
        bool match1=false;
        const Value &cell1=row[idx1];

        if(cell1.get_type()==DataType::INT){
            int left=cell1.as_int();
            int right=stoi(statement.where_value);

            if(statement.where_operator=="=")match1=(left==right);
            else if(statement.where_operator==">")match1=(left>right);
            else if(statement.where_operator=="<")match1=(left<right);
            else if(statement.where_operator==">=")match1=(left>=right);
            else if(statement.where_operator=="<=")match1=(left<=right);
            else if(statement.where_operator=="!=")match1=(left!=right);
        }else{
            string left=cell1.as_text();

            if(statement.where_operator=="=")match1=(left==statement.where_value);
            else if(statement.where_operator=="!=")match1=(left!=statement.where_value);
        }

        bool match2=false;
        if(statement.has_second_condition){
            const Value &cell2=row[idx2];

            if(cell2.get_type()==DataType::INT){
                int left=cell2.as_int();
                int right=stoi(statement.where_value2);

                if(statement.where_operator2=="=")match2=(left==right);
                else if(statement.where_operator2==">")match2=(left>right);
                else if(statement.where_operator2=="<")match2=(left<right);
                else if(statement.where_operator2==">=")match2=(left>=right);
                else if(statement.where_operator2=="<=")match2=(left<=right);
                else if(statement.where_operator2=="!=")match2=(left!=right);
            }else{
                string left=cell2.as_text();

                if(statement.where_operator2=="=")match2=(left==statement.where_value2);
                else if(statement.where_operator2=="!=")match2=(left!=statement.where_value2);
            }
        }
        bool final_match;
        if(statement.has_second_condition){
            if(statement.logical_operator=="AND"){
                final_match=match1&&match2;
            }else{
                final_match=match1||match2;
            }
        }else{
            final_match=match1;
        }

        if(final_match){
            result.push_back(row);
        }
    }
    return result;
}

void Table::print_rows(const vector<vector<Value>>&rows)const{
    for(const auto &values:rows){
        cout<<"(";
        for(size_t j=0;j<values.size();j++){
            if(values[j].get_type()==DataType::INT){
                cout<<values[j].as_int();
            }else{
                cout<<values[j].as_text();
            }

            if(j!=values.size()-1){
                cout<<", ";
            }
        }
        cout<<")\n";
    }
}

void Table::print_selected_columns(const vector<vector<Value>>&rows,const vector<string>&columns,const Schema &schema)const{
    vector<int>column_indexes;
    const auto &schema_cols=schema.get_columns();

    for(const auto &raw_name:columns){
        string name=raw_name;
        if(!name.empty()&&name.back()==';'){
            name.pop_back();
        }
        name=trim_copy(name);
        bool found=false;
        for(size_t i=0;i<schema_cols.size();i++){
            if(schema_cols[i].name==name){
                column_indexes.push_back(i);
                found=true;
                break;
            }
        }
        if(!found){
            cout<<"Error: Column does not exist.\n";
            return;
        }
    }
    for(const auto &values:rows){
        cout<<"(";
        for(size_t j=0;j<column_indexes.size();j++){
            int idx=column_indexes[j];
            if(values[idx].get_type()==DataType::INT){
                cout<<values[idx].as_int();
            }else{
                cout<<values[idx].as_text();
            }

            if(j!=column_indexes.size()-1){
                cout<<", ";
            }
        }
        cout<<")\n";
    }
}

void Table::sort_rows(vector<vector<Value>>&rows,const string &order_by_column,bool descending)const{
    string clean_column=trim_copy(order_by_column);
    if(!clean_column.empty()&&clean_column.back()==';'){
        clean_column.pop_back();
    }
    clean_column=trim_copy(clean_column);
    int order_index=-1;
    const auto &cols=schema.get_columns();

    for(size_t i=0;i<cols.size();i++){
        if(cols[i].name==clean_column){
            order_index=i;
            break;
        }
    }

    if(order_index==-1){
        cout<<"Error: Column does not exist.\n";
        return;
    }

    sort(rows.begin(),rows.end(),[&](const vector<Value>&a,const vector<Value>&b){
        const Value &left=a[order_index];
        const Value &right=b[order_index];

        bool result=false;
        if(left.get_type()==DataType::INT){
            result=left.as_int()<right.as_int();
        }else{
            result=left.as_text()<right.as_text();
        }

        return descending?!result:result;
    });
}