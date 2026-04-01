#include "table.h"
#include <iostream>
#include <cstring>
#include <algorithm>
#include <climits>
#include <map>

static const uint32_t HEADER_SIZE=sizeof(uint32_t);
static const uint32_t FIXED_TEXT_SIZE=256;
static string trim_copy(string s){
    size_t start = s.find_first_not_of(" \t");
    if(start == string::npos) return "";

    size_t end = s.find_last_not_of(" \t");
    return s.substr(start, end - start + 1);
}
static void row_slot(uint32_t row_index,uint32_t row_size,uint32_t &out_page,uint32_t &out_offset){
    uint32_t rows_per_page=PAGE_SIZE/row_size;
    if(rows_per_page==0)rows_per_page=1;
    out_page=1+(row_index/rows_per_page);
    out_offset=(row_index%rows_per_page)*row_size;
}

Table::Table(const string &filename,const Schema &schema):pager(filename),schema(schema){
    if(pager.file_length>=HEADER_SIZE){
        void *page = pager.get_page(0);
        if(page != nullptr){
            memcpy(&num_rows, page, HEADER_SIZE);
        }else{
            num_rows = 0;
        }
    }else{
        num_rows=0;
    }
}

uint32_t Table::compute_row_size() const{
    uint32_t size=0;
    for(const auto& col: schema.get_columns()){
        size+=1;
        if(col.type==DataType::INT)size+=sizeof(int);
        else if(col.type==DataType::TEXT)size+=FIXED_TEXT_SIZE;
    }
    return size;
}

void Table::serialize_row(const vector<Value> &values,void *destination) const{
    char *ptr=(char*)destination;

    for(size_t i=0;i<values.size();i++){
        char null_flag=values[i].is_null()?1:0;
        memcpy(ptr,&null_flag,1);
        ptr+=1;

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
        char null_flag=0;
        memcpy(&null_flag,ptr,1);
        ptr+=1;

        if(col.type==DataType::INT){
            int v;
            memcpy(&v,ptr,sizeof(int));
            ptr+=sizeof(int);
            if(null_flag)values.push_back(Value::make_null());
            else values.push_back(Value::from_int(v));
        }else{
            char buffer[FIXED_TEXT_SIZE+1]={};
            memcpy(buffer,ptr,FIXED_TEXT_SIZE);
            ptr+=FIXED_TEXT_SIZE;
            if(null_flag)values.push_back(Value::make_null());
            else values.push_back(Value::from_text(buffer));
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
    uint32_t page_num,page_offset;
    row_slot(num_rows,row_size,page_num,page_offset);

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
        uint32_t page_num,page_offset;
        row_slot(i,row_size,page_num,page_offset);

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
        uint32_t page_num,page_offset;
        row_slot(i,row_size,page_num,page_offset);

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
        uint32_t page_num,page_offset;
        row_slot(i,row_size,page_num,page_offset);

        void *page=pager.get_page(page_num);
        void *source=(char*)page+page_offset;
        vector<Value> values;
        deserialize_row(source,values);
        const Value &cell=values[column_index];
        bool match=false;
        if(cell.get_type()==DataType::INT){
            int left=cell.as_int();
            int right;
            try{
                right=stoi(clean_value);
            }catch(...){
                cout<<"Error: Invalid number.\n";
                return;
            }
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
        uint32_t page_num,page_offset;
        row_slot(i,row_size,page_num,page_offset);

        void *page =pager.get_page(page_num);
        void *source=(char*)page+page_offset;

        vector<Value>values;
        deserialize_row(source,values);
        rows.push_back(values);
    }
    return rows;
}


vector<vector<Value>>Table::scan_rows(const Statement &statement,HashIndex *index){
    vector<vector<Value>>result;
    const auto &cols=schema.get_columns();

    if(index!=nullptr&&statement.has_where_clause&&statement.where_operator=="="&&!statement.has_second_condition&&!statement.has_order_by){
        string lookup_key=trim_copy(statement.where_value);
        vector<uint32_t>row_nums=index->lookup(lookup_key);

        int count=0;
        int skip=statement.has_offset?statement.offset_count:0;

        for(uint32_t rn:row_nums){
            if(skip>0){
                skip--;
                continue;
            }
            vector<Value>row=get_row(rn);
            if(!row.empty()){
                result.push_back(row);
                count++;
                if(statement.has_limit&&count>=statement.limit_count)break;
            }
        }
        return result;
    }
    uint32_t row_size=compute_row_size();
    int count=0;
    int skip=statement.has_offset?statement.offset_count:0;

    bool need_all=(statement.has_order_by||statement.has_group_by);

    for(uint32_t i=0;i<num_rows;i++){
        uint32_t page_num,page_offset;
        row_slot(i,row_size,page_num,page_offset);

        void *page=pager.get_page(page_num);
        void *source=(char*)page+page_offset;

        vector<Value>values;
        deserialize_row(source,values);

        if(statement.has_where_clause){
            bool match=false;
            int idx1=-1;
            for(size_t c=0;c<cols.size();c++){
                if(cols[c].name==statement.where_column){
                    idx1=c;
                    break;
                }
            }
            if(idx1==-1)continue;

            const Value &cell=values[idx1];
            if(cell.is_null()){
                match=false;
            }else if(cell.get_type()==DataType::INT){
                int left=cell.as_int();
                int right;
                try{
                    right=stoi(trim_copy(statement.where_value));
                }catch(...){
                    continue;
                }
                if(statement.where_operator=="=")match=(left==right);
                else if(statement.where_operator==">")match=(left>right);
                else if(statement.where_operator==">=")match=(left>=right);
                else if(statement.where_operator=="<")match=(left<right);
                else if(statement.where_operator=="<=")match=(left<=right);
                else if(statement.where_operator=="!=")match=(left!=right);
            }else{
                string left=cell.as_text();
                string right=trim_copy(statement.where_value);
                if(statement.where_operator=="=")match=(left==right);
                else if(statement.where_operator=="!=")match=(left!=right);
            }
            
            if(statement.has_second_condition){
                int idx2=-1;
                for(size_t c=0;c<cols.size();c++){
                    if(cols[c].name==statement.where_column2){idx2=c;break;}
                }
                bool match2=false;
                if(idx2!=-1){
                    const Value &cell2=values[idx2];
                    if(!cell2.is_null()){
                        if(cell2.get_type()==DataType::INT){
                            int left=cell2.as_int();
                            int right;
                            try
                            {
                                right=stoi(trim_copy(statement.where_value2));
                            }catch(...)
                            {
                                right=0;
                            }
                            if(statement.where_operator2=="=") match2=(left==right);
                            else if(statement.where_operator2==">") match2=(left>right);
                            else if(statement.where_operator2=="<") match2=(left<right);
                            else if(statement.where_operator2==">=")match2=(left>=right);
                            else if(statement.where_operator2=="<=")match2=(left<=right);
                            else if(statement.where_operator2=="!=")match2=(left!=right);
                        }else{
                            string left=cell2.as_text();
                            string right=trim_copy(statement.where_value2);
                            if(statement.where_operator2=="=") match2=(left==right);
                            else if(statement.where_operator2=="!=")match2=(left!=right);
                        }
                    }
                }
                if(statement.logical_operator=="AND") match=match&&match2;
                else                                  match=match||match2;
            }
            if(!match) continue;
        }
        if(skip>0){skip--;continue;}

        result.push_back(values);
        count++;

        if(!need_all&&statement.has_limit&&count>=statement.limit_count) break;
    }
    return result;
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
        uint32_t page_num,page_offset;
        row_slot(i,row_size,page_num,page_offset);

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
        uint32_t page_num,page_offset;
        row_slot(i,row_size,page_num,page_offset);

        void *page=pager.get_page(page_num);
        void *source=(char*)page+page_offset;
        vector<Value>values;
        deserialize_row(source,values);
        bool match=false;
        string op=clean_op;
        if(values[where_index].get_type()==DataType::INT){
            int left=values[where_index].as_int();
            int right;
            try{
                right=stoi(clean_value);
            }catch(...){
                cout<<"Error: Invalid number.\n";
                return;
            }

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
            int right;
            try{
                right=stoi(clean_value);
            }catch(...){
                cout<<"Error: Invalid number.\n";
                return;
            }

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
        uint32_t page_num,page_offset;
        row_slot(num_rows,rows_size,page_num,page_offset);

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
            int right;
            try{
                right=stoi(clean_where_value);
            }catch(...){
                cout<<"Error: Invalid number.\n";
                return;
            }

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
                int val;
                try{
                    val=stoi(clean_new_value);
                }catch(...){
                    cout<<"Error: Invalid number.\n";
                    return;
                }
                row[target_index]=Value::from_int(val);
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

    uint32_t rows_size=compute_row_size();

    for(const auto &row:all_rows){
        uint32_t page_num,page_offset;
        row_slot(num_rows,rows_size,page_num,page_offset);

        void *page=pager.get_page(page_num);
        void *dest=(char*)page+page_offset;

        serialize_row(row,dest);
        num_rows++;
    }

    first_page=pager.get_page(0);
    memcpy(first_page,&num_rows,HEADER_SIZE);

    cout<<"Updated "<<updated_count<<" row(s).\n";
}

vector<vector<Value>>Table::filter_rows(const Statement &statement,HashIndex * index){
    vector<vector<Value>>result;
    string logical_op=trim_copy(statement.logical_operator);
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

    if(index!=nullptr&&statement.where_operator=="="&&!statement.has_second_condition){
        string lookup_key=trim_copy(statement.where_value);
        vector<uint32_t>row_nums=index->lookup(lookup_key);

        for(uint32_t rn:row_nums){
            vector<Value>row=get_row(rn);
            if(!row.empty())result.push_back(row);
        }
        return result;
    }

    vector<vector<Value>>all_rows=get_all_rows();

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
            int right;
            try{
                string val=trim_copy(statement.where_value);
                if(!val.empty()&&val.back()==';')val.pop_back();
                right=stoi(val);
            }catch(...){
                cout<<"Error: Invalid number.\n";
                return {};
            }

            if(trim_copy(statement.where_operator)=="=")match1=(left==right);
            else if(trim_copy(statement.where_operator)==">")match1=(left>right);
            else if(trim_copy(statement.where_operator)=="<")match1=(left<right);
            else if(trim_copy(statement.where_operator)==">=")match1=(left>=right);
            else if(trim_copy(statement.where_operator)=="<=")match1=(left<=right);
            else if(trim_copy(statement.where_operator)=="!=")match1=(left!=right);
        }else{
            string left=cell1.as_text();

            if(trim_copy(statement.where_operator)=="=")match1=(left==trim_copy(statement.where_value));
            else if(trim_copy(statement.where_operator)=="!=")match1=(left!=trim_copy(statement.where_value));
        }

        bool match2=false;
        if(statement.has_second_condition){
            const Value &cell2=row[idx2];

            if(cell2.get_type()==DataType::INT){
                int left=cell2.as_int();
                int right;
                try{
                    string val=trim_copy(statement.where_value2);
                    if(!val.empty()&&val.back()==';')val.pop_back();
                    right=stoi(val);
                }catch(...){
                    cout<<"Error: Invalid number.\n";
                    return {};
                }

                if(trim_copy(statement.where_operator2)=="=")match2=(left==right);
                else if(trim_copy(statement.where_operator2)==">")match2=(left>right);
                else if(trim_copy(statement.where_operator2)=="<")match2=(left<right);
                else if(trim_copy(statement.where_operator2)==">=")match2=(left>=right);
                else if(trim_copy(statement.where_operator2)=="<=")match2=(left<=right);
                else if(trim_copy(statement.where_operator2)=="!=")match2=(left!=right);
            }else{
                string left=cell2.as_text();

                if(trim_copy(statement.where_operator2)=="=")match2=(left==trim_copy(statement.where_value2));
                else if(trim_copy(statement.where_operator2)=="!=")match2=(left!=trim_copy(statement.where_value2));
            }
        }
        bool final_match;
        if(statement.has_second_condition){
            if(logical_op=="AND"){
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

void Table::print_rows(const vector<vector<Value>>&rows,bool distinct)const{
    vector<string>seen;
    for(const auto &values:rows){
        string key;
        for(const auto &val:values){
            if(val.get_type()==DataType::INT){
                key+=to_string(val.as_int())+"|";
            }else{
                key+=val.as_text()+"|";
            }
        }
        if(distinct){
            bool found=false;
            for(const auto &s:seen){
                if(s==key){
                    found=true;
                    break;
                }
            }
            if(found)continue;
            seen.push_back(key);
        }
        cout<<"(";
        for(size_t j=0;j<values.size();j++){
            if(values[j].is_null()){
                cout<<"NULL";
            }else if(values[j].get_type()==DataType::INT){
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

void Table::print_rows_with_schema(const vector<vector<Value>>&rows,const Schema &schema,bool distinct)const{
    const auto &cols=schema.get_columns();

    cout<<"(";
    for(size_t i=0;i<cols.size();i++){
        cout<<cols[i].name;
        if(i!=cols.size()-1)cout<<", ";
    }
    cout<<")\n";
    cout<<string(40,'-')<<"\n";
    vector<string>seen;
    for(const auto &values:rows){
        string key;
        for(const auto &val:values){
            if(val.get_type()==DataType::INT){
                key+=to_string(val.as_int())+"|";
            }else{
                key+=val.as_text()+"|";
            }
        }
        if(distinct){
            bool found=false;
            for(const auto &s:seen){
                if(s==key){
                    found=true;
                    break;
                }
            }
            if(found)continue;
            seen.push_back(key);
        }
        cout<<"(";
        for(size_t j=0;j<values.size();j++){
            if(values[j].is_null()){
                cout<<"NULL";
            }else if(values[j].get_type()==DataType::INT)
                cout<<values[j].as_int();
            else
                cout<<values[j].as_text();
            if(j!=values.size()-1) cout<<", ";
        }
        cout<<")\n";
    }
}


void Table::print_selected_columns(const vector<vector<Value>>&rows,const vector<string>&columns,const Schema &schema,bool distinct,const vector<string>&aliases)const{
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

    cout<<"(";
    for(size_t i=0;i<columns.size();i++){
        if(!aliases.empty()&&i<aliases.size()){
            cout<<aliases[i];
        }else{
            cout<<columns[i];
        }
        if(i!=columns.size()-1)cout<<", ";
    }
    cout<<")\n";
    cout<<string(40,'-')<<"\n";

    vector<string>seen;
    for(const auto &values:rows){
        string key;
        for(int idx:column_indexes){
            if(values[idx].get_type()==DataType::INT){
                key+=to_string(values[idx].as_int())+"|";
            }else{
                key+=values[idx].as_text()+"|";
            }
        }
        if(distinct){
            bool found=false;
            for(const auto &s:seen){
                if(s==key){
                    found=true;
                    break;
                }
            }
            if(found)continue;
            seen.push_back(key);
        }
        cout<<"(";
        for(size_t j=0;j<column_indexes.size();j++){
            int idx=column_indexes[j];
            if(values[idx].is_null()){
                cout<<"NULL";
            }else if(values[idx].get_type()==DataType::INT){
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

void Table::aggregate(const vector<vector<Value>>&rows,const Statement &statement){
    const auto &cols=schema.get_columns();

    vector<AggregateType> types=statement.aggregate_types;
    vector<string> agg_cols=statement.aggregate_columns;
    if(types.empty()){
        types.push_back(statement.aggregate_type);
        agg_cols.push_back(statement.aggregate_column);
    }

    bool first=true;
    for(size_t ai=0;ai<types.size();ai++){
        AggregateType atype=types[ai];
        string acol=agg_cols[ai];

        if(!first) cout<<"  ";
        first=false;

        if(atype==AggregateType::COUNT){
            cout<<"COUNT: "<<rows.size();
            continue;
        }

        int col_index=-1;
        for(size_t i=0;i<cols.size();i++){
            if(cols[i].name==acol){col_index=(int)i;break;}
        }
        if(col_index==-1){
            cout<<"Error: Column '"<<acol<<"' does not exist.";
            continue;
        }

        int sum=0,count=0,min_val=INT_MAX,max_val=INT_MIN;
        for(const auto &row:rows){
            int v=row[col_index].as_int();
            sum+=v; count++;
            if(v<min_val)min_val=v;
            if(v>max_val)max_val=v;
        }

        if(atype==AggregateType::SUM)       cout<<"SUM: "<<sum;
        else if(atype==AggregateType::AVG)  cout<<"AVG: "<<(count==0?0:sum/count);
        else if(atype==AggregateType::MIN)  cout<<"MIN: "<<(count==0?0:min_val);
        else if(atype==AggregateType::MAX)  cout<<"MAX: "<<(count==0?0:max_val);
    }
    cout<<"\n";
}

void Table::group_by_aggregate(const vector<vector<Value>>&rows,const Statement &statement){
    const auto &cols=schema.get_columns();

    int group_index=-1;
    int agg_index=-1;

    for(size_t i=0;i<cols.size();i++){
        if(cols[i].name==statement.group_by_column){
            group_index=i;
        }
        if(cols[i].name==statement.aggregate_column){
            agg_index=i;
        }
    }

    if(group_index==-1){
        cout<<"Error: GROUP BY column not found.\n";
        return;
    }

    if(statement.aggregate_type!=AggregateType::COUNT&&agg_index==-1){
        cout<<"Error: AGGREGATE column not found.\n";
        return;
    }

    map<string,tuple<int,int,int,int>>groups;
    for(const auto &row:rows){
        string key=(cols[group_index].type==DataType::INT)?to_string(row[group_index].as_int()):row[group_index].as_text();
        auto &[cnt,sum,mn,mx]=groups[key];
        cnt++;
        if(agg_index!=-1){
            int v=row[agg_index].as_int();
            sum+=v;
            if(cnt==1){
                mn=v;
                mx=v;
            }else{
                mn=min(mn,v);
                mx=max(mx,v);
            }
        }
    }
    
    for(auto &[key,tup]:groups){
        auto &[cnt,sum,mn,mx]=tup;
        int result=0;
        if(statement.aggregate_type==AggregateType::COUNT) result=cnt;
        else if(statement.aggregate_type==AggregateType::SUM) result=sum;
        else if(statement.aggregate_type==AggregateType::AVG) result=(cnt?sum/cnt:0);
        else if(statement.aggregate_type==AggregateType::MIN) result=mn;
        else if(statement.aggregate_type==AggregateType::MAX) result=mx;
        cout<<"("<<key<<", "<<result<<")\n";
    }
}

vector<vector<Value>>Table::inner_join(Table *other,const string &col1,const string &col2){
    vector<vector<Value>>result;

    const auto &cols1=schema.get_columns();
    const auto &cols2=other->schema.get_columns();

    int idx1=-1,idx2=-1;

    for(size_t i=0;i<cols1.size();i++){
        if(cols1[i].name==col1){
            idx1=i;
            break;
        }
    }

    for(size_t i=0;i<cols2.size();i++){
        if(cols2[i].name==col2){
            idx2=i;
            break;
        }
    }

    if(idx1==-1||idx2==-1){
        cout<<"Error: JOIN column not found.\n";
        return result;
    }

    vector<vector<Value>>rows1=get_all_rows();
    vector<vector<Value>>rows2=other->get_all_rows();

    for(const auto &r1:rows1){
        for(const auto &r2:rows2){
            bool match=false;

            if(r1[idx1].get_type()==DataType::INT){
                match=(r1[idx1].as_int()==r2[idx2].as_int());
            }else{
                match=(r1[idx1].as_text()==r2[idx2].as_text());
            }

            if(match){
                vector<Value>combined=r1;
                combined.insert(combined.end(),r2.begin(),r2.end());
                result.push_back(combined);
            }
        }
    }
    return result;
}

vector<vector<Value>>Table::left_join(Table *other,const string &col1,const string &col2,const Schema &other_schema){
    vector<vector<Value>>result;

    const auto &cols1=schema.get_columns();
    const auto &cols2=other_schema.get_columns();

    int idx1=-1,idx2=-1;

    for(size_t i=0;i<cols1.size();i++){
        if(cols1[i].name==col1){
            idx1=i;
            break;
        }
    }

    for(size_t i=0;i<cols2.size();i++){
        if(cols2[i].name==col2){
            idx2=i;
            break;
        }
    }

    if(idx1==-1||idx2==-1){
        cout<<"Error: JOIN column not found.\n";
        return result;
    }

    vector<Value>null_rows;
    for(size_t i=0;i<cols2.size();i++){
        null_rows.push_back(Value::make_null());
    }

    vector<vector<Value>>rows1=get_all_rows();
    vector<vector<Value>>rows2=other->get_all_rows();

    for(const auto &r1:rows1){
        bool matched=false;

        for(const auto &r2:rows2){
            bool match=false;
            if(r1[idx1].get_type()==DataType::INT){
                 match=(r1[idx1].as_int()==r2[idx2].as_int());
            }else{
                match=(r1[idx1].as_text()==r2[idx2].as_text());
            }

            if(match){
                vector<Value>combined=r1;
                combined.insert(combined.end(),r2.begin(),r2.end());
                result.push_back(combined);
                matched=true;
            }
        }
        if(!matched){
            vector<Value>combined=r1;
            combined.insert(combined.end(),null_rows.begin(),null_rows.end());
            result.push_back(combined);
        }
    }
    return result;
}

vector<vector<Value>>Table::right_join(Table *other,const string &col1,const string &col2,const Schema &other_schema){
    vector<vector<Value>>result;

    const auto &cols1=schema.get_columns();
    const auto &cols2=other_schema.get_columns();

    int idx1=-1,idx2=-1;

    for(size_t i=0;i<cols1.size();i++){
        if(cols1[i].name==col1){
            idx1=i;
            break;
        }
    }

    for(size_t i=0;i<cols2.size();i++){
        if(cols2[i].name==col2){
            idx2=i;
            break;
        }
    }

    if(idx1==-1||idx2==-1){
        cout<<"Error: JOIN column not found.\n";
        return result;
    }

    vector<Value>null_row;
    for(size_t i=0;i<cols1.size();i++){
        null_row.push_back(Value::make_null());
    }

    vector<vector<Value>>rows1=get_all_rows();
    vector<vector<Value>>rows2=other->get_all_rows();

    for(const auto &r2:rows2){
        bool matched=false;
        for(const auto &r1:rows1){
            bool match=false;
            if(r1[idx1].get_type()==DataType::INT){
                match=(r1[idx1].as_int()==r2[idx2].as_int());
            }else{
                match=(r1[idx1].as_text()==r2[idx2].as_text());
            }

            if(match){
                vector<Value>combined=r1;
                combined.insert(combined.end(),r2.begin(),r2.end());
                result.push_back(combined);
                matched=true;
            }
        }
        if(!matched){
            vector<Value>combined=null_row;
            combined.insert(combined.end(),r2.begin(),r2.end());
            result.push_back(combined);
        }
    }
    return result;
}

vector<vector<Value>>Table::full_outer_join(Table *other,const string &col1,const string &col2,const Schema &other_schema){
    vector<vector<Value>>result;

    const auto &cols1=schema.get_columns();
    const auto &cols2=other_schema.get_columns();

    int idx1=-1,idx2=-1;

    for(size_t i=0;i<cols1.size();i++){
        if(cols1[i].name==col1){
            idx1=i;
            break;
        }
    }

    for(size_t i=0;i<cols2.size();i++){
        if(cols2[i].name==col2){
            idx2=i;
            break;
        }
    }

    if(idx1==-1||idx2==-1){
        cout<<"Error: JOIN column not found.\n";
        return result;
    }

    vector<Value>null_left;
    
    for(size_t i=0;i<cols1.size();i++){
        null_left.push_back(Value::make_null());
    }

    vector<Value>null_right;
    for(size_t i=0;i<cols2.size();i++){
        null_right.push_back(Value::make_null());
    }

    vector<vector<Value>>rows1=get_all_rows();
    vector<vector<Value>>rows2=other->get_all_rows();

    vector<bool>right_matched(rows2.size(),false);

    for(const auto &r1:rows1){
        bool matched=false;
        for(size_t j=0;j<rows2.size();j++){
            const auto &r2=rows2[j];
            bool match=false;
            if(r1[idx1].get_type()==DataType::INT){
                match=(r1[idx1].as_int()==r2[idx2].as_int());
            }else{
                match=(r1[idx1].as_text()==r2[idx2].as_text());
            }

            if(match){
                vector<Value>combined=r1;
                combined.insert(combined.end(),r2.begin(),r2.end());
                result.push_back(combined);
                matched=true;
                right_matched[j]=true;
            }
        }
        if(!matched){
            vector<Value>combined=r1;
            combined.insert(combined.end(),null_right.begin(),null_right.end());
            result.push_back(combined);
        }
    }
    for(size_t j=0;j<rows2.size();j++){
        if(!right_matched[j]){
            vector<Value>combined=null_left;
            combined.insert(combined.end(),rows2[j].begin(),rows2[j].end());
            result.push_back(combined);
        }
    }
    return result;
}

vector<vector<Value>>Table::filter_joined_rows(const vector<vector<Value>>&rows,const Schema &combined_schema,const Statement &statement){
    vector<vector<Value>>result;
    const auto &cols=combined_schema.get_columns();

    int idx1=-1;
    for(size_t i=0;i<cols.size();i++){
        if(cols[i].name==statement.where_column){
            idx1=i;
            break;
        }
    }
    if(idx1==-1){
        cout<<"Error: Column '"<<statement.where_column<<"' not found.\n";
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
            cout<<"Error: Column '"<<statement.where_column2<<"' not found.\n";
            return result;
        }
    }
    for(const auto &row:rows){
        auto eval=[&](int idx,const string &op,const string &val)->bool{
            const Value &cell=row[idx];
            if(cell.get_type()==DataType::INT){
                int left=cell.as_int();
                int right;
                try
                {
                   right=stoi(val); 
                }
                catch(...)
                {
                    return false;
                }
                if(op=="=")return left==right;
                if(op==">")return left>right;
                if(op=="<")return left<right;
                if(op==">=")return left>=right;
                if(op=="<=")return left<=right;
                if(op=="!=")return left!=right; 
            }else{
                if(op=="=")return cell.as_text()==val;
                if(op=="!=")return cell.as_text()!=val;
            }
            return false;
        };

        bool match1=eval(idx1,statement.where_operator,statement.where_value);
        bool match2=true;
        if(statement.has_second_condition){
            match2=eval(idx2,statement.where_operator2,statement.where_value2);
            if(statement.logical_operator=="AND"){
                if(!(match1&&match2))continue;
            }else{
                if(!(match1||match2))continue;
            }
        }else{
            if(!match1)continue;
        }
        result.push_back(row);
    }
    return result;
}

void Table::sort_joined_rows(vector<vector<Value>>&rows,const Schema &combined_schema,const string &order_by_column,bool descending)const{
    const auto &cols=combined_schema.get_columns();
    int idx=-1;
    for(size_t i=0;i<cols.size();i++){
        if(cols[i].name==order_by_column){
            idx=i;
            break;
        }
    }
    if(idx==-1){
        cout<<"Error: ORDER BY column not found.\n";
        return;
    }

    sort(rows.begin(),rows.end(),[&](const vector<Value>&a,const vector<Value>&b){
        bool res=(a[idx].get_type()==DataType::INT)?(a[idx].as_int()<b[idx].as_int()):(a[idx].as_text()<b[idx].as_text());
        return descending?!res:res;
    });
}

vector<Value>Table::get_row(uint32_t row_num){
    if(row_num>=num_rows)return {};
    uint32_t row_size=compute_row_size();
    uint32_t page_num,page_offset;
    row_slot(row_num,row_size,page_num,page_offset);
    void *page=pager.get_page(page_num);
    void *source=(char*)page+page_offset;
    vector<Value>values;
    deserialize_row(source,values);
    return values;
}

void Table::restore_rows(const vector<vector<Value>>&rows){
    num_rows=0;
    void *first_page=pager.get_page(0);
    memset(first_page,0,PAGE_SIZE);
    memcpy(first_page,&num_rows,HEADER_SIZE);

    uint32_t row_size=compute_row_size();

    for(const auto &row:rows){
        uint32_t page_num,page_offset;
        row_slot(num_rows,row_size,page_num,page_offset);

        void *page=pager.get_page(page_num);
        void *dest=(char*)page+page_offset;

        serialize_row(row,dest);
        num_rows++;
    }

    first_page=pager.get_page(0);
    memcpy(first_page,&num_rows,HEADER_SIZE);
}
