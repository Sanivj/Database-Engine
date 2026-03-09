#include "table.h"
#include <iostream>
#include <cstring>

static const uint32_t HEADER_SIZE=sizeof(uint32_t);
static const uint32_t FIXED_TEXT_SIZE=256;

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

void Table::select_where(const string &column,const string &value){
    string clean_column=column;
    string clean_value=value;

    if(!clean_column.empty()&&clean_column.back()==';'){
        clean_column.pop_back();
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
            int compare_value=stoi(value);
            match=(cell.as_int()==compare_value);
        }else{
            match=(cell.as_text()==value);
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
