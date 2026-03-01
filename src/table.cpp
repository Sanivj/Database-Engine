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

    cout<<"Executed\n";
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