#include "table.h"
#include "serializer.h"
#include <iostream>
#include <cstring>
using namespace std;

const uint32_t ROW_SIZE=sizeof(Row);
const uint32_t ROWS_PER_PAGE=PAGE_SIZE/ROW_SIZE;

Table::Table(const string &filename):pager(filename){
    if(pager.file_length>=sizeof(uint32_t)){
        void *page=pager.get_page(0);
        memcpy(&num_rows,page,sizeof(uint32_t));
    }else{
        num_rows=0;
    }
}

Table::~Table(){}

void Table::insert_row(const Row &row){
    if(id_exists(row.id)){
        cout<<"Error:Duplicate ID not allowed/\n";
        return;
    }
    uint32_t page_num=(num_rows*ROW_SIZE+sizeof(uint32_t))/PAGE_SIZE;
    void *page=pager.get_page(page_num);

    uint32_t row_offset=(num_rows*ROW_SIZE+sizeof(uint32_t))%PAGE_SIZE;
    void *destination=(char*)page+row_offset;

    serialize_row(&row,destination);

    num_rows++;

    void *first_page=pager.get_page(0);
    memcpy(first_page,&num_rows,sizeof(uint32_t));
    cout<<"Executed\n";
}

void Table::select_all(){
    for(uint32_t i=0;i<num_rows;i++){
        uint32_t offset=i*ROW_SIZE+sizeof(uint32_t);
        uint32_t page_num=offset/PAGE_SIZE;
        uint32_t page_offset=offset%PAGE_SIZE;
        void *page=pager.get_page(page_num);
        void *source= (char*)page+page_offset;

        Row row;
        deserialize_row(source, &row);

        cout<<"("<<row.id<<", "<<row.username<<", "<<row.email<<")\n";
    }
}

void Table::select_by_id(uint32_t id){
    for(uint32_t i=0;i<num_rows;i++){
        uint32_t offset=i*ROW_SIZE+sizeof(uint32_t);
        uint32_t page_num=offset/PAGE_SIZE;
        uint32_t page_offset=offset%PAGE_SIZE;

        void *page=pager.get_page(page_num);
        void *source=(char*)page+page_offset;

        Row row;
        deserialize_row(source,&row);

        if(row.id==id){
            cout<<"("<<row.id<<", "<<row.username<<", "<<row.email<<")\n";
            return;
        }
    }
    cout<<"Row not found\n";
}

bool Table::id_exists(uint32_t id){
    for(uint32_t i=0;i<num_rows;i++){
        uint32_t offset=i*ROW_SIZE+sizeof(uint32_t);
        uint32_t page_num=offset/PAGE_SIZE;
        uint32_t page_offset=offset%PAGE_SIZE;

        void *page=pager.get_page(page_num);
        void *source=(char*)page+page_offset;

        Row row;
        deserialize_row(source,&row);

        if(row.id==id){
            return true;
        }
    }
    return false;
}