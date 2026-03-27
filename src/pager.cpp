#include "pager.h"
#include <iostream>
#include <cstring>
using namespace std;

Pager::Pager(const string &filename):pages(TABLE_MAX_PAGES){
    file.open(filename,ios::in|ios::out|ios::binary);

    if(!file.is_open()){
        file.open(filename,ios::out|ios::binary);
        file.close();
        file.open(filename,ios::in|ios::out|ios::binary);
    }

    file.seekg(0,ios::end);
    file_length=file.tellg();
}

Pager::~Pager(){
    for(uint32_t i=0;i<TABLE_MAX_PAGES;i++){
        if(pages[i]){
            flush(i);
        }
    }
    file.close();
}

void *Pager::get_page(uint32_t page_num){
    if(page_num>=TABLE_MAX_PAGES){
        cout<<"Page number out of bounds.\n";
        exit(EXIT_FAILURE);
    }

    if(!pages[page_num]){
        pages[page_num]=make_unique<char[]>(PAGE_SIZE);
        uint32_t num_pages=file_length/PAGE_SIZE;
        if(file_length%PAGE_SIZE){
            num_pages+=1;
        }
        if(page_num<num_pages){
            file.seekg(page_num*PAGE_SIZE);
            file.read(pages[page_num].get(),PAGE_SIZE);
        }
    }
    return pages[page_num].get();
}

void Pager::flush(uint32_t page_num){
    if(!pages[page_num])return;
    file.seekp(page_num*PAGE_SIZE);
    file.write(pages[page_num].get(),PAGE_SIZE);
    file.flush();
}