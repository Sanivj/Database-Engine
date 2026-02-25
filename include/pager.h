#ifndef PAGER_H
#define PAGER_H

#include <fstream>
#include <vector>
#include <memory>

using namespace std;

const uint32_t PAGE_SIZE=4096;
const uint32_t TABLE_MAX_PAGES=100;

class Pager{
    public:
        Pager(const string &filename);
        ~Pager();

        void *get_page(uint32_t page_num);
        void flush(uint32_t page_num);

        uint32_t file_length;
        fstream file;
    
    private:
        vector<unique_ptr<char[]>>pages;
};

#endif