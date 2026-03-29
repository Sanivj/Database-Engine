#ifndef INDEX_H
#define INDEX_H

#include <string>
#include <vector>
#include <unordered_map>
#include <cstdint>
using namespace std;

class HashIndex{
    public:
        HashIndex()=default;
        HashIndex(const string &table_name,const string &column_name);

        void insert(const string &key,uint32_t row_num);
        void remove(const string &key,uint32_t rows_num);
        void clear();

        vector<uint32_t>lookup(const string &key)const;

        bool has_key(const string &key)const;

        const string &get_table_name()const;
        const string &get_column_name()const;

    private:
        string table_name;
        string column_name;
        unordered_map<string,vector<uint32_t>>buckets;
};
#endif