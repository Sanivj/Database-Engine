#include "index.h"
#include <algorithm>

HashIndex::HashIndex(const string &table_name,const string &column_name):table_name(table_name),column_name(column_name){}

void HashIndex::insert(const string &key,uint32_t rows_num){
    buckets[key].push_back(rows_num);
}

void HashIndex::remove(const string &key,uint32_t row_num){
    auto it=buckets.find(key);
    if(it==buckets.end())return;
    auto &vec=it->second;
    vec.erase(std::remove(vec.begin(),vec.end(),row_num),vec.end());
    if(vec.empty())buckets.erase(it);
}

void HashIndex::clear(){
    buckets.clear();
}

vector<uint32_t>HashIndex::lookup(const string &key)const{
    auto it=buckets.find(key);
    if(it==buckets.end())return{};
    return it->second;
}

bool HashIndex::has_key(const string &key)const{
    return buckets.find(key)!=buckets.end();
}

const string &HashIndex::get_table_name()const{
    return table_name;
}

const string &HashIndex::get_column_name()const{
    return column_name;
}