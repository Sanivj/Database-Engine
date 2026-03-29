#ifndef VALUE_H
#define VALUE_H

#include <string>
#include "schema.h"
using namespace std;
class Value{
    public:
        Value()=default;

        static Value from_int(int v);
        static Value from_text(const string &v);
        static Value make_null();

        bool is_null() const;
        DataType get_type() const;

        int as_int() const;
        const string &as_text() const;
    private:
        DataType type;
        int int_value;
        string text_value;
        bool null_flag=false;
};

#endif