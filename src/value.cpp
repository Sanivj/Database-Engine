#include "value.h"

Value Value::from_int(int v){
    Value val;
    val.type=DataType::INT;
    val.int_value=v;
    return val;
}

Value Value::from_text(const string &v){
    Value val;
    val.type=DataType::TEXT;
    val.text_value=v;
    return val;
}

DataType Value::get_type() const{
    return type;
}

int Value::as_int() const{
    return int_value;
}

const string &Value::as_text() const{
    return text_value;
}