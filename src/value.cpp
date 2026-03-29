#include "value.h"

Value Value::make_null(){
    Value v;
    v.null_flag=true;
    v.type=DataType::INT;
    v.int_value=0;
    return v;
}

bool Value::is_null()const{
    return null_flag;
}

Value Value::from_int(int v){
    Value val;
    val.null_flag=false;
    val.type=DataType::INT;
    val.int_value=v;
    return val;
}

Value Value::from_text(const string &v){
    Value val;
    val.null_flag=false;
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