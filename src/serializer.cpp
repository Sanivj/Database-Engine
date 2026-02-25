#include "serializer.h"
#include <cstring>

const uint32_t ID_OFFSET=0;
const uint32_t USERNAME_OFFSET=ID_OFFSET+sizeof(uint32_t);
const uint32_t EMAIL_OFFSET=USERNAME_OFFSET+COLUMN_USERNAME_SIZE;

void serialize_row(const Row *source,void *destination){
    memcpy((char*)destination+ID_OFFSET,&(source->id),sizeof(source->id));
    memcpy((char*)destination+USERNAME_OFFSET,&(source->username),COLUMN_USERNAME_SIZE);
    memcpy((char*)destination+EMAIL_OFFSET,&(source->email),COLUMN_EMAIL_SIZE);
}

void deserialize_row(const void* source,Row *destination){
    memcpy(&(destination->id),(char*)source+ID_OFFSET,sizeof(destination->id));
    memcpy(&(destination->username),(char*)source+USERNAME_OFFSET,COLUMN_USERNAME_SIZE);
    memcpy(&(destination->email),(char*)source+EMAIL_OFFSET,COLUMN_EMAIL_SIZE);
}