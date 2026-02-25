#ifndef SERIALIZER_H
#define SERIALIZER_H

#include "row.h"

void serialize_row(const Row* source,void *destination);
void deserialize_row(const void*source,Row* destination);

#endif