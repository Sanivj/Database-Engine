cmake_minimum_required(VERSION 3.10)
project(miniDB)

set(CMAKE_CXX_STANDARD 17)

include_directories(include)

add_executable(miniDB
    src/main.cpp
    src/serializer.cpp
    src/statement.cpp
    src/pager.cpp
    src/table.cpp
    src/schema.cpp
    src/database.cpp
    src/value.cpp
    src/btree.cpp
    src/index.cpp
)