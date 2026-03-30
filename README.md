# MiniDB 🗄️

A lightweight, SQLite-inspired relational database engine built from scratch in C++. MiniDB implements core database internals — custom storage engine, SQL parser, query optimizer, hash indexing, and transaction support — without any external database libraries.

---

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Getting Started](#getting-started)
- [SQL Reference](#sql-reference)
- [Meta Commands](#meta-commands)
- [Design Decisions](#design-decisions)
- [Project Structure](#project-structure)
- [Roadmap](#roadmap)

---

## Overview

MiniDB was built as a systems engineering project to deeply understand how relational databases work internally. Every layer is implemented from scratch:

- A custom pager that manages fixed-size pages on disk  
- A binary serializer that encodes rows with typed columns and NULL flags  
- A SQL parser that tokenizes and builds structured statement objects  
- A query executor that evaluates WHERE, JOIN, GROUP BY, ORDER BY, and aggregates  
- A hash index for O(1) equality lookups  
- A snapshot-based transaction engine for BEGIN / COMMIT / ROLLBACK  
- A system catalog (`sys_tables`, `sys_columns`) that persists schema across restarts  

---

## Features

### ✅ Core SQL

| Feature | Example |
|--------|--------|
| CREATE TABLE | `CREATE TABLE students (id INT PRIMARY KEY, name TEXT, age INT);` |
| DROP TABLE | `DROP TABLE students;` |
| INSERT | `INSERT INTO students VALUES (1, 'Alice', 20);` |
| SELECT * | `SELECT * FROM students;` |
| SELECT columns | `SELECT name, age FROM students;` |
| WHERE | `SELECT * FROM students WHERE age > 20;` |
| AND / OR | `SELECT * FROM students WHERE age > 20 AND grade = 'A';` |
| UPDATE | `UPDATE students SET grade = 'A+' WHERE id = 1;` |
| DELETE | `DELETE FROM students WHERE grade = 'C';` |

---

### ✅ Query Clauses

| Feature | Example |
|--------|--------|
| ORDER BY ASC/DESC | `SELECT * FROM students ORDER BY age DESC;` |
| LIMIT | `SELECT * FROM students LIMIT 5;` |
| OFFSET | `SELECT * FROM students LIMIT 5 OFFSET 10;` |
| DISTINCT | `SELECT DISTINCT grade FROM students;` |
| Column Alias | `SELECT name AS student_name FROM students;` |
| Table Alias | `SELECT s.name FROM students s;` |

---

### ✅ Aggregations

| Feature | Example |
|--------|--------|
| COUNT | `SELECT COUNT(*) FROM students;` |
| SUM | `SELECT SUM(age) FROM students;` |
| AVG | `SELECT AVG(age) FROM students;` |
| MIN / MAX | `SELECT MIN(age), MAX(age) FROM students;` |
| GROUP BY | `SELECT grade, COUNT(*) FROM students GROUP BY grade;` |

---

### ✅ Joins

| Feature | Example |
|--------|--------|
| INNER JOIN | `SELECT * FROM orders JOIN customers ON orders.cid = customers.id;` |
| LEFT JOIN | `SELECT * FROM students LEFT JOIN enrollments ON students.id = enrollments.sid;` |
| RIGHT JOIN | `SELECT * FROM students RIGHT JOIN enrollments ON students.id = enrollments.sid;` |
| FULL OUTER JOIN | `SELECT * FROM students FULL OUTER JOIN enrollments ON students.id = enrollments.sid;` |
| JOIN + WHERE | `SELECT * FROM orders JOIN customers ON orders.cid = customers.id WHERE customers.city = 'Delhi';` |
| JOIN + ORDER BY | `SELECT * FROM orders JOIN customers ON orders.cid = customers.id ORDER BY amount DESC;` |

---

### ✅ NULL Handling

```sql
INSERT INTO students VALUES (1, 'Alice', NULL, 'A');
SELECT * FROM students WHERE grade IS NOT NULL;
```

NULLs are stored with a 1-byte flag per column on disk and displayed as `NULL` in output.

---

### ✅ Indexing

```sql
CREATE INDEX idx_id ON students(id);
DROP INDEX idx_id;
.indexes
```

Hash index built over existing rows. Automatically used for equality WHERE clauses (`=`). Falls back to full scan for range queries.

---

### ✅ Transactions

```sql
BEGIN;
INSERT INTO students VALUES (9, 'Zara', 19, 'A');
UPDATE students SET grade = 'F' WHERE id = 1;
ROLLBACK;

BEGIN;
INSERT INTO students VALUES (9, 'Zara', 19, 'A');
COMMIT;
```

---

### ✅ Query Optimization

- Predicate pushdown — WHERE filtering happens inside the scan loop  
- LIMIT early exit — scan stops as soon as enough rows are found  
- Index scan — equality WHERE uses O(1) lookup  

---

### ✅ Persistence

- Binary `.db` files inside `data/`  
- Schema catalog reloaded on startup  
- Page-level write-through flushing  

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                        CLI Layer                         │
└────────────────────────┬────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────┐
│                     SQL Parser                           │
└────────────────────────┬────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────┐
│                  Query Executor                          │
└──────┬───────────┬──────────────┬────────────┬──────────┘
       │           │              │            │
   Table       Database        Index      Transaction
       │
   Pager → Disk
```

---

## Storage Format

- Page size: 4096 bytes  
- Page 0 → metadata  
- Pages 1+ → row data  

Row format:

```
[1 byte null_flag][data]
```

- INT → 4 bytes  
- TEXT → 256 bytes  

---

## Getting Started

### Prerequisites

- g++ with C++17 support  
- CMake 3.10+  

### Build

```bash
git clone https://github.com/yourusername/Database-Engine.git
cd Database-Engine
cmake -S . -B build
cmake --build build
```

### Run

```bash
./build/miniDB
```

> Always run from the project root.

---

## SQL Reference

### Data Types

| Type | Size | Notes |
|------|------|------|
| INT | 4 bytes | Signed |
| TEXT | 256 bytes | Fixed |

---

### Operators

| Operator | Example |
|----------|--------|
| = | `WHERE id = 5` |
| != | `WHERE grade != 'F'` |
| > < >= <= | `WHERE age >= 18` |
| AND | `age > 20 AND grade = 'A'` |
| OR | `grade = 'A' OR grade = 'B'` |

---

### Full SELECT Syntax

```sql
SELECT ...
FROM ...
JOIN ...
WHERE ...
GROUP BY ...
ORDER BY ...
LIMIT ...
OFFSET ...
```

---

## Meta Commands

| Command | Description |
|--------|------------|
| .help | Show commands |
| .tables | List tables |
| .schema | Show schema |
| .indexes | Show indexes |
| .exit | Exit |

---

## Design Decisions

- Fixed-size TEXT simplifies storage  
- Hash index enables fast lookup  
- Snapshot transactions simplify rollback  
- System catalog persists schema  
- Dedicated header page avoids memory bugs  

---

## Project Structure

```
Database-Engine/
├── include/
├── src/
├── data/
├── CMakeLists.txt
└── README.md
```

---

## Roadmap

### Completed

- Core SQL (CRUD)  
- WHERE, ORDER BY, LIMIT  
- Aggregations  
- Joins  
- Indexing  
- Transactions  
- Optimization  
- Persistence  

### Future Work

- B-Tree index  
- WAL (Write-Ahead Logging)  
- Variable-length TEXT  
- Subqueries  
- Foreign keys  
- ALTER TABLE  
- CSV export  
- EXPLAIN queries  

---

## License

MIT License