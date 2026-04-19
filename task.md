# **Project: Query Optimization and Indexing in a Lightweight DB Engine**

## **Background**

You will work with [pySimpleDB](https://github.com/CWSwastik/pySimpleDB). This is a lightweight educational database system that supports basic query execution but lacks:

* effective query optimization  
* proper index structures

## **Objective**

Extend the system to improve query performance by implementing:

### 1. Join Order Optimization

* Reorder cartesian products to reduce intermediate results  
* Push selection conditions as early as possible

### 2. B-Tree Indexes

* Implement a B-tree index  
* Support insertion and search  
* Integrate index usage into query execution

### 3. Composite Indexes

* Support multi-attribute indexes

## **Schema**

### Tables

```sql
Student(
  s_id INT PRIMARY KEY,
  s_name VARCHAR(50),
  s_department VARCHAR(30),
  s_year INT
);

Instructor(
  i_id INT PRIMARY KEY,
  i_name VARCHAR(50),
  i_department VARCHAR(30)
);

Course(
  c_id INT PRIMARY KEY,
  c_title VARCHAR(100),
  c_department VARCHAR(30),
  c_credits INT
);

Section(
  sec_id INT PRIMARY KEY,
  sec_course_id INT,
  sec_instructor_id INT,
  sec_semester VARCHAR(10),
  sec_year INT,
  FOREIGN KEY (sec_course_id) REFERENCES Course(c_id),
  FOREIGN KEY (sec_instructor_id) REFERENCES Instructor(i_id)
);

Enrollment(
  e_id INT PRIMARY KEY,
  e_student_id INT,
  e_section_id INT,
  e_grade CHAR(2),
  FOREIGN KEY (e_student_id) REFERENCES Student(s_id),
  FOREIGN KEY (e_section_id) REFERENCES Section(sec_id)
);
```

## **Queries to Optimize**

### **Q1**

```sql
select s_id, s_name
from Student, Enrollment, Section, Course
where s_id = e_student_id
  and e_section_id = sec_id
  and sec_course_id = c_id
  and c_department = 'CS'
```

### **Q2**

```sql
select s_id, s_name
from Student, Enrollment
where s_id = e_student_id
  and e_grade = 'NC';
```

### **Q3**

```sql
select i_id, i_name
from Instructor, Section
where i_id = sec_instructor_id
  and sec_semester = 'Fall'
  and sec_year = 2024;
```

## **Execution Protocol**

Your implementation must run via the interface of pySimpleDB

### **Run Commands**

```bash
python main.py --query Q1 --mode baseline

python main.py --query Q1 --mode opt

python main.py --query Q1 --mode index

python main.py --query Q1 --mode full
```

**baseline**: Runs the query without any optimization or indexing, using the default execution plan. This serves as the reference for correctness and performance comparison.

**opt**: Runs the query with join reordering and selection pushdown enabled, but without using any indexes. This isolates the effect of query optimization alone.

**index**: Runs the query using indexes for selection and joins, but without changing the original join order. This isolates the benefit of indexing alone.

**full**: Runs the query with both query optimization and index usage enabled. This should give the best performance by combining both improvements.

## **Submission**

* All code, data files, and report in a single zip named “Group number XXX”  
* The report should explain the design of the solution, optimizations done, observations made. Include execution timings for each query (Q1, Q2, Q3) under all four modes:  
  * baseline  
  * opt  
  * index  
  * full

### **Evaluation**

Marks will be awarded based on

* Correctness and effectiveness of the implemented solution on the given queries  
  * Follow the execution protocols outlined before  
* Correctness and effectiveness of the implemented solution on hidden test queries (may use different schemas)  
  * **Students are required to modify only `solution.py`**. During evaluation, the instructors may replace or modify `benchmark.py` with different schemas and test queries. Therefore, the implemented solution must be general and should not rely on any hardcoded assumptions specific to the provided example.
