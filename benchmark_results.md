# Benchmark Results — pySimpleDB Query Optimization & Indexing

**Dataset:** 100 Students, 50 Instructors, 20 Courses, 300 Sections, 500 Enrollments
**Block size:** 8192 bytes | **Buffer pool:** 1000 buffers | **Random seed:** 42

---

## Results

### Q1

```sql
select s_id, s_name from Student, Enrollment, Section, Course
where s_id = e_student_id and e_section_id = sec_id
  and sec_course_id = c_id and c_department = 'CS'
```

| Mode     | Rows | Time (s)  | Block Accesses |
|----------|------|-----------|----------------|
| baseline | 103  | 1183.3422 | 6,018,099      |
| opt      | 103  | 0.2503    | 353            |
| index    | 103  | 0.0207    | 329            |
| full     | 103  | 0.0078    | 96             |

### Q2

```sql
select s_id, s_name from Student, Enrollment
where s_id = e_student_id and e_grade = 'NC'
```

| Mode     | Rows | Time (s) | Block Accesses |
|----------|------|----------|----------------|
| baseline | 86   | 0.2019   | 1011           |
| opt      | 86   | 0.1958   | 207            |
| index    | 86   | 0.0102   | 158            |
| full     | 86   | 0.0037   | 158            |

### Q3

```sql
select i_id, i_name from Instructor, Section
where i_id = sec_instructor_id and sec_semester = 'Fall' and sec_year = 2024
```

| Mode     | Rows | Time (s) | Block Accesses |
|----------|------|----------|----------------|
| baseline | 25   | 0.1121   | 310            |
| opt      | 25   | 0.0612   | 107            |
| index    | 25   | 0.0032   | 79             |
| full     | 25   | 0.0027   | 79             |

---

## Observations

**Q1 baseline** took 1183 seconds and 6,018,099 block accesses. Without reordering, the executor forms `Student × Enrollment × Section × Course` as written — 300M combinations before any filtering. Full mode reduces this to 96 accesses: a **62,689× improvement**.

**opt vs baseline:** Selection pushdown eliminates rows before they enter the join. `c_department='CS'` reduces Course from 20 → 4 rows (Q1: 6M → 353 accesses). `e_grade='NC'` reduces Enrollment from 500 → 86 rows (Q2: 1011 → 207). `sec_semester/sec_year` reduces Section from 300 → 25 rows (Q3: 310 → 107).

**index vs opt:** `IndexJoinScan` replaces `ProductScan` for field=field joins — one index lookup per outer row instead of a full inner table rescan. Q1: 353 → 329, Q2: 207 → 158, Q3: 107 → 79. Even with the original join order preserved, direct RID seeks still reduce block accesses across all queries.

**full vs index:** Combining reordering with `IndexJoinScan` gives the best result for Q1 (96 accesses) — the plan starts from 4 filtered Course rows and chains index lookups outward. Q2/Q3 are 2-table queries so reordering adds nothing; full = index for those.

---

## Implementation

| Component | Description |
|-----------|-------------|
| `BetterQueryPlanner` | Classifies terms into single-table (pushdown) and join conditions. Greedy join order by `recordsOutput()`. Applies each join predicate immediately after the `ProductPlan`. |
| `BTreeIndex` | In-memory B+ tree (order 50). Leaf nodes store `list[RecordID]` to handle duplicates. |
| `CompositeIndex` | Dict keyed on `tuple(field_values)`. O(1) exact-match lookup on all indexed fields. |
| `IndexScan` | Fetches `list[RecordID]` from index, seeks `TableScan` to each via `moveToRecordID`. |
| `IndexJoinScan` | For each outer row, calls `index.search(outer_field_value)` and seeks the inner `TableScan` to each matching RID. Accepts `inner_pred` for additional selection filters on the inner table. |
| `IndexQueryPlanner` | `index` mode: original join order with index scans substituted where possible. `full` mode: greedy reordering on top of index scans, driven by the `better_planner` parameter. |
| `create_indexes` | Single table scan per table to populate all B-tree and composite indexes. |
