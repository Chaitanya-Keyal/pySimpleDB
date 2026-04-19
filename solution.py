"""
This file contains the implementation for the Query Optimization & Indexing project.
"""

from Planner import ProductPlan, ProjectPlan, SelectPlan, TablePlan
from Record import Schema, TableScan
from RelationalOp import Constant, Predicate


def _term_fields(term):
    fields = set()
    for side in (term.lhs.exp_value, term.rhs.exp_value):
        if isinstance(side, str):
            fields.add(side)
    return fields


def _term_get_constant(term, field_name):
    lhs, rhs = term.lhs.exp_value, term.rhs.exp_value
    if isinstance(lhs, str) and lhs == field_name and isinstance(rhs, Constant):
        return rhs.const_value
    if isinstance(rhs, str) and rhs == field_name and isinstance(lhs, Constant):
        return lhs.const_value
    return None


def _schema_fields(plan):
    return set(plan.plan_schema().field_info.keys())


def _make_predicate(terms):
    pred = Predicate()
    pred.terms = list(terms)
    return pred


def _classify_terms(tables, table_plans, all_terms):
    """Split terms into {table: [single-table terms]} and [join terms]."""
    single_terms = {t: [] for t in tables}
    join_terms = []
    for term in all_terms:
        tf = _term_fields(term)
        owners = [t for t in tables if tf.issubset(_schema_fields(table_plans[t]))]
        if len(owners) == 1:
            single_terms[owners[0]].append(term)
        else:
            join_terms.append(term)
    return single_terms, join_terms


def _pick_next_table(to_join, base_plans, join_terms, current_fields, used):
    """
    Greedily pick the next table to join: prefer one with a connecting join
    condition, otherwise fall back to the smallest remaining table.
    Returns (best_t, applicable_join_term_indices).
    """
    for t in to_join:
        t_fields = _schema_fields(base_plans[t])
        idxs = [
            i
            for i, jt in enumerate(join_terms)
            if i not in used
            and _term_fields(jt) & t_fields
            and _term_fields(jt) & current_fields
        ]
        if idxs:
            return t, idxs
    to_join.sort(key=lambda t: base_plans[t].recordsOutput())
    return to_join[0], []


def _find_constant_index(indexes, table_name, terms):
    """
    Returns (index, key_value) if a term matches an indexed field on table_name
    with a constant value, else (None, None). Composite indexes checked first.
    """
    tbl_idx = indexes.get(table_name, {})
    for field_key, idx in tbl_idx.items():
        if not isinstance(field_key, tuple):
            continue
        keys, ok = [], True
        for fn in field_key:
            v = next(
                (
                    _term_get_constant(t, fn)
                    for t in terms
                    if _term_get_constant(t, fn) is not None
                ),
                None,
            )
            if v is None:
                ok = False
                break
            keys.append(v)
        if ok:
            return idx, keys
    for term in terms:
        tf = _term_fields(term)
        for field_name, idx in tbl_idx.items():
            if isinstance(field_name, str) and field_name in tf:
                key = _term_get_constant(term, field_name)
                if key is not None:
                    return idx, key
    return None, None


def _find_join_index(indexes, table_name, outer_fields, join_terms):
    """
    Returns (index, outer_field, term_index) if a join term connects an indexed
    field of table_name to a field already in outer_fields, else (None, None, -1).
    """
    tbl_idx = indexes.get(table_name, {})
    for i, term in enumerate(join_terms):
        lhs, rhs = term.lhs.exp_value, term.rhs.exp_value
        if not (isinstance(lhs, str) and isinstance(rhs, str)):
            continue
        for field_name, idx in tbl_idx.items():
            if not isinstance(field_name, str):
                continue
            if lhs == field_name and rhs in outer_fields:
                return idx, rhs, i
            if rhs == field_name and lhs in outer_fields:
                return idx, lhs, i
    return None, None, -1


class BetterQueryPlanner:
    """
    Optimized query planner with selection pushdown and join reordering.
    """

    def __init__(self, mm):
        self.mm = mm

    def createPlan(self, tx, query_data):
        tables = query_data["tables"]
        table_plans = {t: TablePlan(tx, t, self.mm) for t in tables}
        single_terms, join_terms = _classify_terms(
            tables, table_plans, query_data["predicate"].terms
        )
        pushed_plans = {
            t: (
                SelectPlan(table_plans[t], _make_predicate(single_terms[t]))
                if single_terms[t]
                else table_plans[t]
            )
            for t in tables
        }

        order = sorted(tables, key=lambda t: table_plans[t].recordsOutput())
        current_plan = pushed_plans[order[0]]
        current_fields = _schema_fields(table_plans[order[0]])
        to_join = list(order[1:])
        used = set()

        while to_join:
            best_t, best_idxs = _pick_next_table(
                to_join, table_plans, join_terms, current_fields, used
            )
            current_plan = ProductPlan(current_plan, pushed_plans[best_t])
            current_fields |= _schema_fields(table_plans[best_t])
            if best_idxs:
                current_plan = SelectPlan(
                    current_plan, _make_predicate([join_terms[i] for i in best_idxs])
                )
                used.update(best_idxs)
            to_join.remove(best_t)

        leftover = [join_terms[i] for i in range(len(join_terms)) if i not in used]
        if leftover:
            current_plan = SelectPlan(current_plan, _make_predicate(leftover))
        return ProjectPlan(current_plan, *query_data["fields"])


class _BTreeNode:
    ORDER = 50

    def __init__(self, is_leaf=True):
        self.is_leaf = is_leaf
        self.keys = []
        self.values = []
        self.next = None


class BTreeIndex:
    def __init__(self, tx, index_name, key_type, key_length):
        self.index_name = index_name
        self.key_type = key_type
        self.key_length = key_length
        self.root = _BTreeNode(is_leaf=True)

    def insert(self, key_value, record_id):
        result = self._ins(self.root, key_value, record_id)
        if result:
            mid_key, right = result
            new_root = _BTreeNode(is_leaf=False)
            new_root.keys = [mid_key]
            new_root.values = [self.root, right]
            self.root = new_root

    def search(self, key_value):
        node = self.root
        while not node.is_leaf:
            node = node.values[self._child_idx(node, key_value)]
        for i, k in enumerate(node.keys):
            if k == key_value:
                return list(node.values[i])
        return []

    def close(self):
        pass

    def _ins(self, node, key, rid):
        if node.is_leaf:
            self._leaf_insert(node, key, rid)
            if len(node.keys) >= _BTreeNode.ORDER:
                return self._split_leaf(node)
            return None
        idx = self._child_idx(node, key)
        result = self._ins(node.values[idx], key, rid)
        if result:
            mid_key, right = result
            pos = 0
            while pos < len(node.keys) and node.keys[pos] < mid_key:
                pos += 1
            node.keys.insert(pos, mid_key)
            node.values.insert(pos + 1, right)
            if len(node.keys) >= _BTreeNode.ORDER:
                return self._split_internal(node)
        return None

    def _leaf_insert(self, node, key, rid):
        for i, k in enumerate(node.keys):
            if k == key:
                node.values[i].append(rid)
                return
            if k > key:
                node.keys.insert(i, key)
                node.values.insert(i, [rid])
                return
        node.keys.append(key)
        node.values.append([rid])

    def _split_leaf(self, node):
        mid = len(node.keys) // 2
        right = _BTreeNode(is_leaf=True)
        right.keys = node.keys[mid:]
        right.values = node.values[mid:]
        right.next = node.next
        node.keys = node.keys[:mid]
        node.values = node.values[:mid]
        node.next = right
        return right.keys[0], right

    def _split_internal(self, node):
        mid = len(node.keys) // 2
        mid_key = node.keys[mid]
        right = _BTreeNode(is_leaf=False)
        right.keys = node.keys[mid + 1 :]
        right.values = node.values[mid + 1 :]
        node.keys = node.keys[:mid]
        node.values = node.values[: mid + 1]
        return mid_key, right

    def _child_idx(self, node, key):
        for i, k in enumerate(node.keys):
            if key < k:
                return i
        return len(node.keys)


class CompositeIndex:
    def __init__(self, tx, index_name, field_names, field_types, field_lengths):
        self.index_name = index_name
        self.field_names = list(field_names)
        self.field_types = list(field_types)
        self.field_lengths = list(field_lengths)
        self._data = {}

    def insert(self, field_values, record_id):
        key = tuple(field_values)
        if key not in self._data:
            self._data[key] = []
        self._data[key].append(record_id)

    def search(self, field_values):
        return list(self._data.get(tuple(field_values), []))

    def close(self):
        pass


class IndexScan:
    """
    A scan that uses an index instead of scanning all table records.
    Retrieves RecordIDs from the index using search_key, then iterates
    through them and positions table_scan at each RecordID.
    """

    def __init__(self, table_scan, index, search_key):
        self.table_scan = table_scan
        self.index = index
        self.search_key = search_key
        self._rids = index.search(
            list(search_key) if isinstance(search_key, (list, tuple)) else search_key
        )
        self._pos = 0

    def beforeFirst(self):
        self._pos = 0

    def nextRecord(self):
        while self._pos < len(self._rids):
            rid = self._rids[self._pos]
            self._pos += 1
            self.table_scan.moveToRecordID(rid)
            return True
        return False

    def getInt(self, field_name):
        return self.table_scan.getInt(field_name)

    def getString(self, field_name):
        return self.table_scan.getString(field_name)

    def getVal(self, field_name):
        return self.table_scan.getVal(field_name)

    def hasField(self, field_name):
        return self.table_scan.hasField(field_name)

    def closeRecordPage(self):
        self.table_scan.closeRecordPage()


class IndexJoinScan:
    """
    An index nested-loop join scan. For each row in the outer scan, uses an
    index on the inner table to retrieve only the matching rows by join key,
    avoiding a full rescan of the inner table. An optional inner_pred enforces
    any additional selection conditions on the inner table.
    """

    def __init__(self, outer_scan, inner_ts, index, outer_field, inner_pred=None):
        self.outer = outer_scan
        self.inner_ts = inner_ts
        self.index = index
        self.outer_field = outer_field
        self.inner_pred = inner_pred
        self._rids = []
        self._pos = 0
        self._started = False
        self._done = False

    def _advance_outer(self):
        while True:
            if not self.outer.nextRecord():
                self._done = True
                return False
            self._rids = self.index.search(self.outer.getVal(self.outer_field))
            self._pos = 0
            if self._rids:
                return True

    def nextRecord(self):
        if not self._started:
            self._started = True
            if not self._advance_outer():
                return False
        while True:
            if self._done:
                return False
            while self._pos < len(self._rids):
                rid = self._rids[self._pos]
                self._pos += 1
                self.inner_ts.moveToRecordID(rid)
                if self.inner_pred is None or self.inner_pred.isSatisfied(self):
                    return True
            if not self._advance_outer():
                return False

    def beforeFirst(self):
        if hasattr(self.outer, "beforeFirst"):
            self.outer.beforeFirst()
        self._rids = []
        self._pos = 0
        self._started = False
        self._done = False

    def getInt(self, field_name):
        if self.outer.hasField(field_name):
            return self.outer.getInt(field_name)
        return self.inner_ts.getInt(field_name)

    def getString(self, field_name):
        if self.outer.hasField(field_name):
            return self.outer.getString(field_name)
        return self.inner_ts.getString(field_name)

    def getVal(self, field_name):
        if self.outer.hasField(field_name):
            return self.outer.getVal(field_name)
        return self.inner_ts.getVal(field_name)

    def hasField(self, field_name):
        return self.outer.hasField(field_name) or self.inner_ts.hasField(field_name)

    def closeRecordPage(self):
        self.outer.closeRecordPage()
        self.inner_ts.closeRecordPage()


class _IndexPlan:
    """Plan node that opens an IndexScan for field = constant lookups."""

    def __init__(self, table_plan, index, search_key):
        self._tp = table_plan
        self._idx = index
        self._key = search_key

    def open(self):
        return IndexScan(self._tp.open(), self._idx, self._key)

    def blocksAccessed(self):
        return 1

    def recordsOutput(self):
        return self._tp.recordsOutput()

    def distinctValues(self, field_name):
        return self._tp.distinctValues(field_name)

    def plan_schema(self):
        return self._tp.plan_schema()


class _IndexJoinPlan:
    """Plan node that opens an IndexJoinScan for field = field joins."""

    def __init__(
        self, outer_plan, inner_table_plan, index, outer_field, inner_pred=None
    ):
        self._outer = outer_plan
        self._inner_tp = inner_table_plan
        self._index = index
        self._outer_f = outer_field
        self._inner_pred = inner_pred
        self._schema = Schema()
        self._schema.field_info = {
            **outer_plan.plan_schema().field_info,
            **inner_table_plan.plan_schema().field_info,
        }

    def open(self):
        return IndexJoinScan(
            self._outer.open(),
            self._inner_tp.open(),
            self._index,
            self._outer_f,
            self._inner_pred,
        )

    def blocksAccessed(self):
        return self._outer.blocksAccessed() + 1

    def recordsOutput(self):
        return self._outer.recordsOutput()

    def distinctValues(self, field_name):
        return self._outer.distinctValues(field_name)

    def plan_schema(self):
        return self._schema


class IndexQueryPlanner:
    """
    A planner that optimizes queries by using indexes for equality conditions
    (field = constant) and join conditions (field = field).

    When better_planner is None (index mode): preserves original join order,
    substituting index scans wherever possible.

    When better_planner is set (full mode): applies greedy join reordering on
    top of index scans.
    """

    def __init__(self, mm, indexes, better_planner=None):
        self.mm = mm
        self.indexes = indexes or {}
        self.better_planner = better_planner

    def createPlan(self, tx, query_data):
        tables = query_data["tables"]
        base_plans = {t: TablePlan(tx, t, self.mm) for t in tables}
        single_terms, join_terms = _classify_terms(
            tables, base_plans, query_data["predicate"].terms
        )

        pushed_plans = {}
        for t in tables:
            idx, key = _find_constant_index(self.indexes, t, single_terms[t])
            if idx:
                pushed_plans[t] = _IndexPlan(base_plans[t], idx, key)
            elif single_terms[t]:
                pushed_plans[t] = SelectPlan(
                    base_plans[t], _make_predicate(single_terms[t])
                )
            else:
                pushed_plans[t] = base_plans[t]

        if self.better_planner is not None:
            order = sorted(tables, key=lambda t: base_plans[t].recordsOutput())
        else:
            order = tables

        current_plan = pushed_plans[order[0]]
        current_fields = _schema_fields(base_plans[order[0]])
        to_join = list(order[1:])
        used = set()

        while to_join:
            if self.better_planner is not None:
                best_t, best_idxs = _pick_next_table(
                    to_join, base_plans, join_terms, current_fields, used
                )
            else:
                best_t = to_join[0]
                t_fields = _schema_fields(base_plans[best_t])
                best_idxs = [
                    i
                    for i, jt in enumerate(join_terms)
                    if i not in used
                    and _term_fields(jt) & t_fields
                    and _term_fields(jt) & current_fields
                ]

            rem_join = [join_terms[i] for i in best_idxs]
            idx_j, outer_f, local_i = _find_join_index(
                self.indexes, best_t, current_fields, rem_join
            )

            if idx_j is not None:
                inner_pred = (
                    _make_predicate(single_terms[best_t])
                    if single_terms[best_t]
                    else None
                )
                current_plan = _IndexJoinPlan(
                    current_plan, base_plans[best_t], idx_j, outer_f, inner_pred
                )
                used.add(best_idxs[local_i])
                other_idxs = [i for i in best_idxs if i != best_idxs[local_i]]
                if other_idxs:
                    current_plan = SelectPlan(
                        current_plan,
                        _make_predicate([join_terms[i] for i in other_idxs]),
                    )
                    used.update(other_idxs)
            else:
                current_plan = ProductPlan(current_plan, pushed_plans[best_t])
                if best_idxs:
                    current_plan = SelectPlan(
                        current_plan,
                        _make_predicate([join_terms[i] for i in best_idxs]),
                    )
                    used.update(best_idxs)

            current_fields |= _schema_fields(base_plans[best_t])
            to_join.remove(best_t)

        leftover = [join_terms[i] for i in range(len(join_terms)) if i not in used]
        if leftover:
            current_plan = SelectPlan(current_plan, _make_predicate(leftover))
        return ProjectPlan(current_plan, *query_data["fields"])


def create_indexes(db, tx, index_defs=None, composite_index_defs=None):
    """
    Step 1: Instantiate BTreeIndex objects for each entry in index_defs.
            - `index_defs` is a dict {table_name: [(field_name, field_type, field_length), ...]}

    Step 2: Instantiate CompositeIndex objects for each entry in composite_index_defs.
            - `composite_index_defs` is a dict {table_name: [((field_names,...), (field_types,...), (field_lengths,...)), ...]}

    Step 3: Populate all indexes by scanning each table once.

    Returns:
        dict {table_name: {field_key: IndexObject}}
        - field_key is the field name (str) for BTreeIndex
        - field_key is the tuple of field names for CompositeIndex
    """
    index_defs = index_defs or {}
    composite_index_defs = composite_index_defs or {}

    all_tables = set(index_defs) | set(composite_index_defs)
    indexes = {t: {} for t in all_tables}

    for table_name, field_list in index_defs.items():
        for field_name, field_type, field_length in field_list:
            indexes[table_name][field_name] = BTreeIndex(
                tx, f"{table_name}_{field_name}_btree", field_type, field_length
            )

    for table_name, comp_list in composite_index_defs.items():
        for field_names, field_types, field_lengths in comp_list:
            indexes[table_name][tuple(field_names)] = CompositeIndex(
                tx,
                f"{table_name}_{'_'.join(field_names)}_composite",
                field_names,
                field_types,
                field_lengths,
            )

    mm = db.mm
    for table_name in all_tables:
        layout = mm.getLayout(tx, table_name)
        btree_fields = [fk for fk in indexes[table_name] if isinstance(fk, str)]
        composite_keys = [fk for fk in indexes[table_name] if isinstance(fk, tuple)]
        ts = TableScan(tx, table_name, layout)
        while ts.nextRecord():
            rid = ts.currentRecordID()
            for field_name in btree_fields:
                indexes[table_name][field_name].insert(ts.getVal(field_name), rid)
            for field_key in composite_keys:
                indexes[table_name][field_key].insert(
                    [ts.getVal(fn) for fn in field_key], rid
                )
        ts.closeRecordPage()

    return indexes
