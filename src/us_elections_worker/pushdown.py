"""Narrow a cached table with the simple predicates DuckDB pushes down.

The scans declare ``filter_pushdown`` with ``auto_apply_filters``: the SDK
re-applies every pushed predicate exactly to each emitted batch. This module only
*pre-filters* the in-memory table on a few key columns (year, state, district,
congress) so a ``WHERE year = 2024 AND state_po = 'DE'`` scan slices out a
handful of rows instead of streaming every election since 1976 through the
exact filter. It must never drop a matching row, so anything it does not fully
understand is left alone.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pyarrow.compute as _pc
from vgi.table_filter_pushdown import AndFilter, ComparisonOp, ConstantFilter, InFilter

pc: Any = _pc

_OPS = {
    ComparisonOp.EQ: "equal",
    ComparisonOp.GT: "greater",
    ComparisonOp.GE: "greater_equal",
    ComparisonOp.LT: "less",
    ComparisonOp.LE: "less_equal",
}


def _flatten(filters: Any) -> list[Any]:
    out: list[Any] = []
    for f in getattr(filters, "filters", None) or ():
        stack = [f]
        while stack:
            item = stack.pop()
            if isinstance(item, AndFilter):
                stack.extend(item.children)
            else:
                out.append(item)
    return out


def prefilter(table: pa.Table, filters: Any, columns: frozenset[str]) -> pa.Table:
    """Return the rows of ``table`` that can satisfy the pushed filters on ``columns``.

    Args:
        table: The full in-memory table.
        filters: ``params.current_pushdown_filters`` (``PushdownFilters`` or None).
        columns: Columns this table allows pre-filtering on.

    Returns:
        A (possibly) smaller table; a superset of the rows that match.

    """
    mask: Any = None
    for f in _flatten(filters):
        name = getattr(f, "column_name", None)
        if name not in columns or name not in table.column_names:
            continue
        col = table.column(name)
        try:
            if isinstance(f, ConstantFilter) and f.op in _OPS:
                cond = getattr(pc, _OPS[f.op])(col, f.value.cast(col.type))
            elif isinstance(f, InFilter):
                cond = pc.is_in(col, value_set=f.values.cast(col.type))
            else:
                continue
        except pa.ArrowInvalid, pa.ArrowNotImplementedError, TypeError:
            continue  # a type we cannot compare here; the exact filter still applies
        cond = pc.fill_null(cond, False)
        mask = cond if mask is None else pc.and_(mask, cond)
    return table if mask is None else table.filter(mask)
