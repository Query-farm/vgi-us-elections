"""GeoJSON -> WKB, and the GEOMETRY column definition the boundary functions emit.

The column is tagged ``geoarrow.wkb`` with CRS ``OGC:CRS84`` so DuckDB 2.0
surfaces it as ``GEOMETRY('OGC:CRS84')``. Both boundary sources publish
longitude/latitude on NAD83 (EPSG:4269); NAD83 and WGS84 differ by about a
metre in the conterminous US, far below the generalisation of these files, so
the coordinates are passed through unchanged and labelled CRS84 (lon/lat order).
"""

from __future__ import annotations

import json
import struct
from typing import Any

import pyarrow as pa

_TYPES = {"Point": 1, "LineString": 2, "Polygon": 3, "MultiPoint": 4, "MultiLineString": 5, "MultiPolygon": 6}


def geometry_field(comment: str) -> pa.Field[Any]:
    """Build the nullable GEOMETRY('OGC:CRS84') column."""
    return pa.field(
        "geometry",
        pa.binary(),
        nullable=True,
        metadata={
            b"ARROW:extension:name": b"geoarrow.wkb",
            b"ARROW:extension:metadata": json.dumps({"crs": "OGC:CRS84"}).encode(),
            b"comment": comment.encode(),
        },
    )


def _ring(coords: list[Any]) -> bytes:
    out = [struct.pack("<I", len(coords))]
    out.extend(struct.pack("<dd", float(p[0]), float(p[1])) for p in coords)
    return b"".join(out)


def _polygon_body(rings: list[Any]) -> bytes:
    return struct.pack("<I", len(rings)) + b"".join(_ring(r) for r in rings)


def to_wkb(geometry: dict[str, Any] | None) -> bytes | None:
    """Encode a GeoJSON geometry (2-D; extra ordinates dropped) as little-endian ISO WKB.

    Returns None for a missing or empty geometry.
    """
    if not geometry:
        return None
    kind = geometry.get("type")
    coords = geometry.get("coordinates")
    if kind not in _TYPES or coords is None:
        if kind == "GeometryCollection":
            parts = [w for w in (to_wkb(g) for g in geometry.get("geometries") or []) if w]
            return struct.pack("<BII", 1, 7, len(parts)) + b"".join(parts) if parts else None
        return None
    head = struct.pack("<BI", 1, _TYPES[kind])
    if kind == "Point":
        return head + struct.pack("<dd", float(coords[0]), float(coords[1]))
    if kind == "LineString":
        return head + _ring(coords)
    if kind == "Polygon":
        return head + _polygon_body(coords) if coords else None
    if kind == "MultiPoint":
        return head + struct.pack("<I", len(coords)) + b"".join(struct.pack("<BIdd", 1, 1, p[0], p[1]) for p in coords)
    if kind == "MultiLineString":
        return head + struct.pack("<I", len(coords)) + b"".join(struct.pack("<BI", 1, 2) + _ring(c) for c in coords)
    polys = [p for p in coords if p]
    if not polys:
        return None
    return head + struct.pack("<I", len(polys)) + b"".join(struct.pack("<BI", 1, 3) + _polygon_body(p) for p in polys)
