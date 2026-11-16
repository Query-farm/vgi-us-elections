"""US states and territories: the FIPS / USPS / name join keys every table here uses.

FIPS state codes are the two-digit ANSI/FIPS 5-2 codes the Census Bureau uses
(kept as VARCHAR with the leading zero, e.g. '06'); USPS codes are the
two-letter postal abbreviations MEDSL calls ``state_po`` and FEC calls ``state``.
"""

from __future__ import annotations

import re

# (fips, usps, name, kind) - kind is 'state', 'district' (DC) or 'territory'.
STATES: tuple[tuple[str, str, str, str], ...] = (
    ("01", "AL", "Alabama", "state"),
    ("02", "AK", "Alaska", "state"),
    ("04", "AZ", "Arizona", "state"),
    ("05", "AR", "Arkansas", "state"),
    ("06", "CA", "California", "state"),
    ("08", "CO", "Colorado", "state"),
    ("09", "CT", "Connecticut", "state"),
    ("10", "DE", "Delaware", "state"),
    ("11", "DC", "District of Columbia", "district"),
    ("12", "FL", "Florida", "state"),
    ("13", "GA", "Georgia", "state"),
    ("15", "HI", "Hawaii", "state"),
    ("16", "ID", "Idaho", "state"),
    ("17", "IL", "Illinois", "state"),
    ("18", "IN", "Indiana", "state"),
    ("19", "IA", "Iowa", "state"),
    ("20", "KS", "Kansas", "state"),
    ("21", "KY", "Kentucky", "state"),
    ("22", "LA", "Louisiana", "state"),
    ("23", "ME", "Maine", "state"),
    ("24", "MD", "Maryland", "state"),
    ("25", "MA", "Massachusetts", "state"),
    ("26", "MI", "Michigan", "state"),
    ("27", "MN", "Minnesota", "state"),
    ("28", "MS", "Mississippi", "state"),
    ("29", "MO", "Missouri", "state"),
    ("30", "MT", "Montana", "state"),
    ("31", "NE", "Nebraska", "state"),
    ("32", "NV", "Nevada", "state"),
    ("33", "NH", "New Hampshire", "state"),
    ("34", "NJ", "New Jersey", "state"),
    ("35", "NM", "New Mexico", "state"),
    ("36", "NY", "New York", "state"),
    ("37", "NC", "North Carolina", "state"),
    ("38", "ND", "North Dakota", "state"),
    ("39", "OH", "Ohio", "state"),
    ("40", "OK", "Oklahoma", "state"),
    ("41", "OR", "Oregon", "state"),
    ("42", "PA", "Pennsylvania", "state"),
    ("44", "RI", "Rhode Island", "state"),
    ("45", "SC", "South Carolina", "state"),
    ("46", "SD", "South Dakota", "state"),
    ("47", "TN", "Tennessee", "state"),
    ("48", "TX", "Texas", "state"),
    ("49", "UT", "Utah", "state"),
    ("50", "VT", "Vermont", "state"),
    ("51", "VA", "Virginia", "state"),
    ("53", "WA", "Washington", "state"),
    ("54", "WV", "West Virginia", "state"),
    ("55", "WI", "Wisconsin", "state"),
    ("56", "WY", "Wyoming", "state"),
    ("60", "AS", "American Samoa", "territory"),
    ("66", "GU", "Guam", "territory"),
    ("69", "MP", "Northern Mariana Islands", "territory"),
    ("72", "PR", "Puerto Rico", "territory"),
    ("78", "VI", "U.S. Virgin Islands", "territory"),
)

BY_FIPS = {fips: (fips, usps, name) for fips, usps, name, _ in STATES}
BY_USPS = {usps: (fips, usps, name) for fips, usps, name, _ in STATES}
_BY_NAME = {name.upper(): (fips, usps, name) for fips, usps, name, _ in STATES}
_BY_NAME["DISTRICT OF COLUMBIA"] = BY_USPS["DC"]
_BY_NAME["VIRGIN ISLANDS"] = BY_USPS["VI"]


class UnknownStateError(ValueError):
    """A state argument that is not a USPS code, FIPS code or state name."""


def lookup(value: str | None) -> tuple[str, str, str] | None:
    """Resolve a USPS code ('de'), FIPS code ('10' or '10.0') or name ('Delaware') to (fips, usps, name)."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    upper = text.upper()
    if upper in BY_USPS:
        return BY_USPS[upper]
    if re.fullmatch(r"\d{1,2}(\.0+)?", text):
        return BY_FIPS.get(f"{int(float(text)):02d}")
    return _BY_NAME.get(re.sub(r"\s+", " ", upper))


def resolve(value: str | None) -> tuple[str, str, str] | None:
    """Like `lookup`, but raise a helpful error for a non-empty value that matches nothing.

    Args:
        value: A USPS code, FIPS code or state name; NULL/empty means "all states".

    Returns:
        ``(fips, usps, name)``, or None when ``value`` is empty.

    Raises:
        UnknownStateError: The value is not a recognised state or territory.

    """
    if value is None or not str(value).strip():
        return None
    found = lookup(value)
    if found is None:
        raise UnknownStateError(
            f"unknown state {value!r}: pass a USPS code ('DE'), a two-digit FIPS code ('10') or a name ('Delaware')"
        )
    return found
