"""Shared plumbing: ATTACH options, the download cache, polite HTTP and value helpers.

Every upstream URL can be overridden at ``ATTACH`` time (mirrors, air-gapped
copies, recorded fixtures)::

    ATTACH 'us_elections' (TYPE vgi, LOCATION '...',
        DATAVERSE_URL 'http://127.0.0.1:8765/dataverse',
        LEWIS_BASE_URL 'http://127.0.0.1:8765/lewis',
        CENSUS_CD_BASE_URL 'http://127.0.0.1:8765/census',
        FEC_BASE_URL 'http://127.0.0.1:8765/fec/v1',
        CACHE_DIR '/tmp/us-elections-cache');

Nothing is downloaded at import or ATTACH time; each relation fetches what it
needs on first use and reuses the cached copy afterwards.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import platformdirs

log = logging.getLogger(__name__)

USER_AGENT = "vgi-us-elections/0.1 (+https://query.farm)"
TIMEOUT_SECONDS = 120.0
MAX_ATTEMPTS = 4

DEFAULT_DATAVERSE_URL = "https://dataverse.harvard.edu"
# Jeffrey B. Lewis et al., congressional-district-boundaries (MIT), pinned to one commit so
# every download is reproducible and verifiable against the bundled git blob hashes.
LEWIS_COMMIT = "3c663a8098b58156601e0893f0e67b502f7f8354"
DEFAULT_LEWIS_BASE_URL = (
    f"https://raw.githubusercontent.com/JeffreyBLewis/congressional-district-boundaries/{LEWIS_COMMIT}"
)
DEFAULT_CENSUS_CD_BASE_URL = "https://www2.census.gov/geo/tiger/GENZ2024/shp"
DEFAULT_FEC_BASE_URL = "https://api.open.fec.gov/v1"
DEFAULT_MAX_AGE_DAYS = 30


class UpstreamError(RuntimeError):
    """An upstream service returned an error we will not retry."""


@dataclass(frozen=True, slots=True)
class Options:
    """Where the worker reads from and caches to, resolved from the ATTACH options."""

    cache_dir: Path = Path(platformdirs.user_cache_dir("vgi-us-elections", appauthor=False))
    max_age_days: int = DEFAULT_MAX_AGE_DAYS
    offline: bool = False
    dataverse_url: str = DEFAULT_DATAVERSE_URL
    lewis_base_url: str = DEFAULT_LEWIS_BASE_URL
    census_cd_base_url: str = DEFAULT_CENSUS_CD_BASE_URL
    fec_base_url: str = DEFAULT_FEC_BASE_URL
    guestbook_name: str = ""
    guestbook_email: str = ""
    guestbook_institution: str = ""
    guestbook_position: str = ""
    dataverse_api_token: str = ""

    @property
    def has_guestbook(self) -> bool:
        """True when every field a Harvard Dataverse guestbook asks for was supplied."""
        return all((self.guestbook_name, self.guestbook_email, self.guestbook_institution, self.guestbook_position))


def options_from(raw: dict[str, Any] | None) -> Options:
    """Build `Options` from ATTACH options (names lower-cased; absent or empty means default)."""
    opts = {k.lower(): v for k, v in (raw or {}).items() if v is not None and v != ""}
    d = Options()

    def text(name: str, default: str) -> str:
        return str(opts.get(name, default))

    return Options(
        cache_dir=Path(str(opts["cache_dir"])).expanduser() if opts.get("cache_dir") else d.cache_dir,
        max_age_days=int(opts.get("max_age_days", d.max_age_days)),
        offline=bool(opts.get("offline", False)),
        dataverse_url=text("dataverse_url", d.dataverse_url).rstrip("/"),
        lewis_base_url=text("lewis_base_url", d.lewis_base_url).rstrip("/"),
        census_cd_base_url=text("census_cd_base_url", d.census_cd_base_url).rstrip("/"),
        fec_base_url=text("fec_base_url", d.fec_base_url).rstrip("/"),
        guestbook_name=text("guestbook_name", ""),
        guestbook_email=text("guestbook_email", ""),
        guestbook_institution=text("guestbook_institution", ""),
        guestbook_position=text("guestbook_position", ""),
        dataverse_api_token=text("dataverse_api_token", ""),
    )


def options_of(params: Any) -> Options:
    """Return the options for a call, from its ATTACH options when it has any."""
    try:
        raw = dict(params.attach_options or {})
    except Exception:  # not invoked through an ATTACHed catalog
        raw = {}
    return options_from(raw)


def request(
    url: str,
    *,
    data: bytes | None = None,
    headers: dict[str, str] | None = None,
    accept: str = "application/json",
) -> bytes:
    """Fetch ``url`` (POST when ``data`` is given), retrying 429/5xx/network errors with backoff.

    Args:
        url: Absolute URL.
        data: Request body for a POST.
        headers: Extra request headers.
        accept: The Accept header.

    Returns:
        The response body.

    Raises:
        HttpStatusError: A non-retryable HTTP status.
        UpstreamError: A network failure, or retries exhausted.

    """
    all_headers = {"User-Agent": USER_AGENT, "Accept": accept, **(headers or {})}
    shown = redact(url)
    for attempt in range(1, MAX_ATTEMPTS + 1):
        req = urllib.request.Request(url, data=data, headers=all_headers, method="POST" if data else "GET")
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT_SECONDS) as response:
                body: bytes = response.read()
                return body
        except urllib.error.HTTPError as err:
            body = err.read()
            retryable = err.code == 429 or err.code >= 500
            if not retryable or attempt == MAX_ATTEMPTS:
                raise HttpStatusError(err.code, f"{req.get_method()} {shown}", body) from err
            retry_after = err.headers.get("Retry-After")
            delay = float(retry_after) if retry_after and retry_after.isdigit() else 0.5 * 2**attempt
            log.warning("HTTP %s from %s; retrying in %.1fs", err.code, shown, delay)
            time.sleep(min(delay, 30.0))
        except (urllib.error.URLError, TimeoutError, ConnectionError) as err:
            if attempt == MAX_ATTEMPTS:
                raise UpstreamError(f"{req.get_method()} {shown} failed: {getattr(err, 'reason', err)}") from err
            time.sleep(0.5 * 2**attempt)
    raise UpstreamError(f"{shown}: retries exhausted")


class HttpStatusError(UpstreamError):
    """A non-retryable HTTP status, with the (truncated) response body."""

    def __init__(self, status: int, what: str, body: bytes) -> None:
        """Keep the status and body so callers can interpret API-specific errors."""
        self.status = status
        self.body = body
        super().__init__(f"{what} failed with HTTP {status}: {body[:300].decode(errors='replace')}")


def get_json(url: str, headers: dict[str, str] | None = None) -> Any:
    """GET ``url`` and decode its JSON body."""
    return json.loads(request(url, headers=headers) or b"{}")


_KEY_PARAM = re.compile(r"(api_key=)[^&]+", re.IGNORECASE)


def redact(url: str) -> str:
    """Hide an ``api_key`` query parameter so URLs can appear in errors and logs."""
    return _KEY_PARAM.sub(r"\1***", url)


# ---------------------------------------------------------------- cache files


def write_atomic(path: Path, data: bytes) -> None:
    """Write ``data`` to ``path`` via a temp file and rename, so readers never see half a file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    tmp.write_bytes(data)
    tmp.replace(path)


def read_json(path: Path) -> dict[str, Any]:
    """Read a JSON object from ``path``; a missing or corrupt file reads as ``{}``."""
    try:
        loaded = json.loads(path.read_text())
        return loaded if isinstance(loaded, dict) else {}
    except OSError, ValueError:
        return {}


def write_json(path: Path, value: dict[str, Any]) -> None:
    """Write a JSON object atomically."""
    write_atomic(path, json.dumps(value, indent=1, sort_keys=True).encode())


def md5_hex(data: bytes) -> str:
    """MD5 of ``data`` (Dataverse publishes MD5 checksums)."""
    return hashlib.md5(data, usedforsecurity=False).hexdigest()


def sha256_hex(data: bytes) -> str:
    """SHA-256 of ``data``."""
    return hashlib.sha256(data).hexdigest()


def git_blob_sha1(data: bytes) -> str:
    """Return the git blob id of ``data`` (what GitHub's tree listing reports for a file)."""
    return hashlib.sha1(b"blob %d\0" % len(data) + data, usedforsecurity=False).hexdigest()


def quote_path(path: str) -> str:
    """Percent-encode a URL path (file names contain spaces)."""
    return urllib.parse.quote(path)


# ---------------------------------------------------------------- districts


_DISTRICT_NUM = re.compile(r"^\s*0*(\d{1,3})(\.0+)?\s*$")
_AT_LARGE = {"AL", "AT LARGE", "AT-LARGE", "ATLARGE", "STATEWIDE", "0", "00"}


def normalize_district(value: Any) -> str | None:
    """Normalize a House district to the two-digit VARCHAR convention used across this worker.

    '00' is at-large (a state with a single seat): 'AL', 'at-large', '0', 0, 0.0 all map to it.
    '1', 1, '01', 1.0 map to '01'. Non-voting delegates keep Census's '98'. Anything else
    (blank, 'ZZ', words) is NULL.
    """
    if value is None:
        return None
    if isinstance(value, float):
        value = int(value) if value.is_integer() else value
    text = str(value).strip().upper()
    if text in _AT_LARGE:
        return "00"
    match = _DISTRICT_NUM.match(text)
    if not match:
        return None
    number = int(match.group(1))
    return f"{number:02d}" if number < 100 else None


def congress_for_election_year(year: int) -> int:
    """Return the Congress elected at a regular November general election held in ``year`` (2024 -> 119)."""
    return (year - 1786) // 2
