#!/usr/bin/env python3
"""Offline stand-in for every upstream the worker reads, serving ``test/fixtures``.

Routes
------
``/dataverse/api/datasets/:persistentId/?persistentId=doi:10.7910/DVN/<X>``
    Dataset metadata (``fixtures/dataverse/datasets/<X>.json``).
``/dataverse/api/access/datafile/<id>``
    GET: the file (``fixtures/dataverse/files/<id>.csv``). For the guestbook-gated
    House and county files a GET answers HTTP 400 like Harvard Dataverse does, and a
    POST carrying a complete ``guestbookResponse`` returns a signed URL.
``/dataverse/signed/<id>``
    The signed download.
``/lewis/GeoJson/<file>`` and ``/census/<file>``
    Boundary files.
``/fec/v1/<endpoint>/?page=<n>&api_key=<key>``
    OpenFEC pages (``fixtures/fec/<endpoint>_page<n>.json``); the key must be
    ``test-key`` or the response is the real 403 API_KEY_INVALID body.

``MOCK_API_FLAKY=1`` makes the first request for every path answer 429 with
``Retry-After: 0`` so the client's retry path always runs.

Usage::

    python3 test/mock_api/server.py            # prints PORT:<n> once listening
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"
GATED = {"13592823", "13573089"}
FEC_KEY = "test-key"
GUESTBOOK_FIELDS = ("name", "email", "institution", "position")

_seen: set[str] = set()
_lock = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    """Serve the fixture tree."""

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002 - stdlib signature
        sys.stderr.write("mock: " + (format % args) + "\n")

    def _send(self, status: int, body: bytes, content_type: str = "application/json") -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        if status == 429:
            self.send_header("Retry-After", "0")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, status: int, value: object) -> None:
        self._send(status, json.dumps(value).encode())

    def _file(self, path: Path, content_type: str) -> None:
        if path.is_file():
            self._send(200, path.read_bytes(), content_type)
        else:
            self._json(404, {"status": "ERROR", "message": f"not found: {path.name}"})

    def _flaky(self, path: str) -> bool:
        if os.environ.get("MOCK_API_FLAKY") != "1":
            return False
        with _lock:
            if path in _seen:
                return False
            _seen.add(path)
        self._json(429, {"error": "slow down"})
        return True

    def do_GET(self) -> None:
        url = urlparse(self.path)
        query = parse_qs(url.query)
        path = unquote(url.path)
        if self._flaky(path + "?" + url.query.replace("api_key=", "k=")):
            return
        if path == "/health":
            self._json(200, {"status": "ok"})
        elif path.startswith("/dataverse/api/datasets/:persistentId"):
            doi = (query.get("persistentId") or [""])[0]
            self._file(FIXTURES / "dataverse" / "datasets" / f"{doi.rsplit('/', 1)[-1]}.json", "application/json")
        elif m := re.fullmatch(r"/dataverse/api/access/datafile/(\d+)", path):
            if m.group(1) in GATED:
                self._json(
                    400,
                    {
                        "status": "ERROR",
                        "message": "You may not download this file without the required Guestbook response "
                        "for guestbookID 458.",
                    },
                )
            else:
                self._file(FIXTURES / "dataverse" / "files" / f"{m.group(1)}.csv", "text/csv")
        elif m := re.fullmatch(r"/dataverse/signed/(\d+)", path):
            self._file(FIXTURES / "dataverse" / "files" / f"{m.group(1)}.csv", "text/csv")
        elif path.startswith("/lewis/GeoJson/"):
            self._file(FIXTURES / "lewis" / "GeoJson" / Path(path).name, "application/geo+json")
        elif path.startswith("/census/"):
            self._file(FIXTURES / "census" / Path(path).name, "application/zip")
        elif path.startswith("/fec/v1/"):
            self._fec(path, query)
        else:
            self._json(404, {"error": f"no route {path}"})

    def do_POST(self) -> None:
        url = urlparse(self.path)
        m = re.fullmatch(r"/dataverse/api/access/datafile/(\d+)", url.path)
        length = int(self.headers.get("Content-Length") or 0)
        try:
            body = json.loads(self.rfile.read(length) or b"{}")
        except ValueError:
            body = {}
        answer = body.get("guestbookResponse") or {}
        if not m or not all(answer.get(f) for f in GUESTBOOK_FIELDS):
            self._json(400, {"status": "ERROR", "message": "incomplete guestbook response"})
            return
        host = self.headers.get("Host")
        self._json(200, {"status": "OK", "data": {"signedUrl": f"http://{host}/dataverse/signed/{m.group(1)}?t=1"}})

    def _fec(self, path: str, query: dict[str, list[str]]) -> None:
        if (query.get("api_key") or [""])[0] != FEC_KEY:
            self._send(403, (FIXTURES / "fec" / "error_api_key_invalid.json").read_bytes())
            return
        endpoint = path[len("/fec/v1/") :].strip("/").replace("/", "_")
        page = (query.get("page") or ["1"])[0]
        target = FIXTURES / "fec" / f"{endpoint}_page{page}.json"
        if target.is_file():
            self._file(target, "application/json")
        else:
            self._json(200, {"api_version": "1.0", "pagination": {"page": int(page), "pages": 0}, "results": []})


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--port", type=int, default=0)
    args = parser.parse_args()
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    print(f"PORT:{server.server_address[1]}", flush=True)
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
