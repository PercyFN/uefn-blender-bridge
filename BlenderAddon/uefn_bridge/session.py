"""Local authenticated transport and exchange-folder validation."""

import hmac
import json
import os
from pathlib import Path
import re
import tempfile
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener


HOST = "127.0.0.1"
SESSION_ENV = "UEFN_BLENDER_BRIDGE_SESSION_DIR"
TOKEN_HEADER = "X-Bridge-Token"
MAX_BODY = 8 * 1024 * 1024
TOKEN_PATTERN = re.compile(r"[0-9a-fA-F]{64}")
GUID_PATTERN = re.compile(r"[0-9a-fA-F]{32}")


def read_session():
    value = os.environ.get(SESSION_ENV)
    if not value:
        raise ValueError(f"Set {SESSION_ENV} to the local bridge session directory.")
    root = Path(value).resolve(strict=True)
    if not root.is_dir():
        raise ValueError("The bridge session path must be a directory.")
    token_path = root / "session_token"
    exchange = root / "exchange"
    if (token_path.resolve(strict=True) != token_path or not token_path.is_file()
            or token_path.stat().st_size > 128):
        raise ValueError("The bridge session token must be a small regular file.")
    if exchange.resolve(strict=True) != exchange or not exchange.is_dir():
        raise ValueError("The bridge exchange must be a directory inside the session.")
    token = token_path.read_text(encoding="ascii").strip()
    if not TOKEN_PATTERN.fullmatch(token):
        raise ValueError("The bridge session token must contain exactly 64 hex characters.")
    return token, exchange


def make_stage(exchange):
    if exchange.resolve(strict=True) != exchange or not exchange.is_dir():
        raise ValueError("The connected bridge exchange is no longer available.")
    return Path(tempfile.mkdtemp(prefix="send-", dir=exchange))


class NoRedirects(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError("Bridge responses cannot redirect requests.")


DIRECT_OPENER = build_opener(ProxyHandler({}), NoRedirects())


def decode_json(data):
    def reject_nonfinite(value):
        raise ValueError(f"Unexpected JSON number: {value}")

    result = json.loads(data.decode("utf-8"), parse_constant=reject_nonfinite)
    if not isinstance(result, dict):
        raise ValueError("Bridge JSON must be an object.")
    return result


def request(port, token, payload=None, timeout=5.0):
    if type(port) is not int or not 1024 <= port <= 65535:
        raise ValueError("Invalid local bridge port.")
    if not TOKEN_PATTERN.fullmatch(token):
        raise ValueError("The bridge session has no valid token.")
    data = None if payload is None else json.dumps(payload, allow_nan=False).encode("utf-8")
    if data is not None and len(data) > MAX_BODY:
        raise ValueError("The bridge request exceeds 8 MiB.")
    headers = {"Content-Type": "application/json", TOKEN_HEADER: token}
    req = Request(f"http://{HOST}:{port}/", data=data, headers=headers)
    with DIRECT_OPENER.open(req, timeout=timeout) as response:
        if response.headers.get_content_type() != "application/json":
            raise ValueError("Expected a JSON response from the local bridge.")
        body = response.read(MAX_BODY + 1)
    if len(body) > MAX_BODY:
        raise ValueError("The bridge response exceeds 8 MiB.")
    return decode_json(body)


def is_authenticated(headers, token):
    given = headers.get(TOKEN_HEADER, "")
    return (not headers.get("Origin") and not headers.get("Sec-Fetch-Site")
            and not headers.get("Transfer-Encoding")
            and headers.get_content_type() == "application/json"
            and bool(token) and bool(TOKEN_PATTERN.fullmatch(given))
            and hmac.compare_digest(given, token))
