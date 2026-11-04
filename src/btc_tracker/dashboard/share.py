"""Delta Sharing access: env-first, local file fallback.

Secrets never commit: `*.share` is gitignored. In prod (container/VPS) set:
  DELTA_SHARE_ENDPOINT  e.g. https://<workspace>.cloud.databricks.com/api/2.0/unity-catalog/shares/...
  DELTA_SHARE_TOKEN     bearer token from your share profile
Locally, falls back to `<repo-root>/config.share` whatever the cwd is.
"""
import json
import os
import tempfile
from pathlib import Path

import duckdb
import httpx

SHARE = "btc-dashboard"
SCHEMA = "btc_data"

_profile_path = None


def profile_path() -> str:
    global _profile_path
    if _profile_path:
        return _profile_path
    endpoint = os.getenv("DELTA_SHARE_ENDPOINT")
    token = os.getenv("DELTA_SHARE_TOKEN")
    if endpoint and token:
        f = tempfile.NamedTemporaryFile("w", suffix=".share", delete=False)
        json.dump(
            {
                "shareCredentialsVersion": 1,
                "endpoint": endpoint,
                "bearerToken": token,
            },
            f,
        )
        f.close()
        _profile_path = f.name
        return _profile_path
    here = Path(__file__).resolve()
    for parent in [here, *here.parents]:
        cand = parent / "config.share"
        if cand.exists():
            _profile_path = str(cand)
            return _profile_path
    raise FileNotFoundError(
        "Delta Sharing profile not found: set DELTA_SHARE_ENDPOINT/DELTA_SHARE_TOKEN "
        "or place config.share at the repo root (gitignored)."
    )


def table_url(table: str, share: str = SHARE, schema: str = SCHEMA) -> str:
    return f"{profile_path()}#{share}.{schema}.{table}"


def _file_urls(table: str, predicate_hints=None, share: str = SHARE, schema: str = SCHEMA, timeout: int = 120):
    """Pre-signed parquet URLs for a shared table, optionally server-pruned."""
    prof = json.load(open(profile_path()))
    base = f"{prof['endpoint']}/shares/{share}/schemas/{schema}/tables/{table}"
    r = httpx.post(
        f"{base}/query",
        headers={"Authorization": f"Bearer {prof['bearerToken']}"},
        json={"predicateHints": predicate_hints or []},
        timeout=timeout,
    )
    try:
        r.raise_for_status()
    except httpx.HTTPStatusError as e:
        raise httpx.HTTPStatusError(
            f"{e} :: {r.text[:300]}", request=e.request, response=e.response
        )
    return [
        l["file"]["url"]
        for l in map(json.loads, r.text.splitlines())
        if "file" in l and "url" in l.get("file", {})
    ]


def query_share(table: str, sql: str, predicate_hints=None):
    """Run SQL over a shared table's files, return a Polars frame.

    `sql` runs in DuckDB with the file list bound as the `?` parameter, e.g.
    sql="select count(*) from read_parquet(?, union_by_name=True) where ..."
    """
    urls = _file_urls(table, predicate_hints)
    con = duckdb.connect()
    return con.execute(sql, [urls]).pl()
