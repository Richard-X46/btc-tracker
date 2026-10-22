
import os
import io
import concurrent.futures as cf

import polars as pl
import requests
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import CommitFailedError
from dotenv import load_dotenv

load_dotenv()
# --------------------------------------------------------------------------
# 1. Pull public data from GitHub
# --------------------------------------------------------------------------

GITHUB_CSV_URL = (
    "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/tips.csv"
)  # small, stable, public dataset - swap for anything raw.githubusercontent.com serves


def fetch_github_csv(url: str) -> pl.DataFrame:
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return pl.read_csv(io.BytesIO(resp.content))


# --------------------------------------------------------------------------
# 2. Local write/read sanity check (no cloud creds needed)
# --------------------------------------------------------------------------

def test_local_write(df: pl.DataFrame, path: str = "/tmp/delta_test_local"):
    df.write_delta(path, mode="overwrite")
    dt = DeltaTable(path)
    read_back = pl.read_delta(path)
    print(f"[local] wrote {df.height} rows, read back {read_back.height} rows, "
          f"version={dt.version()}")
    assert read_back.height == df.height
    return path


# --------------------------------------------------------------------------
# 3. Write to a Hugging Face Storage Bucket over the S3-compatible gateway
# --------------------------------------------------------------------------
def hf_storage_options() -> dict:
    namespace = os.environ["HF_NAMESPACE"]  # your HF username or org
    return {
        "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],       # HFAK...
        "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
        "AWS_ENDPOINT_URL": f"https://s3.hf.co/{namespace}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ADDRESSING_STYLE": "path",
        # Force conditional-PUT based commits instead of relying on
        # object_store's provider auto-detection, since this endpoint
        # isn't recognized as AWS S3 itself.
        "AWS_CONDITIONAL_PUT": "etag",
    }


def hf_table_uri() -> str:
    bucket = os.environ["HF_BUCKET"]
    return f"s3://{bucket}/delta-test/tips"


def test_hf_write(df: pl.DataFrame):
    uri = hf_table_uri()
    opts = hf_storage_options()
    write_deltalake(uri, df.to_arrow(), mode="overwrite", storage_options=opts)
    dt = DeltaTable(uri, storage_options=opts)
    read_back = pl.from_arrow(dt.to_pyarrow_table())
    print(f"[hf] wrote {df.height} rows, read back {read_back.height} rows, "
          f"version={dt.version()}")
    return uri, opts



def _append_one_row(uri: str, opts: dict, tag: str):
    row = pl.DataFrame({"tag": [tag]})
    write_deltalake(uri, row.to_arrow(), mode="append", storage_options=opts)
    return tag


def test_concurrent_commits(uri: str, opts: dict):
    with cf.ThreadPoolExecutor(max_workers=2) as ex:
        futs = [ex.submit(_append_one_row, uri, opts, tag) for tag in ("A", "B")]
        results, errors = [], []
        for f in futs:
            try:
                results.append(f.result())
            except CommitFailedError as e:
                errors.append(str(e))

    print(f"[concurrency] succeeded: {results}, rejected: {len(errors)}")
    if len(errors) == 1 and len(results) == 1:
        print("[concurrency] PASS - conditional PUT is enforcing atomic commits")
    elif len(results) == 2:
        print("[concurrency] FAIL - both writers succeeded; no real atomicity, "
              "check AWS_CONDITIONAL_PUT support on this endpoint")
    else:
        print("[concurrency] inconclusive - inspect errors:", errors)


if __name__ == "__main__":
    df = fetch_github_csv(GITHUB_CSV_URL)
    print(f"pulled {df.height} rows x {df.width} cols from GitHub")

    # Always safe to run:
    test_local_write(df)
    df
    # Only run once HF_NAMESPACE / HF_BUCKET / HF_S3_KEY_ID / HF_S3_SECRET are set:
    if all(k in os.environ for k in
           ("HF_NAMESPACE", "HF_BUCKET", "HF_S3_KEY_ID", "HF_S3_SECRET")):
        uri, opts = test_hf_write(df)
        test_concurrent_commits(uri, opts)
    else:
        print("skipping HF bucket test - set HF_NAMESPACE, HF_BUCKET, "
              "HF_S3_KEY_ID, HF_S3_SECRET to run it")