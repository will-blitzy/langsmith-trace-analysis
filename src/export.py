#!/usr/bin/env python3
"""
Export full LangSmith trace data to JSON for offline analysis.

Runs on YOUR machine (same IP/browser session that can reach langsmith.blitzy.com),
since the instance is behind a Cloudflare managed challenge.

Setup:
    pip install requests python-dotenv
    cp .env.example .env   # then fill in values

Usage:
    # Full trace tree by root run ID (recommended):
    python src/export.py --name my-export --trace <root-run-id>

    # Skip S3 blob resolution (faster, leaves inputs/outputs empty):
    python src/export.py --name my-export --trace <root-run-id> --no-resolve-blobs

    # By explicit run IDs (flat, no children):
    python src/export.py --name my-export --run-ids <id1> <id2> ...

    # By project + most recent N (optionally only root runs):
    python src/export.py --name my-export --project "my-project" --limit 25 --root-only

    Output is written to exports/<name>/runs_export.json
"""
import argparse
import base64
import concurrent.futures
import json
import os
import sys
import time

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv optional; set env vars manually or via .env

BASE = "https://langsmith.blitzy.com/api/v1"
BASE_HOST = "https://langsmith.blitzy.com"

_BLOB_WORKERS = 20
_BLOB_RETRIES = 3
_BLOB_BACKOFF = 0.5  # seconds; doubles each retry


# ---------------------------------------------------------------------------
# Sessions
# ---------------------------------------------------------------------------

def session():
    s = requests.Session()
    key = os.environ.get("LANGSMITH_API_KEY")
    if not key:
        sys.exit("Set LANGSMITH_API_KEY in .env or environment")
    s.headers["x-api-key"] = key
    ua = os.environ.get("BROWSER_UA")
    if ua:
        s.headers["User-Agent"] = ua
    cf = os.environ.get("CF_CLEARANCE")
    if cf:
        s.cookies.set("cf_clearance", cf, domain="langsmith.blitzy.com")
    return s


def _blob_session():
    """Session for presigned blob fetches — CF cookie + UA, no x-api-key."""
    s = requests.Session()
    ua = os.environ.get("BROWSER_UA")
    if ua:
        s.headers["User-Agent"] = ua
    cf = os.environ.get("CF_CLEARANCE")
    if cf:
        s.cookies.set("cf_clearance", cf, domain="langsmith.blitzy.com")
    return s


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def guard(resp):
    if "Just a moment" in resp.text[:500] or resp.status_code == 403:
        sys.exit(
            "Blocked by Cloudflare. Refresh cf_clearance from the browser, "
            "confirm BROWSER_UA matches exactly, and run from the same IP."
        )
    resp.raise_for_status()
    return resp


def get_run(s, run_id):
    r = guard(s.get(f"{BASE}/runs/{run_id}", timeout=30))
    return r.json()


def project_id(s, name):
    r = guard(s.get(f"{BASE}/sessions", params={"name": name}, timeout=30))
    data = r.json()
    sessions = data if isinstance(data, list) else data.get("sessions", [])
    if not sessions:
        sys.exit(f"No project named {name!r}")
    return sessions[0]["id"]


def query_runs(s, pid, limit, root_only):
    body = {"session": [pid], "limit": limit, "order": "desc"}
    if root_only:
        body["is_root"] = True
    r = guard(s.post(f"{BASE}/runs/query", json=body, timeout=60))
    return r.json().get("runs", [])


def get_trace(s, root_run_id):
    """Fetch all runs in a trace and return them nested as a tree."""
    body = {"trace": root_run_id}
    r = guard(s.post(f"{BASE}/runs/query", json=body, timeout=60))
    runs = r.json().get("runs", [])
    if not runs:
        return get_run(s, root_run_id)
    return _build_tree(runs, root_run_id)


def _build_tree(runs, root_id):
    """Nest runs under their parents; return the root node."""
    by_id = {r["id"]: dict(r, child_runs=[]) for r in runs}
    root = None
    for node in by_id.values():
        if node["id"] == root_id:
            root = node
        parent_id = node.get("parent_run_id")
        if parent_id and parent_id in by_id:
            by_id[parent_id]["child_runs"].append(node)
    return root if root is not None else list(by_id.values())


# ---------------------------------------------------------------------------
# S3 blob resolution
# ---------------------------------------------------------------------------

def _jwt_exp(presigned_url):
    """Decode the exp claim from the JWT in a presigned URL, or return None."""
    try:
        jwt = presigned_url.split("jwt=")[1].split("&")[0]
        payload_b64 = jwt.split(".")[1]
        payload_b64 += "=" * (-len(payload_b64) % 4)
        return json.loads(base64.urlsafe_b64decode(payload_b64)).get("exp")
    except Exception:
        return None


def _url_expired(presigned_url):
    exp = _jwt_exp(presigned_url)
    return exp is not None and time.time() > exp


def _fetch_blob(bs, url, retries=_BLOB_RETRIES):
    """GET a presigned URL with retries. Returns (data, error_str)."""
    for attempt in range(retries):
        try:
            r = bs.get(url, timeout=30)
            if r.status_code in (403, 404):
                return None, f"HTTP {r.status_code}"
            r.raise_for_status()
            return r.json(), None
        except requests.exceptions.RequestException as exc:
            if attempt < retries - 1:
                time.sleep(_BLOB_BACKOFF * (2 ** attempt))
            else:
                return None, str(exc)


def _iter_tree(node):
    """Yield every node in the run tree (depth-first)."""
    if isinstance(node, list):
        for n in node:
            yield from _iter_tree(n)
        return
    yield node
    for child in node.get("child_runs", []):
        yield from _iter_tree(child)


def _collect_tasks(tree):
    """
    Return (run, target_field, full_url) for every blob that needs fetching.

    Mapping rules:
      inputs_s3_urls["ROOT"]  -> run["inputs"]
      outputs_s3_urls["ROOT"] -> run["outputs"]
      s3_urls[key]            -> run[key]   (e.g. "error", "messages")
    """
    tasks = []
    for run in _iter_tree(tree):
        for s3_field, target_field in [
            ("inputs_s3_urls", "inputs"),
            ("outputs_s3_urls", "outputs"),
        ]:
            ref = run.get(s3_field, {})
            entry = ref.get("ROOT") if ref else None
            if entry and run.get(target_field) is None:
                url = entry.get("presigned_url", "")
                if url:
                    tasks.append((run, target_field, BASE_HOST + url))

        for field_name, entry in (run.get("s3_urls") or {}).items():
            if run.get(field_name) is None:
                url = (entry or {}).get("presigned_url", "")
                if url:
                    tasks.append((run, field_name, BASE_HOST + url))

    return tasks


def resolve_blobs(tree):
    """
    Fetch all S3-offloaded payloads and merge them into run dicts in-place.
    Runs concurrently; logs failures without aborting.
    """
    tasks = _collect_tasks(tree)
    if not tasks:
        print("No S3 blobs to resolve.")
        return

    print(f"Resolving {len(tasks)} S3 blobs ({_BLOB_WORKERS} workers)...", flush=True)
    bs = _blob_session()
    failed = 0
    done = 0

    def _work(task):
        run, field, url = task
        if _url_expired(url):
            return run, field, None, "expired"
        data, err = _fetch_blob(bs, url)
        return run, field, data, err

    with concurrent.futures.ThreadPoolExecutor(max_workers=_BLOB_WORKERS) as pool:
        futures = {pool.submit(_work, t): t for t in tasks}
        for fut in concurrent.futures.as_completed(futures):
            run, field, data, err = fut.result()
            done += 1
            if err:
                failed += 1
                print(
                    f"  WARN  run={run.get('id', '?')[:8]}  "
                    f"field={field}  reason={err}",
                    flush=True,
                )
            else:
                run[field] = data
            if done % 500 == 0 or done == len(tasks):
                print(f"  {done}/{len(tasks)} fetched  {failed} failed", flush=True)

    print(f"Blobs resolved: {done - failed} ok, {failed} failed.")


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

_DEFAULT_MAX_MB = 30


def _count_tree(node):
    if isinstance(node, list):
        return sum(_count_tree(n) for n in node)
    return 1 + _count_tree(node.get("child_runs", []))


def _write_output(result, out_path, max_part_bytes):
    """
    Write result to out_path, splitting into _part1, _part2, … files when the
    serialized size would exceed max_part_bytes.  Only trace-tree results (a
    root dict with child_runs) can be split; flat lists are always written as
    one file.  Returns the list of paths actually written.
    """
    if not isinstance(result, dict) or not result.get("child_runs"):
        with open(out_path, "w") as f:
            json.dump(result, f, indent=2, default=str)
        print(f"  {out_path}  ({os.path.getsize(out_path) / 1e6:.1f} MB)")
        return [out_path]

    root_meta = {k: v for k, v in result.items() if k != "child_runs"}
    root_overhead = len(json.dumps(root_meta, indent=2, default=str).encode())
    children = result["child_runs"]

    print(f"Measuring sizes for {len(children)} child subtrees …", flush=True)
    child_sizes = [
        len(json.dumps(c, indent=2, default=str).encode()) for c in children
    ]
    total = root_overhead + sum(child_sizes)

    if total <= max_part_bytes:
        with open(out_path, "w") as f:
            json.dump(result, f, indent=2, default=str)
        print(f"  {out_path}  ({total / 1e6:.1f} MB)")
        return [out_path]

    # Greedy bin-pack children into parts, each under the size cap
    parts, current_indices, current_sz = [], [], root_overhead
    for i, sz in enumerate(child_sizes):
        if current_indices and current_sz + sz > max_part_bytes:
            parts.append(current_indices)
            current_indices, current_sz = [], root_overhead
        current_indices.append(i)
        current_sz += sz
    if current_indices:
        parts.append(current_indices)

    base, ext = os.path.splitext(out_path)
    written = []
    for n, indices in enumerate(parts, 1):
        path = f"{base}_part{n}{ext}"
        part = dict(root_meta, child_runs=[children[i] for i in indices])
        with open(path, "w") as f:
            json.dump(part, f, indent=2, default=str)
        print(f"  {path}  ({os.path.getsize(path) / 1e6:.1f} MB)")
        written.append(path)

    return written


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    p = argparse.ArgumentParser(description="Export LangSmith runs/traces to JSON")
    p.add_argument("--trace", metavar="ROOT_RUN_ID",
                   help="fetch full trace tree for this root run ID")
    p.add_argument("--run-ids", nargs="+",
                   help="fetch individual runs by ID (flat, no children)")
    p.add_argument("--project",
                   help="fetch recent runs from a named project")
    p.add_argument("--limit", type=int, default=25)
    p.add_argument("--root-only", action="store_true")
    p.add_argument("--no-resolve-blobs", action="store_true",
                   help="skip S3 blob resolution (leaves inputs/outputs empty)")
    p.add_argument("--name", required=True,
                   help="export name; files are written to exports/<name>/")
    p.add_argument("--out", default="runs_export.json",
                   help="filename inside the export folder (default: runs_export.json)")
    p.add_argument("--max-mb", type=float, default=_DEFAULT_MAX_MB,
                   help=f"max size per output file in MB (default: {_DEFAULT_MAX_MB})")
    a = p.parse_args()

    export_dir = os.path.join("exports", a.name)
    os.makedirs(export_dir, exist_ok=True)
    out_path = os.path.join(export_dir, a.out)

    s = session()

    if a.trace:
        result = get_trace(s, a.trace)
        if not a.no_resolve_blobs:
            resolve_blobs(result)
        count = _count_tree(result)
    elif a.run_ids:
        result = [get_run(s, rid) for rid in a.run_ids]
        count = len(result)
    elif a.project:
        result = query_runs(s, project_id(s, a.project), a.limit, a.root_only)
        count = len(result)
    else:
        p.error("Provide --trace, --run-ids, or --project")

    written = _write_output(result, out_path, int(a.max_mb * 1_000_000))
    parts_label = f"{len(written)} file(s)" if len(written) > 1 else "1 file"
    print(f"Wrote {count} run(s) across {parts_label} in {export_dir}/")


if __name__ == "__main__":
    main()
