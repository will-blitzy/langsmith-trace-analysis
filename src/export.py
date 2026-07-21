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

    # Multiple trace trees at once (comma- and/or space-separated):
    python src/export.py --name my-export --trace <root-id-1>,<root-id-2>,<root-id-3>
    python src/export.py --name my-export --trace <root-id-1> <root-id-2>

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
import re
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

_QUERY_TIMEOUT = 120     # seconds; per /runs/query request (was 60, tight for big traces)
_QUERY_RETRIES = 3       # retry a query that times out / drops the connection
_QUERY_BACKOFF = 2.0     # seconds; doubles each retry
_TRACE_PAUSE = 1.5       # seconds between traces in a batch, to ease API throttling


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


def _post_query(s, body, timeout=_QUERY_TIMEOUT, retries=_QUERY_RETRIES):
    """
    POST /runs/query, retrying on network timeouts / dropped connections with
    exponential backoff.  A batch of traces can throttle the API, so a slow
    response should be retried rather than aborting the whole export.  Cloudflare
    blocks and HTTP errors (raised by guard) are not retried — they won't recover
    without a fresh cf_clearance.
    """
    last_exc = None
    for attempt in range(retries):
        try:
            return guard(s.post(f"{BASE}/runs/query", json=body, timeout=timeout))
        except (requests.exceptions.Timeout,
                requests.exceptions.ConnectionError) as exc:
            last_exc = exc
        except requests.exceptions.HTTPError as exc:
            # Transient gateway/upstream errors are worth retrying; other HTTP
            # errors (4xx, etc.) are not and propagate immediately.
            status = getattr(exc.response, "status_code", None)
            if status not in (502, 503, 504):
                raise
            last_exc = exc
        if attempt < retries - 1:
            wait = _QUERY_BACKOFF * (2 ** attempt)
            reason = getattr(getattr(last_exc, "response", None),
                             "status_code", None) or type(last_exc).__name__
            print(f"  query error ({reason}); retry "
                  f"{attempt + 1}/{retries - 1} in {wait:.0f}s…", flush=True)
            time.sleep(wait)
    raise last_exc


def query_runs(s, pid, limit, root_only, timeout=_QUERY_TIMEOUT):
    body = {"session": [pid], "limit": limit, "order": "desc"}
    if root_only:
        body["is_root"] = True
    r = _post_query(s, body, timeout=timeout)
    return r.json().get("runs", [])


def get_trace(s, root_run_id, timeout=_QUERY_TIMEOUT):
    """Fetch all runs in a trace and return them nested as a tree."""
    body = {"trace": root_run_id}
    r = _post_query(s, body, timeout=timeout)
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


def _is_tree_list(result):
    """True for a list of trace trees (each a dict carrying child_runs).

    Distinguishes a multi-`--trace` result from a flat list of runs produced by
    --run-ids / --project, whose runs never carry a child_runs key.
    """
    return isinstance(result, list) and any(
        isinstance(r, dict) and "child_runs" in r for r in result
    )


def _dump_json(obj, path):
    with open(path, "w") as f:
        json.dump(obj, f, indent=2, default=str)
    print(f"  {path}  ({os.path.getsize(path) / 1e6:.1f} MB)")


def _split_tree_payloads(tree, max_part_bytes):
    """
    Split one trace tree into a list of tree-dicts whose serialized size each
    stays under max_part_bytes where possible.  A tree that already fits is
    returned as [tree].  Otherwise the root metadata is repeated in every
    payload and the child subtrees are greedily bin-packed across payloads.  A
    single child subtree larger than the cap is placed alone (it cannot be split
    further here, so that one payload may still exceed the cap).
    """
    if not isinstance(tree, dict) or not tree.get("child_runs"):
        return [tree]

    root_meta = {k: v for k, v in tree.items() if k != "child_runs"}
    root_overhead = len(json.dumps(root_meta, indent=2, default=str).encode())
    children = tree["child_runs"]
    child_sizes = [
        len(json.dumps(c, indent=2, default=str).encode()) for c in children
    ]
    if root_overhead + sum(child_sizes) <= max_part_bytes:
        return [tree]

    groups, current, current_sz = [], [], root_overhead
    for i, sz in enumerate(child_sizes):
        if current and current_sz + sz > max_part_bytes:
            groups.append(current)
            current, current_sz = [], root_overhead
        current.append(i)
        current_sz += sz
    if current:
        groups.append(current)

    return [
        dict(root_meta, child_runs=[children[i] for i in idxs]) for idxs in groups
    ]


def _write_output(result, out_path, max_part_bytes):
    """
    Write result to out_path, splitting into _part1, _part2, … files when the
    serialized size would exceed max_part_bytes.  Trace-tree results are split
    by child subtree so no file exceeds the cap (a single subtree larger than
    the cap is the only thing that can): a single tree is written as a bare
    dict (one per part), and multiple trees (several --trace IDs) as a JSON
    array, bin-packing tree payloads across parts.  Flat lists of runs
    (--run-ids / --project) are always written as one file.  When output is
    split, consumers should merge runs across files by id (parts may repeat a
    root's metadata with disjoint child_runs).  Returns the paths written.
    """
    # Flat list of runs (--run-ids / --project): always one file.
    if isinstance(result, list) and not _is_tree_list(result):
        _dump_json(result, out_path)
        return [out_path]

    # Multiple trace trees: JSON array, split so no file exceeds the cap.
    if _is_tree_list(result):
        print(f"Measuring sizes for {len(result)} trace tree(s) …", flush=True)
        payloads = []
        for tree in result:
            payloads.extend(_split_tree_payloads(tree, max_part_bytes))
        sizes = [len(json.dumps(p, indent=2, default=str).encode()) for p in payloads]

        if sum(sizes) <= max_part_bytes:
            _dump_json(payloads, out_path)
            return [out_path]

        groups, current, current_sz = [], [], 0
        for i, sz in enumerate(sizes):
            if current and current_sz + sz > max_part_bytes:
                groups.append(current)
                current, current_sz = [], 0
            current.append(i)
            current_sz += sz
        if current:
            groups.append(current)

        base, ext = os.path.splitext(out_path)
        written = []
        for n, idxs in enumerate(groups, 1):
            path = f"{base}_part{n}{ext}"
            _dump_json([payloads[i] for i in idxs], path)
            written.append(path)
        return written

    # Single trace tree: bare dict, split by child subtree when oversized.
    if isinstance(result, dict) and result.get("child_runs"):
        print(f"Measuring sizes for {len(result['child_runs'])} child subtrees …",
              flush=True)
    payloads = _split_tree_payloads(result, max_part_bytes)
    if len(payloads) == 1:
        _dump_json(result, out_path)
        return [out_path]

    base, ext = os.path.splitext(out_path)
    written = []
    for n, payload in enumerate(payloads, 1):
        path = f"{base}_part{n}{ext}"
        _dump_json(payload, path)
        written.append(path)
    return written


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_ids(raw):
    """
    Split run IDs from one or more raw strings on commas, whitespace, and
    newlines.  Trims blanks and de-duplicates while preserving first-seen
    order.  Accepts a single string or a list of strings (e.g. argparse's
    nargs="+"), so `--run-ids id1,id2 id3` and a comma-pasted UI blob both work.
    """
    if isinstance(raw, str):
        raw = [raw]
    seen, ids = set(), []
    for chunk in raw or []:
        for token in re.split(r"[,\s]+", chunk):
            token = token.strip()
            if token and token not in seen:
                seen.add(token)
                ids.append(token)
    return ids


def main():
    p = argparse.ArgumentParser(description="Export LangSmith runs/traces to JSON")
    p.add_argument("--trace", nargs="+", metavar="ROOT_RUN_ID",
                   help="fetch full trace tree(s) for these root run ID(s); "
                        "IDs may be comma- and/or space-separated")
    p.add_argument("--run-ids", nargs="+",
                   help="fetch individual runs by ID (flat, no children); "
                        "IDs may be comma- and/or space-separated")
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
    p.add_argument("--timeout", type=int, default=_QUERY_TIMEOUT,
                   help=f"per-query network timeout in seconds "
                        f"(default: {_QUERY_TIMEOUT})")
    a = p.parse_args()

    export_dir = os.path.join("exports", a.name)
    os.makedirs(export_dir, exist_ok=True)
    out_path = os.path.join(export_dir, a.out)

    s = session()
    failures = []  # (id, exception) for traces that could not be fetched

    if a.trace:
        trace_ids = parse_ids(a.trace)
        if not trace_ids:
            p.error("--trace given but no valid root run IDs were parsed")
        trees = []
        for i, rid in enumerate(trace_ids):
            if len(trace_ids) > 1:
                print(f"[{i + 1}/{len(trace_ids)}] trace {rid}", flush=True)
            try:
                tree = get_trace(s, rid, timeout=a.timeout)
                if not a.no_resolve_blobs:
                    resolve_blobs(tree)
                trees.append(tree)
            except SystemExit:
                raise  # Cloudflare block etc. — no point continuing the batch
            except Exception as exc:  # isolate one trace's failure from the rest
                failures.append((rid, exc))
                print(f"  ERROR  trace={rid}  {type(exc).__name__}: {exc}",
                      flush=True)
            # Pace the batch to ease API throttling between traces.
            if i < len(trace_ids) - 1:
                time.sleep(_TRACE_PAUSE)
        if not trees:
            sys.exit(f"All {len(trace_ids)} trace(s) failed; nothing written.")
        # A single ID keeps the original single-tree output; multiple IDs
        # produce a list of trees.
        result = trees[0] if len(trees) == 1 else trees
        count = _count_tree(result)
    elif a.run_ids:
        run_ids = parse_ids(a.run_ids)
        if not run_ids:
            p.error("--run-ids given but no valid IDs were parsed")
        result = [get_run(s, rid) for rid in run_ids]
        count = len(result)
    elif a.project:
        result = query_runs(s, project_id(s, a.project), a.limit, a.root_only,
                            timeout=a.timeout)
        count = len(result)
    else:
        p.error("Provide --trace, --run-ids, or --project")

    written = _write_output(result, out_path, int(a.max_mb * 1_000_000))
    parts_label = f"{len(written)} file(s)" if len(written) > 1 else "1 file"
    print(f"Wrote {count} run(s) across {parts_label} in {export_dir}/")

    if failures:
        # Partial success: successful traces are still written above, so exit 0
        # (the UI shows their downloads).  Only a total failure exits non-zero,
        # which is handled earlier.  Re-run the skipped IDs to fill the gaps.
        print(f"\nWARNING: {len(failures)} of {len(trees) + len(failures)} "
              f"trace(s) failed and were skipped:")
        for rid, exc in failures:
            print(f"  {rid}  {type(exc).__name__}: {exc}")


if __name__ == "__main__":
    main()
