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
import glob
import hashlib
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

_API_RETRIES = 4
_API_BACKOFF = 1.0  # seconds; doubles each retry
_RETRY_STATUS = (502, 503, 504)  # transient gateway errors on heavy queries


def guard(resp):
    if "Just a moment" in resp.text[:500] or resp.status_code == 403:
        sys.exit(
            "Blocked by Cloudflare. Refresh cf_clearance from the browser, "
            "confirm BROWSER_UA matches exactly, and run from the same IP."
        )
    resp.raise_for_status()
    return resp


def _request(method, s, url, **kwargs):
    """
    Issue a request, retrying transient gateway errors (502/503/504) and
    connection failures with exponential backoff. Large trace queries
    intermittently 502 before the backend finishes; a retry succeeds.
    """
    for attempt in range(_API_RETRIES):
        last_attempt = attempt == _API_RETRIES - 1
        try:
            resp = s.request(method, url, **kwargs)
        except requests.exceptions.RequestException:
            if last_attempt:
                raise
            time.sleep(_API_BACKOFF * (2 ** attempt))
            continue
        if resp.status_code in _RETRY_STATUS and not last_attempt:
            print(
                f"  transient HTTP {resp.status_code}; retrying "
                f"({attempt + 1}/{_API_RETRIES - 1})...",
                flush=True,
            )
            time.sleep(_API_BACKOFF * (2 ** attempt))
            continue
        return guard(resp)


def get_run(s, run_id):
    r = _request("GET", s, f"{BASE}/runs/{run_id}", timeout=30)
    return r.json()


def project_id(s, name):
    r = _request("GET", s, f"{BASE}/sessions", params={"name": name}, timeout=30)
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


def _existing_outputs(out_path):
    """Return existing output files for this base: the single file and any _partN."""
    base, ext = os.path.splitext(out_path)
    found = []
    if os.path.exists(out_path):
        found.append(out_path)
    found.extend(sorted(glob.glob(f"{base}_part*{ext}")))
    return found


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
# Slim mode: drop redundant fields + deduplicate repeated messages
# ---------------------------------------------------------------------------
#
# In a LangGraph agent trace, ~95% of the bytes are exact-duplicate messages:
# every LLM call re-sends the growing conversation (huge system prompt + history)
# so the same message objects repeat thousands of times.  --slim:
#   1. drops fields that carry no analytical signal (see sets below),
#   2. pools every unique message once per file and replaces each occurrence
#      with a short reference key, keeping each file self-contained.
# It preserves everything that shows what the agent decided: content, tool_calls,
# invalid_tool_calls, outputs, error, status, extra (graph state).  Reversible
# with --rehydrate.  Content truncation (--max-content-kb) is the only lossy
# lever and is off by default.

# Run-level fields dropped in --slim (redundant once blobs are inlined, or pure
# operational/retention metadata):
_SLIM_DROP_RUN_FIELDS = {
    "inputs_s3_urls", "outputs_s3_urls", "s3_urls",  # pointers to now-inlined blobs
    "serialized",                                    # the runnable's static config
    "app_path",                                      # LangSmith UI deep-link
    "trace_tier", "ttl_seconds", "trace_upgrade", "in_dataset",  # retention/billing
}

# Message-level fields dropped in --slim (provider diagnostics, not decisions):
_SLIM_DROP_MSG_FIELDS = {"response_metadata", "usage_metadata"}

_MSG_REF_PREFIX = "@m:"   # a string with this prefix in a messages list is a pool key
_SLIM_FORMAT = "slim-v1"


def _truncate_content(content, max_bytes):
    """Truncate long text content (lossy). Handles a str or a list of blocks."""
    if isinstance(content, str):
        b = content.encode()
        if len(b) > max_bytes:
            return b[:max_bytes].decode("utf-8", "ignore") + f"…[truncated {len(b) - max_bytes} bytes]"
        return content
    if isinstance(content, list):
        out = []
        for blk in content:
            if isinstance(blk, dict) and isinstance(blk.get("text"), str):
                blk = dict(blk)
                blk["text"] = _truncate_content(blk["text"], max_bytes)
            out.append(blk)
        return out
    return content


def _walk_message_dicts(msgs, fn):
    """Apply fn to every message dict in a (possibly nested) messages list."""
    for m in msgs:
        if isinstance(m, list):
            _walk_message_dicts(m, fn)
        elif isinstance(m, dict):
            fn(m)


def _slim_message_inplace(m, max_bytes):
    target = m.get("kwargs") if isinstance(m.get("kwargs"), dict) else m
    for f in _SLIM_DROP_MSG_FIELDS:
        target.pop(f, None)
    if max_bytes is not None and "content" in target:
        target["content"] = _truncate_content(target["content"], max_bytes)


def _slim_tree_inplace(node, max_bytes):
    """Drop redundant run/message fields (and optionally truncate) in place."""
    if isinstance(node, list):
        for n in node:
            _slim_tree_inplace(n, max_bytes)
        return
    for f in _SLIM_DROP_RUN_FIELDS:
        node.pop(f, None)
    inp = node.get("inputs")
    if isinstance(inp, dict) and isinstance(inp.get("messages"), list):
        _walk_message_dicts(inp["messages"], lambda m: _slim_message_inplace(m, max_bytes))
    for c in node.get("child_runs", []) or []:
        _slim_tree_inplace(c, max_bytes)


def _msg_key(m):
    h = hashlib.sha1(json.dumps(m, sort_keys=True, default=str).encode()).hexdigest()
    return _MSG_REF_PREFIX + h[:16]


def _dedup_messages_inplace(msgs, pool):
    for i, m in enumerate(msgs):
        if isinstance(m, list):
            _dedup_messages_inplace(m, pool)
        elif isinstance(m, dict):
            k = _msg_key(m)
            pool.setdefault(k, m)
            msgs[i] = k


def _dedup_tree_inplace(node, pool):
    """Replace every message dict with a pool key; collect the pool in place."""
    if isinstance(node, list):
        for n in node:
            _dedup_tree_inplace(n, pool)
        return
    inp = node.get("inputs")
    if isinstance(inp, dict) and isinstance(inp.get("messages"), list):
        _dedup_messages_inplace(inp["messages"], pool)
    for c in node.get("child_runs", []) or []:
        _dedup_tree_inplace(c, pool)


def _collect_keys(msgs, acc):
    if isinstance(msgs, list):
        for m in msgs:
            if isinstance(m, list):
                _collect_keys(m, acc)
            elif isinstance(m, str) and m.startswith(_MSG_REF_PREFIX):
                acc.add(m)


def _keys_in(node, acc):
    inp = node.get("inputs")
    if isinstance(inp, dict):
        _collect_keys(inp.get("messages"), acc)
    for c in node.get("child_runs", []) or []:
        _keys_in(c, acc)


def _slim_doc(root_meta, child_runs, keys, pool):
    """Assemble one self-contained slim document with its own message pool."""
    doc = {k: v for k, v in root_meta.items() if k != "child_runs"}
    doc["_format"] = _SLIM_FORMAT
    doc["_messages"] = {k: pool[k] for k in keys}
    if child_runs is not None:
        doc["child_runs"] = child_runs
    return doc


def _write_output_slim(result, out_path, max_part_bytes, max_content_bytes):
    """Slim + dedup, then write self-contained files each under the size cap."""
    for stale in _existing_outputs(out_path):
        os.remove(stale)

    print("Slimming runs and deduplicating messages …", flush=True)
    _slim_tree_inplace(result, max_content_bytes)
    pool = {}
    _dedup_tree_inplace(result, pool)
    msg_sizes = {k: len(json.dumps(v, indent=2, default=str).encode()) for k, v in pool.items()}
    print(f"  {len(pool)} unique messages pooled "
          f"({sum(msg_sizes.values()) / 1e6:.1f} MB)", flush=True)

    # Flat list (run-ids / project) or single node — one self-contained file.
    if not isinstance(result, dict) or not result.get("child_runs"):
        if isinstance(result, list):
            keys = set()
            for r in result:
                _keys_in(r, keys)
            doc = {"_format": _SLIM_FORMAT,
                   "_messages": {k: pool[k] for k in keys},
                   "runs": result}
        else:
            keys = set()
            _keys_in(result, keys)
            doc = _slim_doc(result, result.get("child_runs"), keys, pool)
        with open(out_path, "w") as f:
            json.dump(doc, f, indent=2, default=str)
        size = os.path.getsize(out_path)
        over = "  ** OVER CAP **" if size > max_part_bytes else ""
        print(f"  {out_path}  ({size / 1e6:.1f} MB){over}")
        return [out_path]

    root_meta = {k: v for k, v in result.items() if k != "child_runs"}
    children = result["child_runs"]

    # The root's own messages must live in every part's pool (root_meta is repeated).
    root_keys = set()
    _keys_in({k: v for k, v in result.items() if k != "child_runs"}, root_keys)
    root_overhead = (len(json.dumps(root_meta, indent=2, default=str).encode())
                     + sum(msg_sizes[k] for k in root_keys))

    print(f"Measuring {len(children)} slimmed child subtrees …", flush=True)
    child_info = []
    for c in children:
        ks = set()
        _keys_in(c, ks)
        cbytes = len(json.dumps(c, indent=2, default=str).encode())
        child_info.append((cbytes, ks))

    # Greedy pack: a part's size ≈ root overhead + child bytes + the NEW message
    # bytes each child adds to that part's pool (a message shared across children
    # in the same file is stored once).
    parts = []
    cur_idx, cur_keys, cur_sz = [], set(root_keys), root_overhead
    for i, (cbytes, ks) in enumerate(child_info):
        new = ks - cur_keys
        add = cbytes + sum(msg_sizes[k] for k in new)
        if cur_idx and cur_sz + add > max_part_bytes:
            parts.append((cur_idx, cur_keys))
            cur_idx, cur_keys, cur_sz = [], set(root_keys), root_overhead
            new = ks - cur_keys
            add = cbytes + sum(msg_sizes[k] for k in new)
        cur_idx.append(i)
        cur_keys |= new
        cur_sz += add
    if cur_idx:
        parts.append((cur_idx, cur_keys))

    base, ext = os.path.splitext(out_path)
    single = len(parts) == 1
    written = []
    for n, (indices, keys) in enumerate(parts, 1):
        path = out_path if single else f"{base}_part{n}{ext}"
        doc = _slim_doc(root_meta, [children[i] for i in indices], keys, pool)
        with open(path, "w") as f:
            json.dump(doc, f, indent=2, default=str)
        size = os.path.getsize(path)
        over = "  ** OVER CAP (single run subtree exceeds limit) **" if size > max_part_bytes else ""
        print(f"  {path}  ({size / 1e6:.1f} MB){over}")
        written.append(path)

    return written


def _rehydrate_doc(doc):
    """Expand a slim doc's message refs back inline using its own pool."""
    pool = doc.get("_messages", {})

    def expand(msgs):
        for i, m in enumerate(msgs):
            if isinstance(m, list):
                expand(m)
            elif isinstance(m, str) and m.startswith(_MSG_REF_PREFIX):
                msgs[i] = pool.get(m, m)

    def walk(node):
        inp = node.get("inputs")
        if isinstance(inp, dict) and isinstance(inp.get("messages"), list):
            expand(inp["messages"])
        for c in node.get("child_runs", []) or []:
            walk(c)

    if isinstance(doc.get("runs"), list):
        for r in doc["runs"]:
            walk(r)
    else:
        walk(doc)
    return {k: v for k, v in doc.items() if k not in ("_messages", "_format")}


def rehydrate_path(path):
    """Expand slim file(s) back to full form. Accepts a file or a directory."""
    if os.path.isdir(path):
        files = sorted(glob.glob(os.path.join(path, "*.json")))
    else:
        files = [path]
    done = 0
    for f in files:
        if f.endswith(".full.json"):
            continue
        try:
            doc = json.load(open(f))
        except (ValueError, OSError) as exc:
            print(f"  skip {f} ({exc})")
            continue
        if not (isinstance(doc, dict) and doc.get("_format") == _SLIM_FORMAT):
            print(f"  skip {os.path.basename(f)} (not slim-v1)")
            continue
        out = f[:-5] + ".full.json"
        with open(out, "w") as fh:
            json.dump(_rehydrate_doc(doc), fh, indent=2, default=str)
        print(f"  {out}  ({os.path.getsize(out) / 1e6:.1f} MB)")
        done += 1
    if not done:
        sys.exit("No slim-v1 files found to rehydrate.")


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
    p.add_argument("--name",
                   help="export name; files are written to exports/<name>/ (required "
                        "unless --rehydrate)")
    p.add_argument("--out", default="runs_export.json",
                   help="filename inside the export folder (default: runs_export.json)")
    p.add_argument("--max-mb", type=float, default=_DEFAULT_MAX_MB,
                   help=f"max size per output file in MB (default: {_DEFAULT_MAX_MB})")
    p.add_argument("--timeout", type=int, default=_QUERY_TIMEOUT,
                   help=f"per-query network timeout in seconds "
                        f"(default: {_QUERY_TIMEOUT})")
    p.add_argument("--force", action="store_true",
                   help="overwrite an existing export in exports/<name>/ (default: refuse)")
    p.add_argument("--slim", action="store_true",
                   help="drop redundant fields and dedup repeated messages (lossless; "
                        "see --rehydrate to expand back)")
    p.add_argument("--max-content-kb", type=int, default=None, metavar="N",
                   help="with --slim, truncate any single message content block over N KB "
                        "(LOSSY; off by default)")
    p.add_argument("--rehydrate", metavar="PATH",
                   help="expand slim file(s) back to full JSON (a file or a directory); "
                        "no fetching, ignores all other options")
    a = p.parse_args()

    if a.rehydrate:
        rehydrate_path(a.rehydrate)
        return
    if not a.name:
        p.error("--name is required (unless using --rehydrate)")

    export_dir = os.path.join("exports", a.name)
    os.makedirs(export_dir, exist_ok=True)
    out_path = os.path.join(export_dir, a.out)

    # Fail fast, before any (potentially huge) download, if this name already
    # holds an export — reusing a name silently overwrote/mixed traces before.
    existing = _existing_outputs(out_path)
    if existing and not a.force:
        listing = "\n".join(f"  {os.path.basename(f)}" for f in existing)
        sys.exit(
            f"{export_dir}/ already contains an export ({len(existing)} file(s)):\n"
            f"{listing}\n"
            f"Use a different --name, or pass --force to overwrite it."
        )

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

    max_part_bytes = int(a.max_mb * 1_000_000)
    if a.slim:
        max_bytes = a.max_content_kb * 1000 if a.max_content_kb else None
        written = _write_output_slim(result, out_path, max_part_bytes, max_bytes)
    else:
        written = _write_output(result, out_path, max_part_bytes)
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
