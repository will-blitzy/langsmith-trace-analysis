# langsmith-trace-analysis

Export full LangSmith traces—including S3-offloaded inputs/outputs—to local JSON for offline analysis.

The target LangSmith instance (`langsmith.blitzy.com`) sits behind a Cloudflare managed challenge, so exports must run from the same machine and browser session used to access the UI.

## Features

- Fetches an entire trace tree by root run ID (all child runs, fully nested)
- Accepts **multiple root run IDs at once** (comma- or space-separated) — handy for re-exporting a batch of errored-out runs; one slow/failed trace is skipped without aborting the rest
- Resolves S3-offloaded payloads concurrently so `inputs`/`outputs`/`error` fields are always populated
- Retries transient query failures (timeouts, 502/503/504) and paces batches to avoid API throttling
- Auto-splits large exports into `_part1`, `_part2`, … files at a configurable size (default 30 MB)
- Organises output under `exports/<name>/` for easy management
- Simple web UI for running exports and downloading results

## Requirements

- Python 3.9+
- pip packages: `requests`, `python-dotenv`, `streamlit`

## Setup

### 1. Clone and install

```bash
git clone git@github.com:will-blitzy/langsmith-trace-analysis.git
cd langsmith-trace-analysis
pip install -r requirements.txt
```

### 2. Configure credentials

```bash
cp .env.example .env
```

Open `.env` and fill in the three values:

| Variable | Where to find it |
|---|---|
| `LANGSMITH_API_KEY` | LangSmith UI → Settings → API Keys |
| `CF_CLEARANCE` | Browser DevTools → Application → Cookies → `langsmith.blitzy.com` → `cf_clearance` |
| `BROWSER_UA` | DevTools → Network → any request → Request Headers → `user-agent` |

`CF_CLEARANCE` and `BROWSER_UA` must come from the **same browser session** on the **same machine** you run the script from. The clearance cookie expires periodically — refresh it from the browser if you get a Cloudflare block error.

## Usage

### Web UI (recommended)

```bash
./run.sh
```

Opens at **http://localhost:8501**. The UI lets you choose an export mode (Trace tree / Run IDs / Project), set the export name and per-file size limit, and download results directly from the browser. In both **Trace tree** and **Run IDs** modes you can enter several IDs at once, separated by commas, spaces, or newlines.

### CLI

#### Export a full trace

```bash
python src/export.py --name <label> --trace <root-run-id>
```

Output is written to `exports/<label>/runs_export.json` (or `_part1.json`, `_part2.json`, … if the trace exceeds the size cap).

#### Export several traces at once

Pass multiple root run IDs, comma- and/or space-separated:

```bash
python src/export.py --name <label> --trace <root-id-1>,<root-id-2>,<root-id-3>
python src/export.py --name <label> --trace <root-id-1> <root-id-2>
```

Each trace is fetched independently. If one fails (e.g. a transient gateway error or timeout), it is retried, then — if still failing — logged and skipped so the others still export; a `WARNING: N of M trace(s) failed` summary lists any skipped IDs at the end. Re-run just those IDs to fill the gaps.

With more than one ID, the output is a **JSON array of trace trees** rather than a single root object (see [Output structure](#output-structure)).

#### Control file size

```bash
python src/export.py --name <label> --trace <root-run-id> --max-mb 100
```

Default is 30 MB per file. Splitting happens at the granularity of a root's direct-child subtrees, so a single subtree larger than the cap is written whole in its own file (that one file may exceed `--max-mb`).

#### Adjust the network timeout

```bash
python src/export.py --name <label> --trace <root-run-id> --timeout 240
```

Per-query network timeout in seconds (default 120). Increase it for very large traces that are slow to query.

#### Skip S3 blob resolution (faster, skeleton only)

```bash
python src/export.py --name <label> --trace <root-run-id> --no-resolve-blobs
```

#### Export specific runs by ID

IDs may be comma- and/or space-separated (paste a list of errored run IDs directly):

```bash
python src/export.py --name <label> --run-ids <id1> <id2> ...
python src/export.py --name <label> --run-ids <id1>,<id2>,<id3>
```

#### Export recent runs from a project

```bash
python src/export.py --name <label> --project "my-project" --limit 50 --root-only
```

#### Custom output filename

```bash
python src/export.py --name <label> --trace <root-run-id> --out my_trace.json
```

## Output structure

```
exports/
└── <label>/
    ├── runs_export_part1.json   # root run + slice of child_runs (nested tree)
    ├── runs_export_part2.json
    └── ...
```

For a **single** `--trace` ID, each file contains the root run's metadata with a `child_runs` array populated recursively. For **multiple** `--trace` IDs, each file is a **JSON array** of such trace-tree objects. All `inputs`, `outputs`, `error`, and `messages` fields are resolved inline.

When an export is split across parts, reassemble it by merging runs by `id`: a large tree's root metadata may repeat across parts with disjoint `child_runs`, so a consumer should union the children of any runs that share an `id`.

## Cloudflare troubleshooting

If the script exits with *"Blocked by Cloudflare"*:

1. Open `langsmith.blitzy.com` in your browser on this machine
2. Complete any challenge presented
3. Copy the fresh `cf_clearance` cookie value into `.env`
4. Confirm `BROWSER_UA` matches the browser's exact user-agent string
