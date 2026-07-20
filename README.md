# langsmith-trace-analysis

Export full LangSmith traces—including S3-offloaded inputs/outputs—to local JSON for offline analysis.

The target LangSmith instance (`langsmith.blitzy.com`) sits behind a Cloudflare managed challenge, so exports must run from the same machine and browser session used to access the UI.

## Features

- Fetches an entire trace tree by root run ID (all child runs, fully nested)
- Resolves S3-offloaded payloads concurrently so `inputs`/`outputs`/`error` fields are always populated
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

Opens at **http://localhost:8501**. The UI lets you choose an export mode (Trace tree / Run IDs / Project), set the export name and per-file size limit, and download results directly from the browser.

### CLI

#### Export a full trace

```bash
python src/export.py --name <label> --trace <root-run-id>
```

Output is written to `exports/<label>/runs_export.json` (or `_part1.json`, `_part2.json`, … if the trace exceeds the size cap).

#### Control file size

```bash
python src/export.py --name <label> --trace <root-run-id> --max-mb 100
```

Default is 30 MB per file.

#### Skip S3 blob resolution (faster, skeleton only)

```bash
python src/export.py --name <label> --trace <root-run-id> --no-resolve-blobs
```

#### Export specific runs by ID

```bash
python src/export.py --name <label> --run-ids <id1> <id2> ...
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

Each file contains the root run's metadata with a `child_runs` array populated recursively. All `inputs`, `outputs`, `error`, and `messages` fields are resolved inline.

## Cloudflare troubleshooting

If the script exits with *"Blocked by Cloudflare"*:

1. Open `langsmith.blitzy.com` in your browser on this machine
2. Complete any challenge presented
3. Copy the fresh `cf_clearance` cookie value into `.env`
4. Confirm `BROWSER_UA` matches the browser's exact user-agent string
