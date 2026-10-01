# AGENTS.md — notes for agents working on this project

## What this project is

A CLI tool (`gpu_jobstats.py`) that reports GPU efficiency (compute and memory
utilization, plus CPU/RAM efficiency) for Slurm jobs on the **b200-batch**
partition. It parses an `sacct` listing, fetches per-job statistics via the
[Princeton `jobstats`](https://github.com/PrincetonUniversity/jobstats)
command, stores everything in a **DuckDB** file, and exposes canned reports,
raw SQL, and a **Textual** TUI.

The tool is developed on a Mac where neither `jobstats` nor `sacct` exists;
its real target is a Slurm cluster login node. Everything is tested locally
against `mock_jobstats` (see Testing below).

## Repository layout

| file                  | purpose                                                                 |
|-----------------------|-------------------------------------------------------------------------|
| `gpu_jobstats.py`     | the whole tool — single file, ~800 lines, numbered sections (see below) |
| `sample_jobs.txt`     | **anonymized** input sample (committed): 2,501 sacct rows, 2,477 in b200-batch, Sept 2026, users renamed `userNN_mit` |
| `anonymize_sacct`     | script that produced `sample_jobs.txt` from a real sacct dump (replaces usernames and `CANCELLED by <uid>` uids, re-emits sacct-width lines) |
| `mock_jobstats`       | fake `jobstats` executable for offline testing (deterministic per jobid)|
| `requirements.txt`    | `pandas>=2.0`, `duckdb>=1.0`, `textual>=0.60`                           |
| `README.md`           | user-facing docs (keep in sync with CLI changes)                        |
| `mit_jobs.txt`        | **untracked, gitignored**: the real sacct dump with real usernames. **Never commit it** or copy its usernames into committed files, commit messages, or docs — use `sample_jobs.txt` everywhere. |
| `gpu_jobstats.duckdb` | untracked local database. If built from `mit_jobs.txt` it contains real usernames (also gitignored via `*.duckdb`); verify provenance with the fetch_status query below. |

**Data privacy rule:** everything derived from `mit_jobs.txt` (databases,
exported CSVs in `user_csvs/`) carries real usernames and must stay out of
git — the `.gitignore` covers these; keep it that way. For docs, tests, and
examples always use `sample_jobs.txt`.

## Publishing / remote safety

This repository has a public upstream — resolve it with `git remote -v`
rather than assuming an owner/name. The working tree holds real cluster
data alongside the public code, so treat every remote operation as a
release:

- **Before creating a remote or pushing:** audit what will actually go out
  with `git ls-files` — the directory listing is **not** the tracked set —
  and confirm the guards still hold:
  `git check-ignore -v mit_jobs.txt gpu_jobstats.duckdb` must match both.
- **After any push:** list the remote tree and confirm nothing private
  landed — no `mit_jobs*`, `*.duckdb`, `*.csv`, `user_csvs/` (`gh` fills
  the placeholders from the current repo's remote and branch):
  ```sh
  gh api 'repos/{owner}/{repo}/git/trees/{branch}?recursive=1' --jq '.tree[].path'
  ```
- **Report only verified facts.** Never tell the user a file was tracked,
  committed, or published without checking `git ls-files` /
  `git log --name-status` / the remote tree first. A false "real data
  leaked" report is as costly as a leak: on 2026-10-01 one triggered an
  emergency deletion of the public repo over a file that was never in git.
- Read this file in full **before** repo-level actions (publishing,
  licensing, history rewrites, remote changes) — not after.

This **is a git repository**. Commit messages end
with an `AI Assisted` line — do **not** add `Co-Authored-By:`/"Generated
with" agent attribution lines. There is a `.venv` (gitignored); if missing,
recreate it before running anything:

```sh
python3 -m venv .venv            # local python is 3.14; code requires >=3.10
.venv/bin/pip install -r requirements.txt
```

`gpu_jobstats.py` also carries a PEP 723 inline-dependency header, so
`uv run gpu_jobstats.py ...` works without a venv if `uv` is available.

## Input data format

Input is the output of:

```sh
sacct -S <start> -E <end> -a -X \
  --format=partition%40,user%40,jobid%20,state%30,elapsed,submit,start,end,node
```

(possibly pre-filtered with grep, as the samples were — so there may be no
header line). Parsing gotchas, all handled by `SACCT_LINE_RE` (duplicated as
`LINE_RE` in `anonymize_sacct` — keep the two in sync):

- Fields are space-padded fixed-width, but **do not split on whitespace**:
  `state` can be `CANCELLED by 20966` and `node` can be `None assigned`.
  The regex anchors on the elapsed token (`(?:\d+-)?\d{1,2}:\d{2}:\d{2}`),
  with non-greedy matches for the two multi-word fields.
- Elapsed may have a day prefix: `1-00:00:18`.
- Timestamps are ISO or the literals `None`/`Unknown` (→ parsed as None/NaT).
  They are timezone-naive.
- Array jobs appear as `995660_2` (ran) or `990590_[2%4]` (never started —
  excluded by the "ran" filter, since start is `None`).
- Header/separator lines simply fail the regex and are counted as skipped.
- Duplicate jobids are deduped keeping the last occurrence.

## Architecture of gpu_jobstats.py

The file is organized into numbered comment sections — keep this structure:

1. **LOADING — parse sacct listing**: `SACCT_LINE_RE`, `parse_elapsed`,
   `parse_timestamp`, `parse_sacct_file(path, partition) -> DataFrame`.
2. **LOADING — fetch per-job statistics**: `fetch_jobstats` tries, in order:
   `jobstats --json <id>` → `jobstats -j <id>` →
   `sacct -j <id> -n -X -P -o AdminComment` decoding the `JS1:` gzip(or zlib)
   +base64 JSON blob. All three failing raises with the concatenated errors.
   `fetch_all_jobstats` runs these in a ThreadPoolExecutor.
   `summarize_jobstats` flattens one JSON blob into a per-job summary dict +
   per-GPU rows.
3. **STORAGE — DuckDB**: `build_database` writes tables; `connect` opens
   read-only and exits with a helpful message if the db is missing.
4. **QUERIES**: `QueryFilters` dataclass + `q_users/q_jobs/q_nodes/q_states/
   q_days/q_gpus/q_overall`, all registered in the `REPORTS` dict. Shared by
   the `report` subcommand **and** the TUI — add new views here, not in the TUI.
5. **CLI**: argparse subcommands `load`, `report`, `export`, `sql`, `tui`.
   `export` writes one `<user>.csv` per user into `--outdir` (default
   `user_csvs/`), using the `q_jobs` columns, sorted by `gpu_util_pct`
   descending (`--sort COL`, `--asc` to flip; `--user`/`--state` to filter).
   Usernames are sanitized to `[\w.-]` for filenames.
6. **TUI**: `run_tui(db_path)` defines `JobstatsTUI(App)` inside the function
   so `textual` stays an optional import (`cmd_tui` checks importability and
   points users at `sql` interactive mode as the fallback). The app owns its
   own DuckDB connection (opened in `__init__`, closed in `on_unmount`) —
   this was deliberately refactored for testability; don't move the
   connection back out.

### Assumed jobstats JSON shape (IMPORTANT, unverified against the real cluster)

`summarize_jobstats` expects the Princeton jobstats format:

```json
{"gpus": 4, "total_time": 3600.0,
 "nodes": {"<nodename>": {
    "cpus": 48,
    "total_time": 164480.1,          // CPU-seconds consumed (sum over cores)
    "used_memory": 33898823680,      // RAM bytes
    "total_memory": 536870912000,
    "gpu_utilization":  {"0": 92.4}, // keyed by GPU index as string
    "gpu_used_memory":  {"0": 84542947328},
    "gpu_total_memory": {"0": 85899345920}}}}
```

This shape was inferred from the Princeton jobstats README, **not verified on
the target cluster**. The same is true of the `--json`/`-j` CLI flags (hence
the try-both fallback chain). First task when cluster access is available:
run `jobstats --json <some-jobid>` and the AdminComment decode on a real job,
and adjust `fetch_jobstats`/`summarize_jobstats` if the shape differs. Only
those two functions should need changes.

### Database schema

| object         | contents                                                              |
|----------------|-----------------------------------------------------------------------|
| `jobs`         | one row per sacct job: partition, user, jobid, state, elapsed_s, submit/start/end (TIMESTAMP), node, **fetch_status** (`ok`, `skipped: never ran`, or `error: ...`) |
| `raw_jobstats` | `(jobid TEXT PRIMARY KEY, js TEXT)` — raw JSON cache; makes `load` incremental (re-run fetches only uncached jobs; `--refresh` wipes it) |
| `job_stats`    | per-job summary: gpus, gpu_util_pct (mean/min/max), gpu_mem_pct, gpu_mem_used/total_gb, cpus, cpu_eff_pct, mem_eff_pct, mem_used/total_gb, elapsed_js_s |
| `job_gpus`     | one row per physical GPU per job: node, gpu_index, util_pct, mem_used/total_bytes |
| `v_jobs`       | view: `jobs` ⟕ `job_stats` + computed `gpu_hours`, `wasted_gpu_hours`, `start_day` |

Metric conventions (keep consistent if adding reports):

- `gpu_hours = gpus * elapsed_s / 3600` using **sacct's** elapsed, not jobstats'.
- `wasted_gpu_hours = gpu_hours * (1 - gpu_util_pct/100)` — the headline metric.
- Aggregated `gpu_util_pct`/`gpu_mem_pct` in grouped reports are **weighted by
  GPU-hours** (see `_AGG_COLS`); `cpu_eff_pct`/`mem_eff_pct` are plain means.
- The node report aggregates from `job_gpus` (per physical GPU), so multi-node
  jobs attribute correctly to each node.

## How to run / test (no cluster needed)

```sh
# full pipeline against the anonymized sample input, using the mock jobstats:
.venv/bin/python gpu_jobstats.py load sample_jobs.txt --jobstats-cmd ./mock_jobstats --db test.duckdb --workers 12
.venv/bin/python gpu_jobstats.py report --db test.duckdb --by user
.venv/bin/python gpu_jobstats.py report --db test.duckdb --by job --user user02_mit --sort wasted_gpu_hours --desc --top 5
.venv/bin/python gpu_jobstats.py report --db test.duckdb --by gpu --job 996719
.venv/bin/python gpu_jobstats.py sql --db test.duckdb "SELECT * FROM v_jobs LIMIT 3"
```

Expected with `sample_jobs.txt` + mock: 2,477 jobs in partition, 2,326 "ran",
~94 deterministic fetch failures (the mock fails ~4% of jobids on purpose to
exercise the error path; they land in `fetch_status` as `error: ...`).
A second `load` run should report those 2,232 as cached and refetch only the
94 failures. Check any db's provenance/health with:

```sh
.venv/bin/python gpu_jobstats.py sql --db <db> "SELECT split_part(fetch_status,':',1) s, count(*) FROM jobs GROUP BY 1"
```

`mock_jobstats` accepts `[--json|-j] [-c cluster] <jobid>` and emits
deterministic values seeded by md5(jobid), so test runs are reproducible.

### TUI testing

The TUI can't be run interactively from an agent, but it has been smoke-tested
headlessly with Textual's pilot. Pattern (capture the app by stubbing
`App.run`, then drive it with `app.run_test()`):

```python
import asyncio, sys; sys.argv = ["x"]
import gpu_jobstats
async def main():
    from textual.app import App
    from textual.widgets import DataTable
    captured = {}
    orig = App.run
    App.run = lambda self, *a, **k: captured.__setitem__("app", self)
    try: gpu_jobstats.run_tui("test.duckdb")
    finally: App.run = orig
    app = captured["app"]
    async with app.run_test(size=(160, 40)) as pilot:
        await pilot.press("enter"); await pilot.pause()   # drill user -> jobs
        assert app.view == "job"
        await pilot.press("q")
asyncio.run(main())
```

TUI behavior to preserve: `u/j/n/s/d/g/o` switch views; `/` focuses the filter
input (substring match on user/jobid/state via `QueryFilters.contains`);
`Esc` clears it; header clicks toggle sort; Enter drills user→jobs→per-GPU;
the DataTable is focused on mount (without this, Enter does nothing at start).

## Known caveats / gotchas

- The user's shell is **zsh**: an unquoted `===` in a command (e.g.
  `echo ===`) breaks with "== not found" — quote such separators.
- `report --sort` applies to **output column names** (post-aggregation), and
  errors list the valid columns.
- RUNNING jobs appear with stats-so-far; PENDING / never-started jobs get
  `fetch_status = 'skipped: never ran'` and NULL stats (reports show NaN).
- `--limit N` on `load` caps fetches per run (meant for trial runs).
- `summarize_jobstats` tolerates per-GPU fields being dicts or lists, and
  missing keys; errors there are recorded per-job, never fatal to the load.
- GB values use 1e9 bytes (decimal), not GiB.
- Timestamps are timezone-naive; `start_day` is the calendar date of job start.
- Delete `__pycache__/` if it appears; nothing here is a package.

## Plausible next steps (not committed to)

- Verify/adapt the jobstats JSON shape and CLI flags on the real cluster
  (the one open risk; see above).
- Requested-vs-used GPU count would need `sacct --format=AllocTRES` — the
  current input file doesn't carry it.
- Charts (e.g. util histograms, per-day trends) — if adding any, invoke the
  `dataviz` skill first.
- Packaging (pyproject.toml + console_scripts) if this outgrows one file.
- A `--parquet` export or `COPY (...) TO` convenience for sharing results.
