# gpu_jobstats — GPU efficiency reports for Slurm b200-batch jobs

Reads an `sacct` listing (see `sample_jobs.txt`), fetches per-job GPU/CPU
statistics with the [`jobstats`](https://github.com/PrincetonUniversity/jobstats)
command, stores everything in a DuckDB file, and provides canned reports,
raw SQL, and an interactive TUI.

## Install

Requires Python ≥ 3.10 with `pandas` and `duckdb` (`textual` only for the TUI).

Set up a virtual environment and install the dependencies:

```sh
python3 -m venv .venv
source .venv/bin/activate        # bash/zsh; use .venv/bin/activate.csh for csh/tcsh
pip install -r requirements.txt  # or: pip install pandas duckdb textual
```

Later sessions only need `source .venv/bin/activate` again (or invoke the
interpreter directly as `.venv/bin/python gpu_jobstats.py ...` without
activating).

Alternatively, with [uv](https://docs.astral.sh/uv/) no venv setup is needed —
dependencies are declared inline (PEP 723) and resolved automatically:

```sh
uv run gpu_jobstats.py --help
```

## Input format

The expected input is the output of:

```sh
sacct -S <start> -E <end> -a -X \
  --format=partition%40,user%40,jobid%20,state%30,elapsed,submit,start,end,node
```

optionally pre-filtered with grep. `sample_jobs.txt` is an anonymized example
(real usernames replaced with `userNN_mit` by the `anonymize_sacct` script —
use it to sanitize your own sacct dumps before sharing). Lines are parsed with
a pattern anchored on the elapsed-time column, so multi-word fields such as
`CANCELLED by 20966` and `None assigned` are handled, and header/separator
lines are skipped automatically. Only rows in the requested partition
(default `b200-batch`) are kept.

## 1. Load stage

```sh
./gpu_jobstats.py load my_jobs.txt                  # writes gpu_jobstats.duckdb
./gpu_jobstats.py load my_jobs.txt --workers 16     # more parallel jobstats calls
./gpu_jobstats.py load my_jobs.txt --limit 50       # quick trial run
```

For each job that actually ran, statistics are fetched by trying, in order:

1. `jobstats --json <jobid>`
2. `jobstats -j <jobid>`
3. `sacct -j <jobid> -o AdminComment` — decoding the `JS1:` gzip+base64 blob
   that jobstats stores in Slurm (works even where the jobstats CLI is absent).

Raw JSON responses are cached in the database, so re-running `load` only
fetches new jobs (`--refresh` forces a refetch). Use `--jobstats-cmd` to point
at a wrapper (e.g. `ssh login-node jobstats`) and `--cluster` to pass `-c`.

Database contents:

| object         | contents                                                        |
|----------------|-----------------------------------------------------------------|
| `jobs`         | one row per sacct job (user, state, times, node, fetch_status)  |
| `raw_jobstats` | cached jobstats JSON per job                                    |
| `job_stats`    | per-job summary: GPU util %, GPU mem %, CPU/RAM efficiency, …   |
| `job_gpus`     | one row per physical GPU per job (util %, memory used/total)    |
| `v_jobs`       | view joining `jobs` + `job_stats`, adds `gpu_hours`, `wasted_gpu_hours`, `start_day` |

## 2. Query stage

### Canned reports

```sh
./gpu_jobstats.py report --by user                  # per-user summary (default)
./gpu_jobstats.py report --by overall               # whole-partition totals
./gpu_jobstats.py report --by job --user alice_mit --sort wasted_gpu_hours --desc --top 20
./gpu_jobstats.py report --by node
./gpu_jobstats.py report --by state
./gpu_jobstats.py report --by day
./gpu_jobstats.py report --by gpu --job 1090914     # per-GPU rows for one job
./gpu_jobstats.py report --by user --csv > users.csv
```

Aggregated `gpu_util_pct` / `gpu_mem_pct` are **weighted by GPU-hours**, so a
100 GPU-hour job influences a user's average 100× more than a 1 GPU-hour job.
`wasted_gpu_hours = gpu_hours × (1 − util/100)` — the headline number for
finding idle reservations.

Filters `--user`, `--state` (prefix, e.g. `TIMEOUT`), and `--job` combine with
any grouping; `--sort/--desc/--top/--csv` shape the output.

### Per-user CSV export

Writes one CSV per user (same per-job columns as `report --by job`: state,
hours, gpus, gpu_hours, gpu_util_pct/min/max, gpu_mem_pct, wasted_gpu_hours,
cpu_eff_pct, mem_eff_pct, node, start):

```sh
./gpu_jobstats.py export                        # user_csvs/<user>.csv for every user
./gpu_jobstats.py export --outdir reports/      # choose the directory
./gpu_jobstats.py export --user user01_mit      # just one user
./gpu_jobstats.py export --state COMPLETED      # only completed jobs
./gpu_jobstats.py export --sort hours --asc     # sort rows within each file
```

Rows default to sorting by `gpu_util_pct` descending.

### SQL

```sh
./gpu_jobstats.py sql "SELECT user, count(*) FROM v_jobs WHERE gpu_util_pct < 10 GROUP BY 1"
./gpu_jobstats.py sql        # interactive prompt (.tables, .schema <t>, .quit)
```

### TUI

```sh
./gpu_jobstats.py tui
```

Keys: `u` users · `j` jobs · `n` nodes · `s` states · `d` days · `g` GPUs ·
`o` overall · `/` filter box (substring of user/jobid/state) · `Esc` clear
filter · `q` quit. Click a column header to sort (click again to reverse).
`Enter` on a user row drills into that user's jobs; `Enter` on a job row
drills into its per-GPU breakdown.

## Testing without a cluster

`mock_jobstats` emits deterministic fake jobstats JSON, and `sample_jobs.txt`
is an anonymized real sacct listing, so the whole pipeline can be exercised
anywhere:

```sh
./gpu_jobstats.py load sample_jobs.txt --jobstats-cmd ./mock_jobstats --db test.duckdb
./gpu_jobstats.py report --db test.duckdb --by user
```
