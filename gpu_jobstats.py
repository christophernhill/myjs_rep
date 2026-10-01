#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["pandas>=2.0", "duckdb>=1.0", "textual>=0.60"]
# ///
"""
gpu_jobstats.py - GPU efficiency reporting for Slurm jobs on the b200-batch partition.

The tool has two stages:

  1. LOAD:  parse an sacct listing (e.g. sample_jobs.txt), keep only jobs in the
            requested partition, fetch per-job GPU/CPU statistics with the
            `jobstats` command (falling back to decoding the JS1 blob stored in
            sacct's AdminComment field), and persist everything into a DuckDB
            database file.

  2. QUERY: canned reports grouped by user / job / node / state / day / gpu,
            arbitrary SQL (batch or interactive), and an optional Textual TUI
            for interactive exploration.

Examples:
    ./gpu_jobstats.py load sample_jobs.txt
    ./gpu_jobstats.py report --by user
    ./gpu_jobstats.py report --by job --user user02_mit --sort gpu_hours --desc
    ./gpu_jobstats.py sql "SELECT * FROM v_jobs LIMIT 5"
    ./gpu_jobstats.py sql            # interactive SQL prompt
    ./gpu_jobstats.py tui            # interactive TUI (requires `textual`)
"""

from __future__ import annotations

import argparse
import base64
import concurrent.futures
import gzip
import json
import re
import subprocess
import sys
import zlib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

DEFAULT_DB = "gpu_jobstats.duckdb"
DEFAULT_PARTITION = "b200-batch"

# ---------------------------------------------------------------------------
# 1. LOADING - parse sacct listing
# ---------------------------------------------------------------------------

# One sacct -X row:  partition user jobid state elapsed submit start end node
# `state` ("CANCELLED by 20966") and `node` ("None assigned") may contain
# spaces, so anchor on the elapsed [d-]hh:mm:ss token instead of splitting.
SACCT_LINE_RE = re.compile(
    r"""^\s*
        (?P<partition>\S+)\s+
        (?P<user>\S+)\s+
        (?P<jobid>\S+)\s+
        (?P<state>\S.*?)\s+
        (?P<elapsed>(?:\d+-)?\d{1,2}:\d{2}:\d{2}(?:\.\d+)?)\s+
        (?P<submit>\S+)\s+
        (?P<start>\S+)\s+
        (?P<end>\S+)\s+
        (?P<node>\S.*?)\s*$
    """,
    re.VERBOSE,
)


def parse_elapsed(text: str) -> int:
    """'1-00:00:18' or '15:31:06' -> seconds."""
    days = 0
    if "-" in text:
        d, text = text.split("-", 1)
        days = int(d)
    parts = [float(p) for p in text.split(":")]
    while len(parts) < 3:
        parts.insert(0, 0.0)
    h, m, s = parts
    return int(days * 86400 + h * 3600 + m * 60 + s)


def parse_timestamp(text: str) -> datetime | None:
    if text in ("None", "Unknown", "N/A", ""):
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def parse_sacct_file(path: str, partition: str) -> pd.DataFrame:
    """Parse an sacct listing into a DataFrame, keeping only `partition` rows."""
    rows, skipped = [], 0
    with open(path) as fh:
        for line in fh:
            if not line.strip():
                continue
            m = SACCT_LINE_RE.match(line)
            if m is None:
                skipped += 1  # header / separator / malformed lines
                continue
            d = m.groupdict()
            if d["partition"] != partition:
                continue
            rows.append(
                {
                    "partition": d["partition"],
                    "user": d["user"],
                    "jobid": d["jobid"],
                    "state": d["state"],
                    "elapsed_s": parse_elapsed(d["elapsed"]),
                    "submit": parse_timestamp(d["submit"]),
                    "start": parse_timestamp(d["start"]),
                    "end": parse_timestamp(d["end"]),
                    "node": d["node"],
                }
            )
    if skipped:
        print(f"note: skipped {skipped} unparseable line(s) in {path}", file=sys.stderr)
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.drop_duplicates(subset="jobid", keep="last")
    return df


# ---------------------------------------------------------------------------
# 1b. LOADING - fetch per-job statistics
# ---------------------------------------------------------------------------


def _decode_js1(blob: str) -> dict:
    """Decode a 'JS1:<base64 gzip json>' AdminComment blob."""
    payload = base64.b64decode(blob.split(":", 1)[1])
    try:
        raw = gzip.decompress(payload)
    except OSError:
        raw = zlib.decompress(payload)
    return json.loads(raw)


def _json_from_stdout(stdout: str) -> dict:
    start = stdout.find("{")
    if start < 0:
        raise ValueError("no JSON object in output")
    return json.loads(stdout[start:])


def fetch_jobstats(jobid: str, jobstats_cmd: str, cluster: str | None, timeout: int) -> dict:
    """Return the jobstats JSON for one job.

    Tries `jobstats --json`, then `jobstats -j`, then decoding the JS1 blob
    from sacct's AdminComment field (which holds the same JSON and works even
    where the jobstats CLI is not installed).
    """
    errors = []
    for flag in ("--json", "-j"):
        argv = [jobstats_cmd, flag, jobid]
        if cluster:
            argv += ["-c", cluster]
        try:
            proc = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
            if proc.returncode == 0:
                return _json_from_stdout(proc.stdout)
            errors.append(f"{flag}: rc={proc.returncode} {proc.stderr.strip()[:120]}")
        except (OSError, subprocess.TimeoutExpired, ValueError, json.JSONDecodeError) as exc:
            errors.append(f"{flag}: {exc}")

    try:
        proc = subprocess.run(
            ["sacct", "-j", jobid, "-n", "-X", "-P", "-o", "AdminComment"],
            capture_output=True, text=True, timeout=timeout,
        )
        blob = proc.stdout.strip().splitlines()[0].strip() if proc.stdout.strip() else ""
        if blob.startswith("JS1:"):
            return _decode_js1(blob)
        errors.append("AdminComment: no JS1 data")
    except (OSError, subprocess.TimeoutExpired, IndexError, ValueError) as exc:
        errors.append(f"AdminComment: {exc}")

    raise RuntimeError("; ".join(errors))


def _as_indexed(value) -> dict:
    """jobstats per-GPU fields are dicts keyed by GPU index; tolerate lists."""
    if isinstance(value, dict):
        return value
    if isinstance(value, (list, tuple)):
        return {str(i): v for i, v in enumerate(value)}
    return {}


def summarize_jobstats(jobid: str, js: dict) -> tuple[dict, list[dict]]:
    """Reduce one jobstats JSON blob to a per-job summary row + per-GPU rows.

    jobstats JSON shape (one entry per node):
      {"gpus": 4, "total_time": <elapsed s>,
       "nodes": {"node1": {"cpus": 48, "total_time": <cpu-seconds used>,
                            "used_memory": b, "total_memory": b,
                            "gpu_utilization": {"0": 92.4, ...},
                            "gpu_used_memory": {"0": b, ...},
                            "gpu_total_memory": {"0": b, ...}}}}
    """
    nodes = js.get("nodes", {}) or {}
    elapsed = float(js.get("total_time") or 0)

    gpu_rows = []
    cpus = cpu_time = mem_used = mem_total = 0.0
    for node, nd in nodes.items():
        cpus += float(nd.get("cpus") or 0)
        cpu_time += float(nd.get("total_time") or 0)
        mem_used += float(nd.get("used_memory") or 0)
        mem_total += float(nd.get("total_memory") or 0)
        util = _as_indexed(nd.get("gpu_utilization"))
        gused = _as_indexed(nd.get("gpu_used_memory"))
        gtotal = _as_indexed(nd.get("gpu_total_memory"))
        for idx in util:
            gpu_rows.append(
                {
                    "jobid": jobid,
                    "node": node,
                    "gpu_index": str(idx),
                    "util_pct": float(util[idx]),
                    "mem_used_bytes": float(gused.get(idx) or 0),
                    "mem_total_bytes": float(gtotal.get(idx) or 0),
                }
            )

    n_gpus = int(js.get("gpus") or len(gpu_rows))
    utils = [g["util_pct"] for g in gpu_rows]
    gmem_used = sum(g["mem_used_bytes"] for g in gpu_rows)
    gmem_total = sum(g["mem_total_bytes"] for g in gpu_rows)

    summary = {
        "jobid": jobid,
        "gpus": n_gpus,
        "gpu_util_pct": sum(utils) / len(utils) if utils else None,
        "gpu_util_min": min(utils) if utils else None,
        "gpu_util_max": max(utils) if utils else None,
        "gpu_mem_pct": 100.0 * gmem_used / gmem_total if gmem_total else None,
        "gpu_mem_used_gb": gmem_used / 1e9,
        "gpu_mem_total_gb": gmem_total / 1e9,
        "cpus": int(cpus),
        "cpu_eff_pct": 100.0 * cpu_time / (cpus * elapsed) if cpus and elapsed else None,
        "mem_eff_pct": 100.0 * mem_used / mem_total if mem_total else None,
        "mem_used_gb": mem_used / 1e9,
        "mem_total_gb": mem_total / 1e9,
        "elapsed_js_s": elapsed,
    }
    return summary, gpu_rows


def fetch_all_jobstats(
    jobids: list[str], jobstats_cmd: str, cluster: str | None, workers: int, timeout: int
) -> dict[str, tuple[str, dict | None]]:
    """Fetch jobstats JSON for many jobs in parallel.

    Returns {jobid: (status, json_or_None)} where status is 'ok' or 'error: ...'.
    """
    results: dict[str, tuple[str, dict | None]] = {}
    if not jobids:
        return results
    done = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(fetch_jobstats, j, jobstats_cmd, cluster, timeout): j for j in jobids
        }
        for fut in concurrent.futures.as_completed(futures):
            jobid = futures[fut]
            try:
                results[jobid] = ("ok", fut.result())
            except Exception as exc:
                results[jobid] = (f"error: {exc}"[:500], None)
            done += 1
            if done % 50 == 0 or done == len(jobids):
                print(f"\r  fetched {done}/{len(jobids)} jobs", end="", file=sys.stderr)
    print(file=sys.stderr)
    return results


# ---------------------------------------------------------------------------
# 2. STORAGE - DuckDB
# ---------------------------------------------------------------------------


def build_database(
    db_path: str,
    jobs: pd.DataFrame,
    jobstats_cmd: str,
    cluster: str | None,
    workers: int,
    timeout: int,
    refresh: bool,
    limit: int | None,
) -> None:
    con = duckdb.connect(db_path)
    con.execute("CREATE TABLE IF NOT EXISTS raw_jobstats (jobid TEXT PRIMARY KEY, js TEXT)")

    # Jobs worth querying: they actually ran on a node.
    ran = jobs[(jobs["elapsed_s"] > 0) & jobs["start"].notna() & (jobs["node"] != "None assigned")]
    wanted = list(ran["jobid"])
    if refresh:
        con.execute("DELETE FROM raw_jobstats")
        cached: set[str] = set()
    else:
        cached = {r[0] for r in con.execute("SELECT jobid FROM raw_jobstats").fetchall()}
    to_fetch = [j for j in wanted if j not in cached]
    if limit is not None:
        to_fetch = to_fetch[:limit]

    print(
        f"{len(jobs)} jobs in partition, {len(wanted)} ran, "
        f"{len(cached)} cached, fetching {len(to_fetch)} via {jobstats_cmd!r}",
        file=sys.stderr,
    )
    fetched = fetch_all_jobstats(to_fetch, jobstats_cmd, cluster, workers, timeout)

    errors: dict[str, str] = {}
    for jobid, (status, js) in fetched.items():
        if js is not None:
            con.execute(
                "INSERT OR REPLACE INTO raw_jobstats VALUES (?, ?)", [jobid, json.dumps(js)]
            )
        else:
            errors[jobid] = status
    if errors:
        print(f"  {len(errors)} job(s) had no jobstats data "
              f"(e.g. {next(iter(errors.items()))})", file=sys.stderr)

    # Summarize every cached blob (old + new) into flat tables.
    summaries, gpu_rows = [], []
    for jobid, js_text in con.execute("SELECT jobid, js FROM raw_jobstats").fetchall():
        try:
            s, g = summarize_jobstats(jobid, json.loads(js_text))
            summaries.append(s)
            gpu_rows.extend(g)
        except Exception as exc:
            errors[jobid] = f"error: summarize failed: {exc}"

    jobs = jobs.copy()
    jobs["fetch_status"] = [
        errors.get(j, "ok" if j in cached or j in fetched else "skipped: never ran")
        for j in jobs["jobid"]
    ]

    stats_df = pd.DataFrame(summaries) if summaries else pd.DataFrame(
        columns=["jobid", "gpus", "gpu_util_pct", "gpu_util_min", "gpu_util_max",
                 "gpu_mem_pct", "gpu_mem_used_gb", "gpu_mem_total_gb", "cpus",
                 "cpu_eff_pct", "mem_eff_pct", "mem_used_gb", "mem_total_gb",
                 "elapsed_js_s"])
    gpu_df = pd.DataFrame(gpu_rows) if gpu_rows else pd.DataFrame(
        columns=["jobid", "node", "gpu_index", "util_pct", "mem_used_bytes", "mem_total_bytes"])

    con.register("jobs_df", jobs)
    con.register("stats_df", stats_df)
    con.register("gpu_df", gpu_df)
    con.execute("CREATE OR REPLACE TABLE jobs AS SELECT * FROM jobs_df")
    con.execute("CREATE OR REPLACE TABLE job_stats AS SELECT * FROM stats_df")
    con.execute("CREATE OR REPLACE TABLE job_gpus AS SELECT * FROM gpu_df")
    con.execute(
        """
        CREATE OR REPLACE VIEW v_jobs AS
        SELECT j.*, s.gpus, s.gpu_util_pct, s.gpu_util_min, s.gpu_util_max,
               s.gpu_mem_pct, s.gpu_mem_used_gb, s.gpu_mem_total_gb,
               s.cpus, s.cpu_eff_pct, s.mem_eff_pct, s.mem_used_gb, s.mem_total_gb,
               s.gpus * j.elapsed_s / 3600.0                             AS gpu_hours,
               s.gpus * j.elapsed_s / 3600.0 * (1 - s.gpu_util_pct/100)  AS wasted_gpu_hours,
               CAST(j.start AS DATE)                                     AS start_day
        FROM jobs j LEFT JOIN job_stats s USING (jobid)
        """
    )
    n_ok = con.execute("SELECT count(*) FROM job_stats").fetchone()[0]
    con.close()
    print(f"wrote {db_path}: {len(jobs)} jobs, GPU stats for {n_ok}", file=sys.stderr)


def connect(db_path: str) -> duckdb.DuckDBPyConnection:
    try:
        return duckdb.connect(db_path, read_only=True)
    except duckdb.Error as exc:
        sys.exit(f"cannot open {db_path!r} ({exc}) - run the `load` subcommand first")


# ---------------------------------------------------------------------------
# 3. QUERIES - shared by `report` and the TUI
# ---------------------------------------------------------------------------

_AGG_COLS = """
    count(*)                                                        AS jobs,
    count(gpu_util_pct)                                             AS jobs_with_stats,
    round(sum(gpu_hours), 1)                                        AS gpu_hours,
    round(sum(gpu_hours * gpu_util_pct/100) / nullif(sum(gpu_hours), 0) * 100, 1)
                                                                    AS gpu_util_pct,
    round(sum(gpu_hours * gpu_mem_pct/100) / nullif(sum(gpu_hours), 0) * 100, 1)
                                                                    AS gpu_mem_pct,
    round(sum(wasted_gpu_hours), 1)                                 AS wasted_gpu_hours,
    round(avg(cpu_eff_pct), 1)                                      AS cpu_eff_pct,
    round(avg(mem_eff_pct), 1)                                      AS mem_eff_pct
"""
# gpu_util_pct / gpu_mem_pct above are weighted by each job's GPU-hours, so a
# 100 GPU-hour job counts 100x more than a 1 GPU-hour job.


@dataclass
class QueryFilters:
    user: str | None = None
    state: str | None = None
    jobid: str | None = None
    contains: str | None = None  # substring across user/jobid, used by TUI filter box


def _where(f: QueryFilters) -> tuple[str, list]:
    conds, params = [], []
    if f.user:
        conds.append("user = ?")
        params.append(f.user)
    if f.state:
        conds.append("state ILIKE ?")
        params.append(f"{f.state}%")
    if f.jobid:
        conds.append("jobid = ?")
        params.append(f.jobid)
    if f.contains:
        conds.append("(user ILIKE ? OR jobid ILIKE ? OR state ILIKE ?)")
        params += [f"%{f.contains}%"] * 3
    return ("WHERE " + " AND ".join(conds)) if conds else "", params


def _grouped(con, group_expr: str, alias: str, f: QueryFilters) -> pd.DataFrame:
    where, params = _where(f)
    return con.execute(
        f"SELECT {group_expr} AS {alias}, {_AGG_COLS} FROM v_jobs {where} "
        f"GROUP BY 1 ORDER BY gpu_hours DESC NULLS LAST",
        params,
    ).df()


def q_overall(con, f: QueryFilters) -> pd.DataFrame:
    return _grouped(con, "'all jobs'", "scope", f)


def q_users(con, f: QueryFilters) -> pd.DataFrame:
    return _grouped(con, "user", "user", f)


def q_states(con, f: QueryFilters) -> pd.DataFrame:
    return _grouped(con, "split_part(state, ' ', 1)", "state", f)


def q_days(con, f: QueryFilters) -> pd.DataFrame:
    df = _grouped(con, "start_day", "day", f)
    return df.sort_values("day", na_position="last").reset_index(drop=True)


def q_nodes(con, f: QueryFilters) -> pd.DataFrame:
    """Per physical node, from the per-GPU table (handles multi-node jobs)."""
    where, params = _where(f)
    return con.execute(
        f"""
        SELECT g.node,
               count(DISTINCT g.jobid)                          AS jobs,
               count(*)                                         AS gpu_slots,
               round(sum(j.elapsed_s) / 3600.0, 1)              AS gpu_hours,
               round(sum(j.elapsed_s * g.util_pct/100) / nullif(sum(j.elapsed_s),0) * 100, 1)
                                                                AS gpu_util_pct,
               round(sum(j.elapsed_s * g.mem_used_bytes/nullif(g.mem_total_bytes,0))
                     / nullif(sum(j.elapsed_s),0) * 100, 1)     AS gpu_mem_pct
        FROM job_gpus g JOIN v_jobs j USING (jobid) {where}
        GROUP BY 1 ORDER BY gpu_hours DESC
        """,
        params,
    ).df()


def q_jobs(con, f: QueryFilters) -> pd.DataFrame:
    where, params = _where(f)
    return con.execute(
        f"""
        SELECT jobid, user, split_part(state, ' ', 1) AS state,
               round(elapsed_s/3600.0, 2)      AS hours,
               gpus,
               round(gpu_hours, 1)             AS gpu_hours,
               round(gpu_util_pct, 1)          AS gpu_util_pct,
               round(gpu_util_min, 1)          AS gpu_util_min,
               round(gpu_util_max, 1)          AS gpu_util_max,
               round(gpu_mem_pct, 1)           AS gpu_mem_pct,
               round(wasted_gpu_hours, 1)      AS wasted_gpu_hours,
               round(cpu_eff_pct, 1)           AS cpu_eff_pct,
               round(mem_eff_pct, 1)           AS mem_eff_pct,
               node, start
        FROM v_jobs {where}
        ORDER BY gpu_hours DESC NULLS LAST
        """,
        params,
    ).df()


def q_gpus(con, f: QueryFilters) -> pd.DataFrame:
    """Individual GPU rows - most useful with --job <id>."""
    where, params = _where(f)
    return con.execute(
        f"""
        SELECT g.jobid, j.user, g.node, g.gpu_index,
               round(g.util_pct, 1)                                    AS util_pct,
               round(g.mem_used_bytes / 1e9, 1)                        AS mem_used_gb,
               round(g.mem_total_bytes / 1e9, 1)                       AS mem_total_gb,
               round(100 * g.mem_used_bytes / nullif(g.mem_total_bytes, 0), 1)
                                                                       AS mem_pct
        FROM job_gpus g JOIN v_jobs j USING (jobid) {where}
        ORDER BY g.jobid, g.node, CAST(g.gpu_index AS INT)
        """,
        params,
    ).df()


REPORTS = {
    "overall": q_overall,
    "user": q_users,
    "job": q_jobs,
    "node": q_nodes,
    "state": q_states,
    "day": q_days,
    "gpu": q_gpus,
}


# ---------------------------------------------------------------------------
# 4. CLI
# ---------------------------------------------------------------------------


def cmd_load(args) -> None:
    jobs = parse_sacct_file(args.sacct_file, args.partition)
    if jobs.empty:
        sys.exit(f"no jobs found for partition {args.partition!r} in {args.sacct_file}")
    build_database(
        args.db, jobs, args.jobstats_cmd, args.cluster,
        args.workers, args.timeout, args.refresh, args.limit,
    )


def cmd_report(args) -> None:
    con = connect(args.db)
    f = QueryFilters(user=args.user, state=args.state, jobid=args.job)
    df = REPORTS[args.by](con, f)
    if args.sort:
        if args.sort not in df.columns:
            sys.exit(f"unknown sort column {args.sort!r}; available: {', '.join(df.columns)}")
        df = df.sort_values(args.sort, ascending=not args.desc, na_position="last")
    if args.top:
        df = df.head(args.top)
    if args.csv:
        df.to_csv(sys.stdout, index=False)
    else:
        with pd.option_context("display.max_rows", None, "display.width", None,
                               "display.max_columns", None):
            print(df.to_string(index=False))
    con.close()


def cmd_export(args) -> None:
    """Write one CSV of per-job statistics for each user."""
    con = connect(args.db)
    users = [r[0] for r in con.execute("SELECT DISTINCT user FROM jobs ORDER BY user").fetchall()]
    if args.user:
        if args.user not in users:
            sys.exit(f"user {args.user!r} not found in {args.db}")
        users = [args.user]

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    written = 0
    for user in users:
        df = q_jobs(con, QueryFilters(user=user, state=args.state))
        if df.empty:
            continue
        if args.sort not in df.columns:
            sys.exit(f"unknown sort column {args.sort!r}; available: {', '.join(df.columns)}")
        df = df.sort_values(args.sort, ascending=args.asc, na_position="last")
        path = outdir / (re.sub(r"[^\w.-]", "_", user) + ".csv")
        df.to_csv(path, index=False)
        print(f"{path}  ({len(df)} jobs)")
        written += 1
    con.close()
    if not written:
        print("no jobs matched; nothing written", file=sys.stderr)


def cmd_sql(args) -> None:
    con = connect(args.db)

    def run(query: str) -> None:
        try:
            df = con.execute(query).df()
            print(df.to_string(index=False) if not df.empty else "(no rows)")
        except duckdb.Error as exc:
            print(f"error: {exc}", file=sys.stderr)

    if args.query:
        run(args.query)
    else:
        print("Interactive SQL. Tables: jobs, job_stats, job_gpus, v_jobs, raw_jobstats.")
        print("Commands: .tables  .schema <table>  .quit")
        while True:
            try:
                line = input("sql> ").strip()
            except (EOFError, KeyboardInterrupt):
                break
            if not line:
                continue
            if line in (".quit", ".exit", "quit", "exit"):
                break
            if line == ".tables":
                run("SHOW TABLES")
            elif line.startswith(".schema"):
                target = line.split(None, 1)[1] if " " in line else "v_jobs"
                run(f"DESCRIBE {target}")
            else:
                run(line)
    con.close()


def cmd_tui(args) -> None:
    try:
        from textual.app import App  # noqa: F401
    except ImportError:
        sys.exit(
            "the TUI requires the `textual` package (pip install textual).\n"
            "Alternatively use `gpu_jobstats.py sql` for interactive querying."
        )
    run_tui(args.db)


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(
        prog="gpu_jobstats.py",
        description=__doc__.split("\n\n")[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = ap.add_subparsers(dest="command", required=True)

    p = sub.add_parser("load", help="parse sacct file, fetch jobstats, build DuckDB database")
    p.add_argument("sacct_file", help="sacct listing, e.g. sample_jobs.txt")
    p.add_argument("--db", default=DEFAULT_DB, help=f"DuckDB file (default {DEFAULT_DB})")
    p.add_argument("--partition", default=DEFAULT_PARTITION,
                   help=f"only keep this partition (default {DEFAULT_PARTITION})")
    p.add_argument("--jobstats-cmd", default="jobstats", help="jobstats executable")
    p.add_argument("--cluster", default=None, help="passed to jobstats as -c CLUSTER")
    p.add_argument("--workers", type=int, default=8, help="parallel jobstats calls (default 8)")
    p.add_argument("--timeout", type=int, default=60, help="seconds per jobstats call")
    p.add_argument("--refresh", action="store_true", help="refetch jobs already cached in the db")
    p.add_argument("--limit", type=int, default=None, help="fetch at most N jobs (for testing)")
    p.set_defaults(func=cmd_load)

    p = sub.add_parser("report", help="canned efficiency reports")
    p.add_argument("--db", default=DEFAULT_DB)
    p.add_argument("--by", choices=sorted(REPORTS), default="user",
                   help="grouping (default user)")
    p.add_argument("--user", help="restrict to one user")
    p.add_argument("--state", help="restrict to a state prefix, e.g. TIMEOUT or CANCELLED")
    p.add_argument("--job", help="restrict to one jobid (useful with --by gpu)")
    p.add_argument("--sort", help="sort by this output column")
    p.add_argument("--desc", action="store_true", help="sort descending")
    p.add_argument("--top", type=int, help="show only first N rows")
    p.add_argument("--csv", action="store_true", help="emit CSV instead of a table")
    p.set_defaults(func=cmd_report)

    p = sub.add_parser("export", help="write one per-job CSV file per user")
    p.add_argument("--db", default=DEFAULT_DB)
    p.add_argument("--outdir", default="user_csvs",
                   help="directory for <user>.csv files (default user_csvs)")
    p.add_argument("--user", help="export only this user")
    p.add_argument("--state", help="restrict to a state prefix, e.g. TIMEOUT or CANCELLED")
    p.add_argument("--sort", default="gpu_util_pct",
                   help="sort column within each file (default gpu_util_pct)")
    p.add_argument("--asc", action="store_true",
                   help="sort ascending (default is descending)")
    p.set_defaults(func=cmd_export)

    p = sub.add_parser("sql", help="run SQL against the database (no query = interactive)")
    p.add_argument("query", nargs="?", help="SQL to run; omit for an interactive prompt")
    p.add_argument("--db", default=DEFAULT_DB)
    p.set_defaults(func=cmd_sql)

    p = sub.add_parser("tui", help="interactive TUI browser (requires textual)")
    p.add_argument("--db", default=DEFAULT_DB)
    p.set_defaults(func=cmd_tui)

    args = ap.parse_args(argv)
    args.func(args)


# ---------------------------------------------------------------------------
# 5. TUI (optional, requires `textual`)
# ---------------------------------------------------------------------------


def run_tui(db_path: str) -> None:
    from textual.app import App, ComposeResult
    from textual.binding import Binding
    from textual.containers import Vertical
    from textual.widgets import DataTable, Footer, Header, Input, Static

    class JobstatsTUI(App):
        TITLE = "GPU jobstats"
        CSS = """
        #hint { height: 1; color: $text-muted; padding: 0 1; }
        #filter { height: 3; }
        DataTable { height: 1fr; }
        """
        BINDINGS = [
            Binding("u", "set_view('user')", "Users"),
            Binding("j", "set_view('job')", "Jobs"),
            Binding("n", "set_view('node')", "Nodes"),
            Binding("s", "set_view('state')", "States"),
            Binding("d", "set_view('day')", "Days"),
            Binding("g", "set_view('gpu')", "GPUs"),
            Binding("o", "set_view('overall')", "Overall"),
            Binding("slash", "focus_filter", "Filter"),
            Binding("escape", "clear_filter", "Clear filter", show=False),
            Binding("q", "quit", "Quit"),
        ]

        def __init__(self, db: str) -> None:
            super().__init__()
            self.con = connect(db)
            self.view = "user"
            self.filters = QueryFilters()
            self.df = pd.DataFrame()
            self.sort_col: str | None = None
            self.sort_asc = False

        def compose(self) -> ComposeResult:
            yield Header()
            with Vertical():
                yield Static(id="hint")
                yield Input(placeholder="filter (substring of user / jobid / state)…",
                            id="filter")
                yield DataTable(id="table", cursor_type="row", zebra_stripes=True)
            yield Footer()

        def on_mount(self) -> None:
            self.refresh_table()
            self.query_one(DataTable).focus()

        def on_unmount(self) -> None:
            self.con.close()

        # -- data ---------------------------------------------------------
        def refresh_table(self) -> None:
            df = REPORTS[self.view](self.con, self.filters)
            if self.sort_col in df.columns:
                df = df.sort_values(self.sort_col, ascending=self.sort_asc,
                                    na_position="last")
            self.df = df.reset_index(drop=True)
            table = self.query_one(DataTable)
            table.clear(columns=True)
            table.add_columns(*[str(c) for c in df.columns])
            for _, row in self.df.iterrows():
                table.add_row(*["" if pd.isna(v) else str(v) for v in row])
            drill = {"user": "Enter: user's jobs", "job": "Enter: job's GPUs"}
            self.query_one("#hint", Static).update(
                f"view: {self.view}  rows: {len(df)}  "
                f"{drill.get(self.view, '')}  (click a column header to sort)"
            )

        # -- actions --------------------------------------------------------
        def action_set_view(self, view: str) -> None:
            self.view = view
            self.sort_col = None
            self.filters.user = self.filters.jobid = None
            self.refresh_table()

        def action_focus_filter(self) -> None:
            self.query_one("#filter", Input).focus()

        def action_clear_filter(self) -> None:
            self.query_one("#filter", Input).value = ""
            self.filters = QueryFilters()
            self.refresh_table()
            self.query_one(DataTable).focus()

        # -- events -------------------------------------------------------
        def on_input_changed(self, event) -> None:
            self.filters.contains = event.value or None
            self.refresh_table()

        def on_input_submitted(self, event) -> None:
            self.query_one(DataTable).focus()

        def on_data_table_header_selected(self, event) -> None:
            col = str(self.df.columns[event.column_index])
            if self.sort_col == col:
                self.sort_asc = not self.sort_asc
            else:
                self.sort_col, self.sort_asc = col, False
            self.refresh_table()

        def on_data_table_row_selected(self, event) -> None:
            if self.df.empty:
                return
            row = self.df.iloc[event.cursor_row]
            if self.view == "user":  # drill into that user's jobs
                self.view = "job"
                self.filters = QueryFilters(user=str(row["user"]))
            elif self.view == "job":  # drill into that job's GPUs
                self.view = "gpu"
                self.filters = QueryFilters(jobid=str(row["jobid"]))
            else:
                return
            self.sort_col = None
            self.refresh_table()

    JobstatsTUI(db_path).run()


if __name__ == "__main__":
    main()
