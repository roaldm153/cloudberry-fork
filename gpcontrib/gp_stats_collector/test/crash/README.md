# gp_stats_collector crash test

Liveness/regression test: with the runtime query-state feature fully enabled and
a poller tracing every running query, Cloudberry must not crash and queries must
still finish with the same results as without tracing.

Driven by `.github/workflows/gpsc-crash-test.yaml` (manual / nightly). One build,
one demo cluster, up to three `installcheck-world` runs on it:

1. **baseline** — no poller → record failed tests.
2. **traced** — poller running → record failed tests, then the crash gate.
3. **reconfirm** — no poller, only if run 2 added new failures → tells a genuine
   tracing regression apart from an independent flake.

Verdict: FAIL iff the crash gate tripped OR a test failed under tracing that
neither the baseline nor the no-poller reconfirm reproduced.

## Files

- `poller.py` — single-process tracer: loops over active client backends in
  `pg_stat_activity` and calls `gpsc.pg_query_state(pid, trace_id)` on each, with
  a per-pid cooldown so no pid is polled while a prior poll is in flight (the
  extension does not support overlapping polls of one pid). Uses `psql`, no
  Python DB driver.
- `uds_drain.py` — minimal `AF_UNIX` sink for `gpsc.uds_path`; reads and discards
  so the serialize+send path runs without the real yagpcc.
- `extract_failures.sh` — pulls the set of `... FAILED` test names from a
  `make installcheck-world` log.
- `crash_scan.sh` — the crash gate: log crash markers, `gpstate -e`, `SELECT 1`.

Design notes: `../../docs/gpsc-crash-test-design.md` (local, not committed).
