# Architecture — ELT Data Contracts Platform

## Component view

```
                 ┌────────────────────┐
  sources ─────▶ │  Extractor          │  declarative SourceSpec:
 (CSV/JSON/JSONL)│  (no business logic)│  name + path + format + table
                 └─────────┬──────────┘
                           │ raw rows
                           ▼
                 ┌────────────────────┐      breaching rows + reasons
                 │  Contract gate      │ ───────────────────────┐
                 │  coerce (per        │                        ▼
                 │  contract types) -> │              ┌──────────────────┐
                 │  validate per row,  │              │ <table>_quarantine│
                 │  uniqueness,        │              │ payload + reasons │
                 │  freshness SLA      │              └──────────────────┘
                 └─────────┬──────────┘
                       valid rows only
                           ▼
                 ┌────────────────────┐
                 │  Loader / Warehouse │  SQLite tables built FROM contracts;
                 │                     │  replace loads in one transaction;
                 │                     │  every table mirrored to
                 │                     │  <export_root>/<table>/<table>.json
                 └─────────┬──────────┘
                           ▼
                 ┌────────────────────┐   retries per step, attempts in manifest
                 │  Transform DAG      │   SQL: DROP + CREATE TABLE AS <select>
                 │                     │   Python: callable -> rows -> table
                 └─────────┬──────────┘
                           ▼
                 ┌────────────────────┐
                 │  Validation round 2 │  transform outputs vs their contracts
                 │  + data tests       │  + row counts / PK uniqueness /
                 │                     │  referential integrity across tables
                 └─────────┬──────────┘
                           ▼
                 RunManifest ──▶ ManifestStore (one JSON per run + index.jsonl)
                           │
                           ▼
                 FastAPI: /health /contracts /runs /runs/{id} /runs/demo
```

## Data flow, step by step

1. **Declare.** A pipeline is data: source specs, a contract per table,
   a transform DAG, and referential checks. The demo pipeline
   (`demo.py`) and `examples/contracts.yml` show the same contracts in
   code and YAML form.
2. **Extract.** Extractors return raw rows and nothing else. In
   particular they do *not* fix types: a CSV is all strings, and that
   fact is the contract layer's business.
3. **Gate.** Rows are coerced to contract types (un-coercible values are
   left as-is so validation can name them), then validated per row:
   presence/nullability, type, numeric bounds, length, allowed values,
   regex pattern, and uniqueness across the batch for `unique`/PK
   columns. Valid rows proceed; breaching rows go to
   `<table>_quarantine` with their reasons. Freshness is checked at
   table level against the contract SLA: a stale source quarantines the
   whole batch, because loading stale data as current is worse than
   loading none.
4. **Load.** The warehouse creates the table *from the contract* (types
   and primary key come from the same single source of truth) and loads
   with `replace` semantics in one transaction, then mirrors the table
   to the JSON file layout.
5. **Transform.** The DAG topologically sorts SQL and Python transforms
   (cycles are rejected before any work runs). SQL transforms are
   materialised as DROP + CREATE TABLE AS, Python transforms return
   rows that are loaded replace-mode — both are re-run safe. A failing
   transform retries up to the pipeline's `max_retries`; attempts land
   in the manifest.
6. **Validate again.** Transform outputs that have contracts are
   validated against them (a model that starts emitting negative totals
   fails the run, not the dashboard). Then data tests run: row counts,
   primary-key uniqueness and non-nullness for every contracted table,
   and declared referential-integrity checks between tables.
7. **Report.** Every run — success or failure — persists a manifest:
   per-step rows in/out/quarantined, attempts, the full data-test
   report, and the error when there is one. The API serves manifests and
   contract status from the store.

## Key decisions

### Why contracts gate twice (extract and post-transform)?

Producer contracts and model contracts fail differently. A producer
breach is usually a deploy or a file change upstream; a transform-output
breach is usually *your own* model change. Gating only at extract lets
model regressions ship; gating only at the end loses the per-source
accounting (which producer, which rows, which reasons) that makes a
breach debuggable. Two gates, two owners, one mechanism.

### Why per-row quarantine instead of failing the batch on any bad row?

Availability vs correctness is a false binary at row level: quarantining
the 1 bad row out of 60,000 keeps the business running *and* keeps the
bad row out of trusted tables, with a complete audit trail. The
exceptions are deliberately structural — zero valid rows, a freshness
breach, a failing data test — because those mean the pipeline's
assumptions, not individual records, are broken.

### How idempotency actually works

Two layers. *Convergence:* replace-mode loads and DROP+CREATE transforms
mean any re-run with the same inputs produces the same warehouse state;
there is no append path for a duplicate to hide in. *Short-circuit:* a
caller may pass an `idempotency_key` (e.g. the batch's logical date);
if the manifest store already holds a *successful* run under that key,
it is returned and nothing re-executes. Failed runs never block a retry
under the same key — that asymmetry is the point.

### Why SQLite + JSON files instead of a "real" warehouse?

The platform's ideas — contracts as schema source of truth, SQL
transforms, SQL-expressible data tests, transactional replace loads —
are dialect-level, not vendor-level. SQLite keeps the whole system
runnable offline in seconds (tests, CI, a laptop demo) while preserving
the interfaces a cloud warehouse would slot into: `Warehouse` is the
only class that knows the dialect. The JSON export mirrors the
partitioned-file layout a lakehouse would use, in a form a human can
diff.

## Failure modes and what the platform does

| Failure | Behaviour |
|---|---|
| A few rows breach a contract | Quarantined with reasons; run succeeds; counts in manifest |
| All rows breach / stale source | Run fails closed; nothing trusted is loaded from that source |
| Transform raises transiently | Retried up to `max_retries`; attempts recorded |
| Transform output breaches its contract | Run fails; output not promoted as trusted |
| Orphan foreign keys after load | Referential data test fails the run; manifest names the check |
| Same batch submitted twice | Idempotency key returns the original successful manifest |
| Process dies mid-run | Last persisted state: warehouse holds the last *completed* load (loads are transactional); the next run converges by replace semantics |

## Scaling notes (what production adds)

- **Incremental loads** with watermark state per source, replacing full
  replace-mode for large tables (the contract and manifest layers are
  unchanged).
- **A real warehouse backend** (DuckDB locally, Snowflake/BigQuery in
  cloud) behind the `Warehouse` interface, and Parquet for the file
  layout.
- **Contract CI**: version contracts, check backward compatibility on
  PRs, and publish them to a registry the API can serve.
- **Scheduling** is intentionally out of scope: the pipeline is a
  callable + CLI + HTTP trigger, so Airflow/Dagster/cron can own the
  clock while this platform owns correctness.
