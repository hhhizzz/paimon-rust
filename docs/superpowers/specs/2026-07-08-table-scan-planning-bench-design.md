# TableScan Planning Bench and Deepening Design

## Purpose

Improve the `TableScan` planning module in two steps:

1. Add a Criterion benchmark harness that measures core scan planning through the existing public interface.
2. Deepen the internal `TableScan` implementation without changing the external interface, then compare benchmark and trace output before and after.

The first implementation pass is structural. It should preserve `ScanTrace` counters and query behavior. Any performance optimization beyond reduced internal friction should be driven by benchmark results, not guessed upfront.

## Scope

In scope:

- Add a Criterion benchmark under the core `paimon` crate.
- Benchmark `Table::new_read_builder().new_scan().plan_with_trace()` directly.
- Use synthetic in-memory tables created by existing public write and commit interfaces.
- Capture `ScanTrace` alongside Criterion timings so timing output has planning context.
- Refactor scan planning internals into deeper internal modules while keeping `ReadBuilder`, `TableScan`, `Plan`, and `DataSplit` external behavior stable.
- Compare before/after by running the same benchmark and existing trace tests.

Out of scope:

- DataFusion SQL benchmark in the first pass.
- Changing `TableScan` public methods or `ReadBuilder` public methods.
- Reader-side Parquet row-group pruning benchmark.
- Query result performance benchmark.
- New storage adapter behavior.

## Current Shape

The current `TableScan` module has a small interface but a large implementation:

- `ReadBuilder::new_scan()` converts projection, partition/data/bucket predicates, limit, and row ranges into `TableScan`.
- `TableScan::plan()` and `TableScan::plan_with_trace()` resolve a snapshot and return a `Plan`.
- `TableScan::plan_manifest_entries()` is also used by non-read paths such as commit overwrite and index builders.
- `TableScan::plan_snapshot()` reads manifest entries, applies pruning, evaluates index metadata, handles data-evolution row ranges, packs files into splits, applies limit pushdown, and records `ScanTrace`.

This is already a deep module at the external seam, but internal locality is weak. Changes to manifest pruning, data evolution, deletion vectors, global index, limit pushdown, and trace counters all land in `table_scan.rs`.

## Design

### Benchmark Module

Add `criterion` as a `dev-dependency` to `crates/paimon/Cargo.toml` and add a benchmark target:

- `crates/paimon/benches/table_scan_planning.rs`

The benchmark will:

- Create synthetic tables in memory storage.
- Write and commit data with existing `WriteBuilder`, `TableWrite`, and `TableCommit`.
- Run `plan_with_trace()` inside Criterion iterations.
- Cover these scan planning scenarios:
  - append full scan over many committed files
  - append partition-pruned scan
  - fixed-bucket primary-key scan with bucket predicate
  - limit pushdown scan

The benchmark should use deterministic table names per setup and create the table once per benchmark case, outside the measured loop. The measured loop should include only planning, not setup or data writes.

### Trace Context

Criterion output should be paired with `ScanTrace` context. The benchmark does not need to assert trace values, but it should make trace available in setup comments and use named benchmark cases that correspond to trace scenarios.

Existing tests remain the hard regression check:

- `crates/integrations/datafusion/tests/scan_pruning_trace.rs`
- `crates/integration_tests/tests/read_tables.rs::test_limit_pushdown`
- `crates/paimon/src/table/table_scan.rs` unit tests

### Internal Module Split

Keep the public interface stable. Add internal modules under `crates/paimon/src/table/`:

- `scan_manifest_planner.rs`
- `scan_split_planner.rs`

`scan_manifest_planner` owns:

- reading base and delta manifest lists
- reading manifest files
- manifest-file partition stats pruning
- entry-level bucket and partition pruning
- level-0 pruning
- data stats pruning for current schema
- merging ADD/DELETE manifest entries
- manifest-related `ScanTrace` counters

`scan_split_planner` owns:

- grouping entries by `(partition, bucket)`
- cross-schema stats pruning pass
- deletion-vector map construction
- global-index row-range evaluation handoff
- data-evolution row-id grouping and read-field pruning
- merge-tree split grouping
- size-based split packing
- limit pushdown
- final `DataSplit` construction and final `ScanTrace` counters

`table_scan.rs` remains the external module and coordinator:

- keeps `TableScan` struct and methods
- resolves snapshots
- checks query authorization
- computes scan options
- delegates manifest planning and split planning internally

### Interfaces

The new internal interfaces should be private to the `table` module where possible. They should use concrete structs rather than long argument lists.

Target shape:

```rust
pub(super) struct ManifestPlanningInput<'a> {
    pub table: &'a Table,
    pub snapshot: &'a Snapshot,
    pub partition_filter: Option<&'a PartitionFilter>,
    pub data_predicates: &'a [Predicate],
    pub bucket_predicate: Option<&'a Predicate>,
    pub scan_all_files: bool,
}

pub(super) struct ManifestPlanningOutput {
    pub entries: Vec<ManifestEntry>,
}
```

```rust
pub(super) struct SplitPlanningInput<'a> {
    pub table: &'a Table,
    pub snapshot: Snapshot,
    pub entries: Vec<ManifestEntry>,
    pub data_predicates: &'a [Predicate],
    pub row_ranges: Option<Vec<RowRange>>,
    pub projected_read_field_ids: Option<&'a HashSet<i32>>,
    pub limit: Option<usize>,
    pub scan_all_files: bool,
}

pub(super) struct SplitPlanningOutput {
    pub plan: Plan,
}
```

The exact fields may change during implementation if Rust ownership makes a smaller interface clearer, but the direction is fixed: one manifest-planning interface and one split-planning interface.

## Success Criteria

Functional success:

- Existing public scan behavior is unchanged.
- Existing tests pass.
- `ScanTrace` values for existing trace tests remain equivalent.
- `plan_manifest_entries()` continues to support commit overwrite and index builder callers.

Benchmark success:

- `cargo bench -p paimon --bench table_scan_planning` runs successfully.
- Benchmark setup excludes table creation and writes from measured timing.
- Bench cases are named clearly enough to compare before/after output.

Architecture success:

- `table_scan.rs` no longer owns every scan planning detail directly.
- Manifest pruning logic has locality in `scan_manifest_planner.rs`.
- Split generation logic has locality in `scan_split_planner.rs`.
- External callers still use the same `ReadBuilder`, `TableScan`, `Plan`, and `DataSplit` interfaces.

## Verification Commands

Run these after adding the benchmark and after the refactor:

```bash
rtk cargo test -p paimon table_scan
rtk cargo test -p paimon-datafusion --test scan_pruning_trace
rtk cargo test -p paimon-integration-tests test_limit_pushdown
rtk cargo bench -p paimon --bench table_scan_planning
```

If Criterion takes too long locally, run the benchmark once with a smaller sample size during development and once with default settings for the final comparison.

## Comparison Plan

1. Add benchmark only.
2. Run benchmark and save the output summary in the working notes for the final report.
3. Refactor internals.
4. Run the same benchmark again.
5. Report:
   - changed files
   - test results
   - benchmark before/after summary
   - whether trace counters stayed stable
   - any hotspot exposed by the benchmark for a follow-up optimization

## Risks

- Criterion adds a new dev-dependency to the core crate.
- In-memory storage can hide object-store latency; this benchmark targets planning CPU and metadata processing, not real remote IO.
- Moving code without changing behavior can still perturb trace counter update order; tests should pin important counters.
- A broad refactor can accidentally alter non-read callers of `plan_manifest_entries()`.

## Non-Goals

- Do not optimize DataFusion SQL planning yet.
- Do not introduce a new public scan interface.
- Do not change `ScanTrace` semantics unless a test exposes a current bug.
- Do not make the benchmark depend on external files, network storage, or wall-clock sleeps.
