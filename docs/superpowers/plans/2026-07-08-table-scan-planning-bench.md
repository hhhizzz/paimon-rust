# TableScan Planning Bench Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a core Criterion benchmark for `TableScan` planning, capture a baseline, then deepen the internal scan-planning implementation without changing public scan behavior.

**Architecture:** Keep the external seam at `ReadBuilder -> TableScan::plan_with_trace() -> Plan`. Add a benchmark target under the core `paimon` crate, then split `table_scan.rs` internals into `scan_manifest_planner.rs` and `scan_split_planner.rs` so manifest pruning and split generation each become deeper internal modules with smaller interfaces.

**Tech Stack:** Rust 2021, Cargo workspace, Criterion, Tokio, Arrow `RecordBatch`, Paimon memory storage, existing `ScanTrace` regression tests.

## Global Constraints

- Work in the existing `table-scan-improve` worktree.
- Prefix shell commands with `rtk`.
- Use `apply_patch` for manual file edits.
- Keep `ReadBuilder`, `TableScan`, `Plan`, and `DataSplit` public behavior stable.
- Do not add a DataFusion SQL benchmark in this pass.
- Do not change `ScanTrace` semantics unless an existing test exposes a bug.
- Do not make the benchmark depend on external files, network storage, or sleeps.
- Use Criterion only in `crates/paimon` dev dependencies.
- Preserve `plan_manifest_entries()` for commit overwrite and index builder callers.

---

## File Structure

- Modify `crates/paimon/Cargo.toml`
  - Add `criterion` dev dependency.
  - Add `[[bench]]` entry for `table_scan_planning` with `harness = false`.

- Create `crates/paimon/benches/table_scan_planning.rs`
  - Owns synthetic table setup for scan-planning benchmarks.
  - Uses public write/commit/read interfaces.
  - Benchmarks only `plan_with_trace()` inside Criterion iterations.

- Modify `crates/paimon/src/table/mod.rs`
  - Register new internal modules:
    - `scan_manifest_planner`
    - `scan_split_planner`

- Modify `crates/paimon/src/table/table_scan.rs`
  - Keep `TableScan` struct and public methods.
  - Keep snapshot resolution and query-auth checks.
  - Delegate manifest entry planning to `scan_manifest_planner`.
  - Delegate split generation to `scan_split_planner`.
  - Keep small compatibility methods such as `plan_manifest_entries()`.

- Create `crates/paimon/src/table/scan_manifest_planner.rs`
  - Owns manifest list reading, manifest file reading, manifest pruning, ADD/DELETE merging, and manifest trace counters.

- Create `crates/paimon/src/table/scan_split_planner.rs`
  - Owns cross-schema pruning, deletion-vector map use, global-index row ranges, data-evolution grouping, split packing, limit pushdown, `DataSplit` construction, and final trace counters.

---

### Task 1: Add Core Criterion Benchmark Harness

**Files:**
- Modify: `crates/paimon/Cargo.toml`
- Create: `crates/paimon/benches/table_scan_planning.rs`

**Interfaces:**
- Consumes: Existing public `Table::new_read_builder()`, `ReadBuilder::new_scan()`, `TableScan::plan_with_trace()`.
- Produces: Cargo bench target `table_scan_planning`, run with `rtk cargo bench -p paimon --bench table_scan_planning`.

- [ ] **Step 1: Write the failing benchmark target declaration**

Edit `crates/paimon/Cargo.toml`:

```toml
[dev-dependencies]
axum = { version = "0.7", features = ["macros", "tokio", "http1", "http2"] }
criterion = { version = "0.5", features = ["async_tokio"] }
rand = "0.8.5"
tempfile = "3"

[[bench]]
name = "table_scan_planning"
harness = false
```

Do not create the bench file yet.

- [ ] **Step 2: Run benchmark compile to verify it fails**

Run:

```bash
rtk cargo bench -p paimon --bench table_scan_planning --no-run
```

Expected: FAIL because `crates/paimon/benches/table_scan_planning.rs` does not exist.

- [ ] **Step 3: Add the benchmark skeleton**

Create `crates/paimon/benches/table_scan_planning.rs` with this initial content:

```rust
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn bench_table_scan_planning(c: &mut Criterion) {
    c.bench_function("append_full_scan_many_files", |b| {
        b.iter(|| black_box(1usize));
    });
}

criterion_group!(benches, bench_table_scan_planning);
criterion_main!(benches);
```

- [ ] **Step 4: Verify the skeleton compiles**

Run:

```bash
rtk cargo bench -p paimon --bench table_scan_planning --no-run
```

Expected: PASS compile.

- [ ] **Step 5: Replace skeleton with public-interface scan setup**

Replace the file with a benchmark that:

- builds a `tokio::runtime::Runtime`
- creates memory-backed tables through `FileIOBuilder::new("memory")`
- creates table directories `snapshot/` and `manifest/`
- builds `Table` values with `Table::new`
- writes synthetic batches through `table.new_write_builder().new_write()`
- commits through `new_commit().commit(messages)`
- benchmarks `plan_with_trace()`

Use this structure:

```rust
use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use paimon::catalog::Identifier;
use paimon::io::{FileIO, FileIOBuilder};
use paimon::spec::{
    DataType, Datum, IntType, Predicate, PredicateBuilder, Schema, TableSchema, VarCharType,
};
use paimon::table::Table;
use std::sync::Arc;
use tokio::runtime::Runtime;

fn bench_table_scan_planning(c: &mut Criterion) {
    let runtime = Runtime::new().expect("benchmark runtime");
    let append = runtime.block_on(setup_append_table("memory:/bench_scan_append", 48, 32));
    let pk = runtime.block_on(setup_pk_table("memory:/bench_scan_pk", 48, 32));

    let mut group = c.benchmark_group("table_scan_planning");

    group.bench_function(BenchmarkId::new("append_full_scan_many_files", 48), |b| {
        b.iter(|| {
            runtime.block_on(async {
                let result = append.new_read_builder().new_scan().plan_with_trace().await;
                black_box(result.expect("append full scan planning"))
            })
        });
    });

    let append_partition_filter = partition_filter(&append, "2024-01-03");
    group.bench_function(BenchmarkId::new("append_partition_pruned", 48), |b| {
        b.iter(|| {
            let filter = append_partition_filter.clone();
            runtime.block_on(async {
                let mut builder = append.new_read_builder();
                builder.with_filter(filter);
                let result = builder.new_scan().plan_with_trace().await;
                black_box(result.expect("append partition-pruned planning"))
            })
        });
    });

    let pk_bucket_filter = id_filter(&pk, 7);
    group.bench_function(BenchmarkId::new("pk_bucket_pruned", 48), |b| {
        b.iter(|| {
            let filter = pk_bucket_filter.clone();
            runtime.block_on(async {
                let mut builder = pk.new_read_builder();
                builder.with_filter(filter);
                let result = builder.new_scan().plan_with_trace().await;
                black_box(result.expect("pk bucket-pruned planning"))
            })
        });
    });

    group.bench_function(BenchmarkId::new("append_limit_pushdown", 48), |b| {
        b.iter(|| {
            runtime.block_on(async {
                let mut builder = append.new_read_builder();
                builder.with_limit(1);
                let result = builder.new_scan().plan_with_trace().await;
                black_box(result.expect("append limit planning"))
            })
        });
    });

    group.finish();
}
```

Add helper functions in the same file:

```rust
async fn setup_dirs(file_io: &FileIO, table_path: &str) {
    file_io
        .mkdirs(&format!("{table_path}/snapshot/"))
        .await
        .expect("snapshot dir");
    file_io
        .mkdirs(&format!("{table_path}/manifest/"))
        .await
        .expect("manifest dir");
}

async fn setup_append_table(table_path: &str, commits: usize, rows_per_commit: usize) -> Table {
    let file_io = FileIOBuilder::new("memory").build().expect("memory file io");
    setup_dirs(&file_io, table_path).await;
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .column("dt", DataType::VarChar(VarCharType::string_type()))
        .partition_keys(["dt"])
        .build()
        .expect("append schema");
    let table = Table::new(
        file_io,
        Identifier::new("default", "bench_append"),
        table_path.to_string(),
        TableSchema::new(0, &schema),
        None,
    );
    for commit_idx in 0..commits {
        let batch = append_batch(commit_idx, rows_per_commit);
        write_commit(&table, batch).await;
    }
    table
}

async fn setup_pk_table(table_path: &str, commits: usize, rows_per_commit: usize) -> Table {
    let file_io = FileIOBuilder::new("memory").build().expect("memory file io");
    setup_dirs(&file_io, table_path).await;
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .primary_key(["id"])
        .option("bucket", "16")
        .build()
        .expect("pk schema");
    let table = Table::new(
        file_io,
        Identifier::new("default", "bench_pk"),
        table_path.to_string(),
        TableSchema::new(0, &schema),
        None,
    );
    for commit_idx in 0..commits {
        let batch = pk_batch(commit_idx, rows_per_commit);
        write_commit(&table, batch).await;
    }
    table
}

async fn write_commit(table: &Table, batch: RecordBatch) {
    let wb = table.new_write_builder();
    let mut write = wb.new_write().expect("new write");
    write.write_arrow_batch(&batch).await.expect("write batch");
    let messages = write.prepare_commit().await.expect("prepare commit");
    wb.new_commit().commit(messages).await.expect("commit");
}

fn append_batch(commit_idx: usize, rows: usize) -> RecordBatch {
    let start = (commit_idx * rows) as i32;
    let ids = (0..rows).map(|row| start + row as i32).collect::<Vec<_>>();
    let values = ids.iter().map(|id| id * 10).collect::<Vec<_>>();
    let dt = format!("2024-01-{:02}", (commit_idx % 8) + 1);
    let partitions = (0..rows).map(|_| dt.as_str()).collect::<Vec<_>>();
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("value", ArrowDataType::Int32, false),
            ArrowField::new("dt", ArrowDataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(Int32Array::from(values)),
            Arc::new(StringArray::from(partitions)),
        ],
    )
    .expect("append batch")
}

fn pk_batch(commit_idx: usize, rows: usize) -> RecordBatch {
    let start = (commit_idx * rows) as i32;
    let ids = (0..rows).map(|row| start + row as i32).collect::<Vec<_>>();
    let values = ids.iter().map(|id| id * 10).collect::<Vec<_>>();
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("value", ArrowDataType::Int32, false),
        ])),
        vec![Arc::new(Int32Array::from(ids)), Arc::new(Int32Array::from(values))],
    )
    .expect("pk batch")
}

fn partition_filter(table: &Table, value: &str) -> Predicate {
    PredicateBuilder::new(table.schema().fields())
        .equal("dt", Datum::String(value.to_string()))
        .expect("partition predicate")
}

fn id_filter(table: &Table, value: i32) -> Predicate {
    PredicateBuilder::new(table.schema().fields())
        .equal("id", Datum::Int(value))
        .expect("id predicate")
}
```

Keep `criterion_group!(benches, bench_table_scan_planning);` and `criterion_main!(benches);`.

- [ ] **Step 6: Run compile check**

Run:

```bash
rtk cargo bench -p paimon --bench table_scan_planning --no-run
```

Expected: PASS compile. If the bench needs additional dev-dependency features for `tokio::runtime::Runtime`, add this to `crates/paimon/Cargo.toml` dev-dependencies:

```toml
tokio = { workspace = true, features = ["rt-multi-thread", "macros"] }
```

- [ ] **Step 7: Run the benchmark baseline**

Run:

```bash
rtk cargo bench -p paimon --bench table_scan_planning
```

Expected: PASS with four Criterion benchmark cases:

- `table_scan_planning/append_full_scan_many_files/48`
- `table_scan_planning/append_partition_pruned/48`
- `table_scan_planning/pk_bucket_pruned/48`
- `table_scan_planning/append_limit_pushdown/48`

Save the terminal summary for the final report. Do not commit generated `target/criterion` output.

- [ ] **Step 8: Commit benchmark harness**

Run:

```bash
rtk git add crates/paimon/Cargo.toml crates/paimon/benches/table_scan_planning.rs Cargo.lock
rtk git commit -m "bench: add table scan planning benchmark"
```

Expected: commit succeeds.

---

### Task 2: Extract Manifest Planning Internal Module

**Files:**
- Modify: `crates/paimon/src/table/mod.rs`
- Modify: `crates/paimon/src/table/table_scan.rs`
- Create: `crates/paimon/src/table/scan_manifest_planner.rs`

**Interfaces:**
- Consumes: `Table`, `Snapshot`, optional `PartitionFilter`, raw scan `Predicate` list, optional bucket predicate, `scan_all_files`.
- Produces: `pub(super) async fn plan_manifest_entries(input: ManifestPlanningInput<'_>, trace: Option<&mut ScanTrace>) -> crate::Result<Vec<ManifestEntry>>`.

- [ ] **Step 1: Write the failing module test**

Add the module declaration in `crates/paimon/src/table/mod.rs`:

```rust
mod scan_manifest_planner;
```

Create `crates/paimon/src/table/scan_manifest_planner.rs` with only the module header and a test that references the future merge helper:

```rust
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#[cfg(test)]
mod tests {
    #[test]
    fn manifest_planner_exposes_merge_entry_behavior() {
        let _ = super::merge_manifest_entries;
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
rtk cargo test -p paimon manifest_planner_exposes_merge_entry_behavior
```

Expected: FAIL because `merge_manifest_entries` is not defined in `scan_manifest_planner`.

- [ ] **Step 3: Move manifest-owned helpers into the new module**

Move these items from `table_scan.rs` into `scan_manifest_planner.rs`:

- `ManifestReadCounters`
- `read_manifest_list`
- `read_all_manifest_entries`
- `merge_manifest_entries`
- `should_skip_level_zero_for_scan`

Add imports needed by the moved code:

```rust
use super::bucket_filter::compute_target_buckets;
use super::kv_file_reader::retain_primary_key_conjuncts;
use super::partition_filter::PartitionFilter;
use super::stats_filter::{
    data_file_matches_predicates, FileStatsRows,
};
use super::{ScanTrace, Table};
use crate::io::FileIO;
use crate::spec::{
    avro::SharedSchemaCache, BinaryRow, BucketFunctionType, CoreOptions, DataField, FileKind,
    ManifestEntry, Predicate, Snapshot,
};
use futures::{StreamExt, TryStreamExt};
use std::collections::{HashMap, HashSet};
```

Keep `MANIFEST_DIR` accessible by defining it in the new module:

```rust
const MANIFEST_DIR: &str = "manifest";
```

- [ ] **Step 4: Add the manifest-planning interface**

Add this interface to `scan_manifest_planner.rs`:

```rust
pub(super) struct ManifestPlanningInput<'a> {
    pub(super) table: &'a Table,
    pub(super) snapshot: &'a Snapshot,
    pub(super) partition_filter: Option<&'a PartitionFilter>,
    pub(super) data_predicates: &'a [Predicate],
    pub(super) bucket_predicate: Option<&'a Predicate>,
    pub(super) scan_all_files: bool,
}

pub(super) async fn plan_manifest_entries(
    input: ManifestPlanningInput<'_>,
    trace: Option<&mut ScanTrace>,
) -> crate::Result<Vec<ManifestEntry>> {
    let table = input.table;
    let core_options = CoreOptions::new(table.schema().options());
    let data_evolution_enabled = core_options.data_evolution_enabled();
    let has_primary_keys = !table.schema().primary_keys().is_empty();
    let deletion_vectors_enabled = core_options.deletion_vectors_enabled();
    let skip_level_zero = should_skip_level_zero_for_scan(
        input.scan_all_files,
        has_primary_keys,
        deletion_vectors_enabled,
        core_options.merge_engine(),
    );
    let partition_fields = table.schema().partition_fields();
    let pushdown_data_predicates = if data_evolution_enabled {
        Vec::new()
    } else {
        stats_pruning_predicates(table, input.data_predicates)
    };
    let bucket_key_fields = bucket_key_fields(table, input.bucket_predicate, &core_options);
    let bucket_function_type = core_options.bucket_function_type()?;
    let entries = read_all_manifest_entries(
        table.file_io(),
        table.location(),
        input.snapshot,
        skip_level_zero,
        input.scan_all_files,
        has_primary_keys,
        input.partition_filter,
        &partition_fields,
        &pushdown_data_predicates,
        table.schema().id(),
        table.schema().fields(),
        input.bucket_predicate,
        &bucket_key_fields,
        bucket_function_type,
        trace,
    )
    .await?;
    Ok(merge_manifest_entries(entries))
}
```

Add helper functions used by the interface:

```rust
pub(super) fn stats_pruning_predicates(table: &Table, data_predicates: &[Predicate]) -> Vec<Predicate> {
    let has_primary_keys = !table.schema().primary_keys().is_empty();
    let core_options = CoreOptions::new(table.schema().options());
    let deletion_vectors_enabled = core_options.deletion_vectors_enabled();
    let first_row = matches!(
        core_options.merge_engine(),
        Ok(crate::spec::MergeEngine::FirstRow)
    );
    if has_primary_keys && !deletion_vectors_enabled && !first_row {
        retain_primary_key_conjuncts(
            data_predicates,
            table.schema().fields(),
            &table.schema().trimmed_primary_keys(),
        )
    } else {
        data_predicates.to_vec()
    }
}

fn bucket_key_fields(
    table: &Table,
    bucket_predicate: Option<&Predicate>,
    core_options: &CoreOptions,
) -> Vec<DataField> {
    if bucket_predicate.is_none() {
        return Vec::new();
    }
    let bucket_keys = core_options.bucket_key().unwrap_or_else(|| {
        if !table.schema().primary_keys().is_empty() {
            table.schema().trimmed_primary_keys()
        } else {
            Vec::new()
        }
    });
    bucket_keys
        .iter()
        .filter_map(|key| {
            table
                .schema()
                .fields()
                .iter()
                .find(|field| field.name() == key)
                .cloned()
        })
        .collect()
}
```

- [ ] **Step 5: Wire `TableScan` to the new module**

In `table_scan.rs`, replace the body of `plan_manifest_entries_with_trace` with:

```rust
    async fn plan_manifest_entries_with_trace(
        &self,
        snapshot: &Snapshot,
        trace: Option<&mut ScanTrace>,
    ) -> crate::Result<Vec<ManifestEntry>> {
        let entries = super::scan_manifest_planner::plan_manifest_entries(
            super::scan_manifest_planner::ManifestPlanningInput {
                table: self.table,
                snapshot,
                partition_filter: self.partition_filter.as_ref(),
                data_predicates: &self.data_predicates,
                bucket_predicate: self.bucket_predicate.as_ref(),
                scan_all_files: self.scan_all_files,
            },
            trace,
        )
        .await?;
        Ok(entries)
    }
```

Remove now-unused manifest helper definitions and imports from `table_scan.rs`. Keep `MANIFEST_DIR` in `table_scan.rs` only if split planning still uses it before Task 3.

- [ ] **Step 6: Move manifest tests**

Move the existing `test_merge_manifest_entries_keeps_in_place_upgraded_file` from `table_scan.rs` into `scan_manifest_planner.rs`. Keep the same assertion data. Update imports so the test builds inside the new module.

- [ ] **Step 7: Run focused tests**

Run:

```bash
rtk cargo test -p paimon manifest_planner_exposes_merge_entry_behavior
rtk cargo test -p paimon test_merge_manifest_entries_keeps_in_place_upgraded_file
rtk cargo test -p paimon table_scan
```

Expected: PASS.

- [ ] **Step 8: Commit manifest planner extraction**

Run:

```bash
rtk git add crates/paimon/src/table/mod.rs crates/paimon/src/table/table_scan.rs crates/paimon/src/table/scan_manifest_planner.rs
rtk git commit -m "refactor: extract scan manifest planner"
```

Expected: commit succeeds.

---

### Task 3: Extract Split Planning Internal Module

**Files:**
- Modify: `crates/paimon/src/table/mod.rs`
- Modify: `crates/paimon/src/table/table_scan.rs`
- Create: `crates/paimon/src/table/scan_split_planner.rs`

**Interfaces:**
- Consumes: `Table`, resolved `Snapshot`, live `ManifestEntry` list, raw scan predicates, row ranges, projected read field IDs, limit, and `scan_all_files`.
- Produces: `pub(super) async fn plan_splits(input: SplitPlanningInput<'_>, trace: Option<&mut ScanTrace>) -> crate::Result<Plan>`.

- [ ] **Step 1: Write the failing module test**

Add the module declaration in `crates/paimon/src/table/mod.rs`:

```rust
mod scan_split_planner;
```

Create `crates/paimon/src/table/scan_split_planner.rs` with this module header and failing test:

```rust
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#[cfg(test)]
mod tests {
    #[test]
    fn split_planner_exposes_limit_pushdown_behavior() {
        let _ = super::LimitPushdownAccumulator::new;
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
rtk cargo test -p paimon split_planner_exposes_limit_pushdown_behavior
```

Expected: FAIL because `LimitPushdownAccumulator` is not defined in `scan_split_planner`.

- [ ] **Step 3: Move split-owned helpers into the new module**

Move these items from `table_scan.rs` into `scan_split_planner.rs`:

- `LimitPushdownResult`
- `LimitPushdownAccumulator`
- `BucketDataFileGroups`
- `global_index_detail_data_ranges`
- `is_system_field_id`
- `is_system_field_name`
- `is_vector_store_file_name`
- `is_normal_data_file`
- `DataFileFieldIdsCache`
- `data_evolution_representative_file`
- `resolve_data_file_field_ids`
- `data_file_field_ids`
- `prune_data_evolution_group_by_read_fields`
- `build_deletion_files_map`

Add imports required by the moved code:

```rust
use super::bin_pack::split_for_batch;
use super::merge_tree_split_generator::{
    merge_tree_split_for_batch, KeyComparator, SplitGroup,
};
use super::scan_manifest_planner::stats_pruning_predicates;
use super::source::{
    any_range_overlaps_file, intersect_ranges_with_file, merge_row_ranges, DataSplit,
    DataSplitBuilder, DeletionFile, PartitionBucket, Plan, RowRange,
};
use super::stats_filter::{
    data_evolution_group_matches_predicates, data_file_matches_predicates_for_table,
    group_by_overlapping_row_id, ResolvedStatsSchema,
};
use super::{ScanTrace, Table};
use crate::spec::{
    avro::SharedSchemaCache, bucket_dir_name, BinaryRow, CoreOptions, DataField, DataFileMeta,
    GlobalIndexSearchMode, IndexManifest, IndexManifestEntry, ManifestEntry, PartitionComputer,
    Predicate, Snapshot, ROW_ID_FIELD_ID, ROW_ID_FIELD_NAME, SEQUENCE_NUMBER_FIELD_ID,
    SEQUENCE_NUMBER_FIELD_NAME, VALUE_KIND_FIELD_ID, VALUE_KIND_FIELD_NAME,
};
use crate::table::schema_manager::SchemaManager;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
```

Define constants in the new module:

```rust
const MANIFEST_DIR: &str = "manifest";
const INDEX_DIR: &str = "index";
```

- [ ] **Step 4: Add the split-planning interface**

Add this interface to `scan_split_planner.rs`:

```rust
pub(super) struct SplitPlanningInput<'a> {
    pub(super) table: &'a Table,
    pub(super) snapshot: Snapshot,
    pub(super) entries: Vec<ManifestEntry>,
    pub(super) data_predicates: &'a [Predicate],
    pub(super) row_ranges: Option<Vec<RowRange>>,
    pub(super) projected_read_field_ids: Option<&'a HashSet<i32>>,
    pub(super) limit: Option<usize>,
    pub(super) scan_all_files: bool,
}

pub(super) async fn plan_splits(
    input: SplitPlanningInput<'_>,
    trace: Option<&mut ScanTrace>,
) -> crate::Result<Plan> {
    if input.entries.is_empty() {
        if let Some(trace) = trace {
            trace.record_final_plan(0, 0, 0);
        }
        return Ok(Plan::new(Vec::new()));
    }

    SplitPlanner { input, trace }.plan().await
}
```

Implement `SplitPlanner` as the owner of the moved `plan_snapshot` body. Move the current `TableScan::plan_snapshot` implementation into `SplitPlanner::plan` in the same edit that adds this type. The moved body starts after `plan_manifest_entries_with_trace` has returned entries, so the new method begins with the local setup that currently follows the `entries.is_empty()` check.

```rust
struct SplitPlanner<'a, 't> {
    input: SplitPlanningInput<'a>,
    trace: Option<&'t mut ScanTrace>,
}

impl SplitPlanner<'_, '_> {
    async fn plan(self) -> crate::Result<Plan> {
        let table = self.input.table;
        let snapshot = self.input.snapshot;
        let entries = self.input.entries;
        let data_predicates = self.input.data_predicates;
        let row_ranges = self.input.row_ranges;
        let projected_read_field_ids = self.input.projected_read_field_ids;
        let limit = self.input.limit;
        let scan_all_files = self.input.scan_all_files;
        let mut trace = self.trace;

        // Paste the current split-generation body from TableScan::plan_snapshot here,
        // replacing self.table / self.data_predicates / self.row_ranges / self.limit /
        // self.scan_all_files references with the locals above.
    }
}
```

The body must compile in the same commit that introduces `SplitPlanner`; do not leave a panic branch or partial implementation in the module.

- [ ] **Step 5: Wire `TableScan::plan_snapshot` to the new module**

Replace `TableScan::plan_snapshot` body in `table_scan.rs` with:

```rust
    async fn plan_snapshot(
        &self,
        snapshot: Snapshot,
        data_evolution_read_field_ids: Option<&HashSet<i32>>,
        mut trace: Option<&mut ScanTrace>,
    ) -> crate::Result<Plan> {
        let entries = self
            .plan_manifest_entries_with_trace(&snapshot, trace.as_deref_mut())
            .await?;
        super::scan_split_planner::plan_splits(
            super::scan_split_planner::SplitPlanningInput {
                table: self.table,
                snapshot,
                entries,
                data_predicates: &self.data_predicates,
                row_ranges: self.row_ranges.clone(),
                projected_read_field_ids: data_evolution_read_field_ids,
                limit: self.limit,
                scan_all_files: self.scan_all_files,
            },
            trace,
        )
        .await
    }
```

Remove moved helper definitions and unused imports from `table_scan.rs`.

- [ ] **Step 6: Move split tests**

Move these focused tests from `table_scan.rs` into `scan_split_planner.rs`:

- `test_plan_with_trace_records_limit_early_stop_during_split_construction`
- tests for `LimitPushdownAccumulator`
- tests for `prune_data_evolution_group_by_read_fields`
- tests for `should_skip_level_zero_for_scan` only if still split-owned; if Task 2 keeps it manifest-owned, leave those tests in `scan_manifest_planner.rs`

Keep the same assertions and fixture helpers. Update imports for the new module.

- [ ] **Step 7: Run focused tests**

Run:

```bash
rtk cargo test -p paimon split_planner_exposes_limit_pushdown_behavior
rtk cargo test -p paimon test_plan_with_trace_records_limit_early_stop_during_split_construction
rtk cargo test -p paimon table_scan
```

Expected: PASS.

- [ ] **Step 8: Commit split planner extraction**

Run:

```bash
rtk git add crates/paimon/src/table/mod.rs crates/paimon/src/table/table_scan.rs crates/paimon/src/table/scan_split_planner.rs
rtk git commit -m "refactor: extract scan split planner"
```

Expected: commit succeeds.

---

### Task 4: Verify Behavior and Compare Benchmarks

**Files:**
- No required source changes.
- Optional local-only benchmark logs under `/tmp`, not committed.

**Interfaces:**
- Consumes: benchmark target from Task 1 and refactored modules from Tasks 2-3.
- Produces: final comparison summary for the user.

- [ ] **Step 1: Run core scan tests**

Run:

```bash
rtk cargo test -p paimon table_scan
```

Expected: PASS.

- [ ] **Step 2: Run DataFusion scan trace regression tests**

Run:

```bash
rtk cargo test -p paimon-datafusion --test scan_pruning_trace
```

Expected: PASS.

- [ ] **Step 3: Run integration limit pushdown regression test**

Run:

```bash
rtk cargo test -p paimon-integration-tests test_limit_pushdown
```

Expected: PASS.

- [ ] **Step 4: Run final benchmark**

Run:

```bash
rtk cargo bench -p paimon --bench table_scan_planning
```

Expected: PASS with the same four benchmark cases from Task 1.

- [ ] **Step 5: Compare before and after**

Prepare the final report with:

```text
Benchmark cases:
- append_full_scan_many_files: baseline line copied from Task 1 Step 7; after line copied from Task 4 Step 4
- append_partition_pruned: baseline line copied from Task 1 Step 7; after line copied from Task 4 Step 4
- pk_bucket_pruned: baseline line copied from Task 1 Step 7; after line copied from Task 4 Step 4
- append_limit_pushdown: baseline line copied from Task 1 Step 7; after line copied from Task 4 Step 4

Trace stability:
- Existing scan_pruning_trace tests: PASS
- Existing limit pushdown integration test: PASS
- Existing table_scan unit tests: PASS

Architecture result:
- table_scan.rs coordinates planning
- scan_manifest_planner.rs owns manifest pruning
- scan_split_planner.rs owns split generation
```

- [ ] **Step 6: Commit any verification-only doc updates if created**

If no source or doc files changed after Task 3, skip this step. If a committed comparison note is added, run:

```bash
rtk git add docs/superpowers/specs/2026-07-08-table-scan-planning-bench-design.md
rtk git commit -m "docs: record table scan planning benchmark comparison"
```

Expected: commit succeeds only when a comparison note file exists.

---

## Self-Review Checklist

- Spec coverage:
  - Criterion benchmark: Task 1.
  - Baseline capture: Task 1 Step 7.
  - Manifest planning internal module: Task 2.
  - Split planning internal module: Task 3.
  - Tests and benchmark comparison: Task 4.

- Public interface stability:
  - `ReadBuilder` methods are not changed.
  - `TableScan::plan`, `TableScan::plan_with_trace`, and `TableScan::plan_manifest_entries` remain callable.
  - `Plan` and `DataSplit` are not changed.

- Commands:
  - All shell commands use `rtk`.
  - Benchmark command targets `paimon` and `table_scan_planning`.
  - Test commands use crate names confirmed by `cargo metadata`.
