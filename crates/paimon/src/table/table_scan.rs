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

//! TableScan for full table scan.
//!
//! Reference: [pypaimon.read.table_scan.TableScan](https://github.com/apache/paimon/blob/release-1.3/paimon-python/pypaimon/read/table_scan.py)
//! and [FullStartingScanner](https://github.com/apache/paimon/blob/release-1.3/paimon-python/pypaimon/read/scanner/full_starting_scanner.py).

use super::partition_filter::PartitionFilter;
use super::Table;
use crate::spec::{CoreOptions, ManifestEntry, Predicate, Snapshot};
use crate::table::source::{Plan, RowRange};
use crate::table::ScanTrace;
use crate::table::SnapshotManager;
use std::collections::HashSet;

/// TableScan for full table scan (no incremental, no predicate).
///
/// Reference: [pypaimon.read.table_scan.TableScan](https://github.com/apache/paimon/blob/master/paimon-python/pypaimon/read/table_scan.py)
#[derive(Debug, Clone)]
pub struct TableScan<'a> {
    table: &'a Table,
    partition_filter: Option<PartitionFilter>,
    data_predicates: Vec<Predicate>,
    bucket_predicate: Option<Predicate>,
    /// Optional limit on the number of rows to return.
    /// When set, the scan will try to return only enough splits to satisfy the limit.
    limit: Option<usize>,
    row_ranges: Option<Vec<RowRange>>,
    /// When true, disables level-0 filtering so all files are visible.
    /// Used by non-read paths (overwrite, truncate, writer restore) that need
    /// the complete file set. Normal read scans leave this as `false`.
    scan_all_files: bool,
    projected_read_field_ids: Option<HashSet<i32>>,
}

impl<'a> TableScan<'a> {
    pub(crate) fn new(
        table: &'a Table,
        partition_filter: Option<PartitionFilter>,
        data_predicates: Vec<Predicate>,
        bucket_predicate: Option<Predicate>,
        limit: Option<usize>,
        row_ranges: Option<Vec<RowRange>>,
    ) -> Self {
        Self {
            table,
            partition_filter,
            data_predicates,
            bucket_predicate,
            limit,
            row_ranges,
            scan_all_files: false,
            projected_read_field_ids: None,
        }
    }

    /// Disable level-0 filtering so all files are visible.
    ///
    /// Used by non-read paths (overwrite, truncate, writer restore) that need
    /// the complete file set regardless of merge engine or DV settings.
    pub fn with_scan_all_files(mut self) -> Self {
        self.scan_all_files = true;
        self.projected_read_field_ids = None;
        self
    }

    /// Set row ranges for scan-time filtering.
    ///
    /// This replaces any existing row_ranges. Typically used to inject
    /// results from global index lookups (e.g. full-text search).
    pub fn with_row_ranges(mut self, ranges: Vec<RowRange>) -> Self {
        self.row_ranges = if ranges.is_empty() {
            None
        } else {
            Some(ranges)
        };
        self
    }

    pub(super) fn with_projected_read_field_ids(
        mut self,
        projected_read_field_ids: Option<HashSet<i32>>,
    ) -> Self {
        self.projected_read_field_ids = projected_read_field_ids;
        self
    }

    /// Plan the full scan: resolve snapshot (via options or latest), then read manifests and build DataSplits.
    ///
    /// Time travel is resolved from table options:
    /// - only one of `scan.version`, `scan.timestamp-millis`,
    ///   `scan.snapshot-id`, `scan.tag-name` may be set
    /// - `scan.version` → tag name (if exists) → snapshot id (if parseable) →
    ///   error (ambiguous by design, like SQL `VERSION AS OF`)
    /// - `scan.snapshot-id` → snapshot id only (never a tag lookup)
    /// - `scan.tag-name` → tag name only (never parsed as a snapshot id)
    /// - `scan.timestamp-millis` → find the latest snapshot <= that timestamp
    /// - otherwise → read the latest snapshot
    ///
    /// Reference: [TimeTravelUtil.tryTravelToSnapshot](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/table/source/snapshot/TimeTravelUtil.java)
    /// for `scan.version`; the strict selectors mirror Java's typed
    /// `scan.snapshot-id` / `scan.tag-name` handling.
    pub async fn plan(&self) -> crate::Result<Plan> {
        self.ensure_query_auth_allowed()?;
        let data_evolution_read_field_ids = self.projected_read_field_ids()?;
        let snapshot = match self.resolve_snapshot().await? {
            Some(snapshot) => snapshot,
            None => return Ok(Plan::new(Vec::new())),
        };
        self.plan_snapshot(snapshot, data_evolution_read_field_ids.as_ref(), None)
            .await
    }

    /// Plan the full scan and return metadata-pruning trace counters.
    pub async fn plan_with_trace(&self) -> crate::Result<(Plan, ScanTrace)> {
        self.ensure_query_auth_allowed()?;
        let data_evolution_read_field_ids = self.projected_read_field_ids()?;
        let mut trace = ScanTrace {
            limit: self.limit,
            ..Default::default()
        };
        let snapshot = match self.resolve_snapshot().await? {
            Some(snapshot) => snapshot,
            None => return Ok((Plan::new(Vec::new()), trace)),
        };
        trace.snapshot_id = Some(snapshot.id());
        let plan = self
            .plan_snapshot(
                snapshot,
                data_evolution_read_field_ids.as_ref(),
                Some(&mut trace),
            )
            .await?;
        Ok((plan, trace))
    }

    /// Fail closed for a `query-auth.enabled` table: scan planning — including
    /// `with_scan_all_files`, which read-facing system tables like `files` use —
    /// exposes file paths, row counts, and stats the client can't authorize.
    fn ensure_query_auth_allowed(&self) -> crate::Result<()> {
        CoreOptions::new(self.table.schema().options()).ensure_read_authorized()
    }

    fn projected_read_field_ids(&self) -> crate::Result<Option<HashSet<i32>>> {
        Ok(self.projected_read_field_ids.clone())
    }

    async fn resolve_snapshot(&self) -> crate::Result<Option<Snapshot>> {
        // A table copy produced by `copy_with_time_travel` already resolved
        // the selector in its options; reuse it instead of re-reading
        // tag/snapshot files on every plan.
        if let Some(snapshot) = self.table.travel_snapshot() {
            return Ok(Some(snapshot.clone()));
        }
        // A time-travelled schema without its resolved snapshot means the
        // selector was changed after the travel (`copy_with_options`).
        // Resolving the new selector here would evolve a different snapshot's
        // files to the stale historical schema, so fail instead.
        if self.table.is_time_traveled() {
            return Err(crate::Error::DataInvalid {
                message: "Table options changed after time travel; \
                          use copy_with_time_travel to re-resolve the snapshot and schema"
                    .to_string(),
                source: None,
            });
        }

        let file_io = self.table.file_io();
        let table_path = self.table.location();

        match super::time_travel::travel_to_snapshot(
            file_io,
            table_path,
            self.table.schema().options(),
        )
        .await?
        {
            Some(snapshot) => Ok(Some(snapshot)),
            None => {
                let snapshot_manager =
                    SnapshotManager::new(file_io.clone(), table_path.to_string());
                snapshot_manager.get_latest_snapshot().await
            }
        }
    }

    /// Read all manifest entries from a snapshot, applying filters and merging.
    ///
    /// This is the shared entry point used by both `plan_snapshot` (scan) and
    /// `TableCommit` (overwrite). Filters include partition predicate, data
    /// predicates, and bucket predicate.
    pub(crate) async fn plan_manifest_entries(
        &self,
        snapshot: &Snapshot,
    ) -> crate::Result<Vec<ManifestEntry>> {
        self.plan_manifest_entries_with_trace(snapshot, None).await
    }

    async fn plan_manifest_entries_with_trace(
        &self,
        snapshot: &Snapshot,
        trace: Option<&mut ScanTrace>,
    ) -> crate::Result<Vec<ManifestEntry>> {
        super::scan_manifest_planner::plan_manifest_entries(
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
        .await
    }

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
}

#[cfg(test)]
mod tests {
    use crate::catalog::Identifier;
    use crate::io::FileIOBuilder;
    use crate::spec::{
        stats::BinaryTableStats, ArrayType, BinaryRowBuilder, BucketFunctionType, DataField,
        DataFileMeta, DataType, Datum, IntType, Predicate, PredicateBuilder, PredicateOperator,
        Schema as PaimonSchema, TableSchema, VarCharType,
    };
    use crate::table::bucket_filter::{compute_target_buckets, extract_predicate_for_keys};
    use crate::table::partition_filter::PartitionFilter;
    use crate::table::scan_manifest_planner::should_skip_level_zero_for_scan;
    use crate::table::stats_filter::{
        data_evolution_group_matches_predicates, data_file_matches_predicates,
        group_by_overlapping_row_id,
    };
    use crate::table::{CommitMessage, Table, TableCommit};
    use crate::Error;
    use chrono::{DateTime, Utc};
    use std::collections::{HashMap, HashSet};

    /// Helper to build a DataFileMeta with data evolution fields.
    fn make_evo_file(
        name: &str,
        file_size: i64,
        row_count: i64,
        max_seq: i64,
        first_row_id: Option<i64>,
    ) -> DataFileMeta {
        DataFileMeta {
            file_name: name.to_string(),
            file_size,
            row_count,
            min_key: Vec::new(),
            max_key: Vec::new(),
            key_stats: BinaryTableStats::new(Vec::new(), Vec::new(), Vec::new()),
            value_stats: BinaryTableStats::new(Vec::new(), Vec::new(), Vec::new()),
            min_sequence_number: 0,
            max_sequence_number: max_seq,
            schema_id: 0,
            level: 0,
            extra_files: Vec::new(),
            creation_time: DateTime::<Utc>::from_timestamp(0, 0),
            delete_row_count: None,
            embedded_index: None,
            first_row_id,
            write_cols: None,
            external_path: None,
            file_source: None,
            value_stats_cols: None,
        }
    }

    fn file_names(groups: &[Vec<DataFileMeta>]) -> Vec<Vec<&str>> {
        groups
            .iter()
            .map(|g| g.iter().map(|f| f.file_name.as_str()).collect())
            .collect()
    }

    fn int_stats_row(value: Option<i32>) -> Vec<u8> {
        let mut builder = BinaryRowBuilder::new(1);
        match value {
            Some(value) => builder.write_int(0, value),
            None => builder.set_null_at(0),
        }
        builder.build_serialized()
    }

    fn partition_string_field() -> Vec<DataField> {
        vec![DataField::new(
            0,
            "dt".to_string(),
            DataType::VarChar(VarCharType::default()),
        )]
    }

    fn int_field() -> Vec<DataField> {
        vec![DataField::new(
            0,
            "id".to_string(),
            DataType::Int(IntType::new()),
        )]
    }

    fn test_data_file_meta(
        min_values: Vec<u8>,
        max_values: Vec<u8>,
        null_counts: Vec<Option<i64>>,
        row_count: i64,
    ) -> DataFileMeta {
        test_data_file_meta_with_schema(
            min_values,
            max_values,
            null_counts,
            row_count,
            0, // default schema_id
        )
    }

    fn test_data_file_meta_with_schema(
        min_values: Vec<u8>,
        max_values: Vec<u8>,
        null_counts: Vec<Option<i64>>,
        row_count: i64,
        schema_id: i64,
    ) -> DataFileMeta {
        DataFileMeta {
            file_name: "test.parquet".into(),
            file_size: 128,
            row_count,
            min_key: Vec::new(),
            max_key: Vec::new(),
            key_stats: BinaryTableStats::new(Vec::new(), Vec::new(), Vec::new()),
            value_stats: BinaryTableStats::new(min_values, max_values, null_counts),
            min_sequence_number: 0,
            max_sequence_number: 0,
            schema_id,
            level: 1,
            extra_files: Vec::new(),
            creation_time: Some(Utc::now()),
            delete_row_count: None,
            embedded_index: None,
            first_row_id: None,
            write_cols: None,
            external_path: None,
            file_source: None,
            value_stats_cols: None,
        }
    }

    fn scan_trace_test_table(table_path: &str) -> Table {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let schema = PaimonSchema::builder()
            .column("id", DataType::Int(IntType::new()))
            .build()
            .unwrap();
        let table_schema = TableSchema::new(0, &schema);
        Table::new(
            file_io,
            Identifier::new("test_db", "scan_trace"),
            table_path.to_string(),
            table_schema,
            None,
        )
    }

    fn scan_trace_small_split_table(table_path: &str) -> Table {
        scan_trace_test_table(table_path).copy_with_options(HashMap::from([
            ("source.split.target-size".to_string(), "1b".to_string()),
            ("source.split.open-file-cost".to_string(), "1b".to_string()),
        ]))
    }

    async fn setup_scan_trace_dirs(table: &Table) {
        table
            .file_io()
            .mkdirs(&format!("{}/snapshot/", table.location()))
            .await
            .unwrap();
        table
            .file_io()
            .mkdirs(&format!("{}/manifest/", table.location()))
            .await
            .unwrap();
    }

    fn stats_trace_file(name: &str, min_id: i32, max_id: i32) -> DataFileMeta {
        let mut file = test_data_file_meta(
            int_stats_row(Some(min_id)),
            int_stats_row(Some(max_id)),
            vec![Some(0)],
            2,
        );
        file.file_name = name.to_string();
        file
    }

    #[test]
    fn test_first_row_skips_level_zero_by_default() {
        assert!(should_skip_level_zero_for_scan(
            false,
            true,
            false,
            Ok(crate::spec::MergeEngine::FirstRow),
        ));
    }

    #[test]
    fn test_scan_all_files_disables_first_row_level_zero_skip() {
        assert!(!should_skip_level_zero_for_scan(
            true,
            true,
            false,
            Ok(crate::spec::MergeEngine::FirstRow),
        ));
    }

    #[test]
    fn test_partition_filter_decode_failure_fails_open() {
        let fields = partition_string_field();
        let predicate = PredicateBuilder::new(&fields)
            .equal("dt", Datum::String("2024-01-01".into()))
            .unwrap();

        // Range predicate to force Predicate variant (fail-open path)
        let filter = PartitionFilter::Predicate(predicate);
        assert!(filter.matches_entry(&[0xFF, 0x00]).unwrap());
    }

    #[test]
    fn test_partition_filter_eval_error_fails_fast() {
        let mut builder = BinaryRowBuilder::new(1);
        builder.write_string(0, "2024-01-01");
        let serialized = builder.build_serialized();

        let predicate = Predicate::Leaf {
            column: "dt".into(),
            index: 0,
            data_type: DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
            op: PredicateOperator::Eq,
            literals: vec![Datum::Int(42)],
        };

        let filter = PartitionFilter::Predicate(predicate);
        let err = filter
            .matches_entry(&serialized)
            .expect_err("eval_row error should propagate");

        assert!(
            matches!(&err, Error::Unsupported { message } if message.contains("extract_datum")),
            "Expected extract_datum unsupported error, got: {err:?}"
        );
    }

    const TEST_SCHEMA_ID: i64 = 0;
    fn test_schema_fields() -> Vec<DataField> {
        int_field()
    }

    #[test]
    fn test_group_by_overlapping_row_id_empty() {
        let result = group_by_overlapping_row_id(vec![]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_group_by_overlapping_row_id_no_row_ids() {
        let files = vec![
            make_evo_file("a", 10, 100, 1, None),
            make_evo_file("b", 10, 100, 2, None),
        ];
        let groups = group_by_overlapping_row_id(files);
        assert_eq!(file_names(&groups), vec![vec!["b"], vec!["a"]]);
    }

    #[test]
    fn test_group_by_overlapping_row_id_same_range() {
        let files = vec![
            make_evo_file("a", 10, 100, 2, Some(0)),
            make_evo_file("b", 10, 100, 1, Some(0)),
        ];
        let groups = group_by_overlapping_row_id(files);
        assert_eq!(groups.len(), 1);
        assert_eq!(file_names(&groups), vec![vec!["a", "b"]]);
    }

    #[test]
    fn test_group_by_overlapping_row_id_overlapping_ranges() {
        let files = vec![
            make_evo_file("a", 10, 100, 1, Some(0)),
            make_evo_file("b", 10, 100, 2, Some(50)),
        ];
        let groups = group_by_overlapping_row_id(files);
        assert_eq!(groups.len(), 1);
        assert_eq!(file_names(&groups), vec![vec!["a", "b"]]);
    }

    #[test]
    fn test_group_by_overlapping_row_id_non_overlapping() {
        let files = vec![
            make_evo_file("a", 10, 100, 1, Some(0)),
            make_evo_file("b", 10, 100, 2, Some(100)),
        ];
        let groups = group_by_overlapping_row_id(files);
        assert_eq!(groups.len(), 2);
        assert_eq!(file_names(&groups), vec![vec!["a"], vec!["b"]]);
    }

    #[test]
    fn test_group_by_overlapping_row_id_mixed() {
        let files = vec![
            make_evo_file("a", 10, 100, 1, Some(0)),
            make_evo_file("b", 10, 100, 2, Some(0)),
            make_evo_file("c", 10, 100, 3, None),
            make_evo_file("d", 10, 100, 4, Some(200)),
        ];
        let groups = group_by_overlapping_row_id(files);
        assert_eq!(
            file_names(&groups),
            vec![vec!["c"], vec!["b", "a"], vec!["d"]]
        );
    }

    #[test]
    fn test_group_by_overlapping_row_id_sorted_by_seq() {
        let files = vec![
            make_evo_file("a", 10, 100, 1, Some(0)),
            make_evo_file("b", 10, 100, 3, Some(0)),
            make_evo_file("c", 10, 100, 2, Some(0)),
        ];
        let groups = group_by_overlapping_row_id(files);
        assert_eq!(groups.len(), 1);
        assert_eq!(file_names(&groups), vec![vec!["b", "c", "a"]]);
    }

    #[test]
    fn test_data_file_matches_eq_prunes_out_of_range() {
        let fields = int_field();
        let file = test_data_file_meta(
            int_stats_row(Some(10)),
            int_stats_row(Some(20)),
            vec![Some(0)],
            5,
        );
        let predicate = PredicateBuilder::new(&fields)
            .equal("id", Datum::Int(30))
            .unwrap();

        assert!(!data_file_matches_predicates(
            &file,
            &[predicate],
            TEST_SCHEMA_ID,
            &test_schema_fields(),
        ));
    }

    #[tokio::test]
    async fn test_plan_with_trace_records_between_data_stats_pruning() {
        let table_path = "memory:/test_plan_with_trace_records_between_data_stats_pruning";
        let table = scan_trace_test_table(table_path);
        setup_scan_trace_dirs(&table).await;

        TableCommit::new(table.clone(), "scan-trace-test".to_string())
            .commit(vec![CommitMessage::new(
                BinaryRowBuilder::new(0).build_serialized(),
                0,
                vec![
                    stats_trace_file("stats-1.parquet", 1, 2),
                    stats_trace_file("stats-2.parquet", 10, 20),
                    stats_trace_file("stats-3.parquet", 100, 101),
                ],
            )])
            .await
            .unwrap();

        let fields = int_field();
        let pb = PredicateBuilder::new(&fields);
        let between = Predicate::and(vec![
            pb.greater_or_equal("id", Datum::Int(10)).unwrap(),
            pb.less_or_equal("id", Datum::Int(20)).unwrap(),
        ]);
        let mut reader = table.new_read_builder();
        reader.with_filter(between);
        let (_plan, trace) = reader.new_scan().plan_with_trace().await.unwrap();

        assert_eq!(
            trace.final_files, 1,
            "BETWEEN should keep only the overlapping stats range: {trace:?}"
        );
        assert!(
            trace.manifest_entries_pruned_by_data_stats >= 2,
            "BETWEEN should prune files outside the min/max range: {trace:?}"
        );
    }

    fn pk_stats_gate_table(table_path: &str) -> Table {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let schema = PaimonSchema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .primary_key(["id"])
            .option("bucket", "1")
            .build()
            .unwrap();
        Table::new(
            file_io,
            Identifier::new("test_db", "pk_stats_gate"),
            table_path.to_string(),
            TableSchema::new(0, &schema),
            None,
        )
    }

    fn two_int_stats_row(id: Option<i32>, value: Option<i32>) -> Vec<u8> {
        let mut builder = BinaryRowBuilder::new(2);
        match id {
            Some(id) => builder.write_int(0, id),
            None => builder.set_null_at(0),
        }
        match value {
            Some(value) => builder.write_int(1, value),
            None => builder.set_null_at(1),
        }
        builder.build_serialized()
    }

    fn pk_stats_file(name: &str, id_range: (i32, i32), value_range: (i32, i32)) -> DataFileMeta {
        let mut file = test_data_file_meta(
            two_int_stats_row(Some(id_range.0), Some(value_range.0)),
            two_int_stats_row(Some(id_range.1), Some(value_range.1)),
            vec![Some(0), Some(0)],
            2,
        );
        file.file_name = name.to_string();
        file
    }

    /// Merge reads combine versions of a key across files, so scan planning
    /// must not prune a PK table's files by NON-key conjuncts: dropping the
    /// file that holds the newest version resurrects an older version from a
    /// surviving file — an error no post-merge residual can repair. Key
    /// conjuncts stay safe (every version of a key shares the key columns)
    /// and must still prune.
    #[tokio::test]
    async fn test_pk_table_stats_pruning_ignores_non_key_conjuncts() {
        let table_path = "memory:/test_pk_stats_gate";
        let table = pk_stats_gate_table(table_path);
        setup_scan_trace_dirs(&table).await;

        // Both files cover key id=1; the newer version's value (50) falls
        // outside the value predicate while the older one (150) matches.
        TableCommit::new(table.clone(), "pk-gate-test".to_string())
            .commit(vec![CommitMessage::new(
                BinaryRowBuilder::new(0).build_serialized(),
                0,
                vec![
                    pk_stats_file("old-version.parquet", (1, 5), (100, 200)),
                    pk_stats_file("new-version.parquet", (1, 5), (10, 60)),
                ],
            )])
            .await
            .unwrap();

        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "value".to_string(), DataType::Int(IntType::new())),
        ];
        let pb = PredicateBuilder::new(&fields);

        // Non-key conjunct: must NOT prune any file of a PK table.
        let value_filter = pb.greater_than("value", Datum::Int(90)).unwrap();
        let mut reader = table.new_read_builder();
        reader.with_filter(value_filter);
        let (plan, trace) = reader.new_scan().plan_with_trace().await.unwrap();
        assert_eq!(
            trace.manifest_entries_pruned_by_data_stats, 0,
            "non-key conjuncts must not file-prune a PK table: {trace:?}"
        );
        let planned_files: usize = plan.splits().iter().map(|s| s.data_files().len()).sum();
        assert_eq!(
            planned_files, 2,
            "both versions must reach the merge reader"
        );

        // Key conjunct: still prunes (id=9 outside both files' key range).
        let key_filter = pb.equal("id", Datum::Int(9)).unwrap();
        let mut reader = table.new_read_builder();
        reader.with_filter(key_filter);
        let (_plan, trace) = reader.new_scan().plan_with_trace().await.unwrap();
        assert!(
            trace.manifest_entries_pruned_by_data_stats >= 2,
            "key conjuncts must still prune PK-table files: {trace:?}"
        );
    }

    /// `merge-engine=first-row` PK tables read raw (no merge on the read
    /// path: planned with `skip_level_zero`, read via `DataFileReader`), so
    /// pruning a file by a non-key conjunct cannot resurrect anything — it
    /// drops exactly the rows the raw path's exact residual filter would
    /// drop. The key-only gate must exempt first-row and keep full-predicate
    /// stats pruning, matching the split-generation path.
    #[tokio::test]
    async fn test_first_row_table_stats_pruning_keeps_non_key_conjuncts() {
        let table_path = "memory:/test_first_row_stats_gate";
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let schema = PaimonSchema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .primary_key(["id"])
            .option("bucket", "1")
            .option("merge-engine", "first-row")
            .build()
            .unwrap();
        let table = Table::new(
            file_io,
            Identifier::new("test_db", "first_row_stats_gate"),
            table_path.to_string(),
            TableSchema::new(0, &schema),
            None,
        );
        setup_scan_trace_dirs(&table).await;

        // Compacted (level 1) files: first-row planning skips level 0, so the
        // fixture files must sit above it to be planned at all. Distinct key
        // ranges; only file A's value range can match `value > 90`.
        TableCommit::new(table.clone(), "first-row-gate-test".to_string())
            .commit(vec![CommitMessage::new(
                BinaryRowBuilder::new(0).build_serialized(),
                0,
                vec![
                    pk_stats_file("file-a.parquet", (1, 5), (100, 200)),
                    pk_stats_file("file-b.parquet", (6, 9), (10, 60)),
                ],
            )])
            .await
            .unwrap();

        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "value".to_string(), DataType::Int(IntType::new())),
        ];
        let pb = PredicateBuilder::new(&fields);

        // Non-key conjunct: first-row reads raw, so full-predicate pruning
        // stays enabled — file-b (value stats [10, 60]) must be pruned.
        let value_filter = pb.greater_than("value", Datum::Int(90)).unwrap();
        let mut reader = table.new_read_builder();
        reader.with_filter(value_filter);
        let (plan, trace) = reader.new_scan().plan_with_trace().await.unwrap();
        assert!(
            trace.manifest_entries_pruned_by_data_stats >= 1,
            "first-row tables must keep full-predicate stats pruning: {trace:?}"
        );
        let planned_files: usize = plan.splits().iter().map(|s| s.data_files().len()).sum();
        assert_eq!(
            planned_files, 1,
            "only the value-matching file should be planned on first-row"
        );
    }

    #[tokio::test]
    async fn test_plan_with_trace_zero_limit_records_no_split_candidates() {
        let table_path = "memory:/test_plan_with_trace_zero_limit_records_no_split_candidates";
        let table = scan_trace_small_split_table(table_path);
        setup_scan_trace_dirs(&table).await;

        TableCommit::new(table.clone(), "scan-trace-zero-limit-test".to_string())
            .commit(vec![CommitMessage::new(
                BinaryRowBuilder::new(0).build_serialized(),
                0,
                vec![
                    stats_trace_file("zero-1.parquet", 1, 1),
                    stats_trace_file("zero-2.parquet", 2, 2),
                ],
            )])
            .await
            .unwrap();

        let mut reader = table.new_read_builder();
        reader.with_limit(0);
        let (plan, trace) = reader.new_scan().plan_with_trace().await.unwrap();

        assert!(plan.splits().is_empty());
        assert!(trace.limit_early_stopped);
        assert_eq!(trace.split_candidates_built, 0);
        assert_eq!(trace.final_splits, 0);
    }

    #[test]
    fn test_data_file_matches_in_prunes_when_all_literals_out_of_range() {
        let fields = int_field();
        let file = test_data_file_meta(
            int_stats_row(Some(10)),
            int_stats_row(Some(20)),
            vec![Some(0)],
            5,
        );
        let predicate = PredicateBuilder::new(&fields)
            .is_in("id", vec![Datum::Int(1), Datum::Int(30)])
            .unwrap();

        assert!(!data_file_matches_predicates(
            &file,
            &[predicate],
            TEST_SCHEMA_ID,
            &test_schema_fields(),
        ));
    }

    #[test]
    fn test_data_file_matches_in_keeps_when_any_literal_in_range() {
        let fields = int_field();
        let file = test_data_file_meta(
            int_stats_row(Some(10)),
            int_stats_row(Some(20)),
            vec![Some(0)],
            5,
        );
        let predicate = PredicateBuilder::new(&fields)
            .is_in("id", vec![Datum::Int(1), Datum::Int(15), Datum::Int(30)])
            .unwrap();

        assert!(data_file_matches_predicates(
            &file,
            &[predicate],
            TEST_SCHEMA_ID,
            &test_schema_fields(),
        ));
    }

    #[test]
    fn test_data_file_matches_in_prunes_all_null_file() {
        let fields = int_field();
        let file = test_data_file_meta(int_stats_row(None), int_stats_row(None), vec![Some(5)], 5);
        let predicate = PredicateBuilder::new(&fields)
            .is_in("id", vec![Datum::Int(10)])
            .unwrap();

        assert!(!data_file_matches_predicates(
            &file,
            &[predicate],
            TEST_SCHEMA_ID,
            &test_schema_fields(),
        ));
    }

    #[test]
    fn test_data_file_matches_in_with_corrupt_stats_fails_open() {
        let fields = int_field();
        let file = test_data_file_meta(Vec::new(), Vec::new(), vec![Some(0)], 5);
        let predicate = PredicateBuilder::new(&fields)
            .is_in("id", vec![Datum::Int(30)])
            .unwrap();

        assert!(data_file_matches_predicates(
            &file,
            &[predicate],
            TEST_SCHEMA_ID,
            &test_schema_fields(),
        ));
    }

    #[test]
    fn test_data_file_matches_in_with_inverted_stats_fails_open() {
        let fields = int_field();
        let file = test_data_file_meta(
            int_stats_row(Some(20)),
            int_stats_row(Some(10)),
            vec![Some(0)],
            5,
        );
        let predicate = PredicateBuilder::new(&fields)
            .is_in("id", vec![Datum::Int(15)])
            .unwrap();

        assert!(data_file_matches_predicates(
            &file,
            &[predicate],
            TEST_SCHEMA_ID,
            &test_schema_fields(),
        ));
    }

    #[test]
    fn test_data_file_matches_not_in_fails_open() {
        let fields = int_field();
        let file = test_data_file_meta(
            int_stats_row(Some(10)),
            int_stats_row(Some(20)),
            vec![Some(0)],
            5,
        );
        let predicate = PredicateBuilder::new(&fields)
            .is_not_in("id", vec![Datum::Int(10), Datum::Int(20)])
            .unwrap();

        assert!(data_file_matches_predicates(
            &file,
            &[predicate],
            TEST_SCHEMA_ID,
            &test_schema_fields(),
        ));
    }

    #[test]
    fn test_data_file_matches_is_null_prunes_when_null_count_is_zero() {
        let fields = int_field();
        let file = test_data_file_meta(
            int_stats_row(Some(10)),
            int_stats_row(Some(20)),
            vec![Some(0)],
            5,
        );
        let predicate = PredicateBuilder::new(&fields).is_null("id").unwrap();

        assert!(!data_file_matches_predicates(
            &file,
            &[predicate],
            TEST_SCHEMA_ID,
            &test_schema_fields(),
        ));
    }

    #[test]
    fn test_data_file_matches_is_not_null_prunes_all_null_file() {
        let fields = int_field();
        let file = test_data_file_meta(int_stats_row(None), int_stats_row(None), vec![Some(5)], 5);
        let predicate = PredicateBuilder::new(&fields).is_not_null("id").unwrap();

        assert!(!data_file_matches_predicates(
            &file,
            &[predicate],
            TEST_SCHEMA_ID,
            &test_schema_fields(),
        ));
    }

    #[test]
    fn test_data_file_matches_or_prunes_when_no_child_matches() {
        let fields = int_field();
        let file = test_data_file_meta(
            int_stats_row(Some(10)),
            int_stats_row(Some(20)),
            vec![Some(0)],
            5,
        );
        let pb = PredicateBuilder::new(&fields);
        let predicate = Predicate::or(vec![
            pb.less_than("id", Datum::Int(5)).unwrap(),
            pb.greater_than("id", Datum::Int(25)).unwrap(),
        ]);

        assert!(!data_file_matches_predicates(
            &file,
            &[predicate],
            TEST_SCHEMA_ID,
            &test_schema_fields(),
        ));
    }

    #[test]
    fn test_data_file_matches_or_keeps_when_any_child_matches() {
        let fields = int_field();
        let file = test_data_file_meta(
            int_stats_row(Some(10)),
            int_stats_row(Some(20)),
            vec![Some(0)],
            5,
        );
        let pb = PredicateBuilder::new(&fields);
        let predicate = Predicate::or(vec![
            pb.less_than("id", Datum::Int(15)).unwrap(),
            pb.greater_than("id", Datum::Int(25)).unwrap(),
        ]);

        assert!(data_file_matches_predicates(
            &file,
            &[predicate],
            TEST_SCHEMA_ID,
            &test_schema_fields(),
        ));
    }

    #[test]
    fn test_data_file_matches_not_fails_open() {
        let fields = int_field();
        let file = test_data_file_meta(
            int_stats_row(Some(10)),
            int_stats_row(Some(20)),
            vec![Some(0)],
            5,
        );
        let predicate = Predicate::negate(
            PredicateBuilder::new(&fields)
                .less_than("id", Datum::Int(5))
                .unwrap(),
        );

        assert!(data_file_matches_predicates(
            &file,
            &[predicate],
            TEST_SCHEMA_ID,
            &test_schema_fields(),
        ));
    }

    #[test]
    fn test_data_evolution_group_matches_or_prunes_when_no_child_matches() {
        let fields = int_field();
        let file = test_data_file_meta(
            int_stats_row(Some(10)),
            int_stats_row(Some(20)),
            vec![Some(0)],
            5,
        );
        let pb = PredicateBuilder::new(&fields);
        let predicate = Predicate::or(vec![
            pb.less_than("id", Datum::Int(5)).unwrap(),
            pb.greater_than("id", Datum::Int(25)).unwrap(),
        ]);

        assert!(!data_evolution_group_matches_predicates(
            &[file],
            &[predicate],
            &fields,
        ));
    }

    #[test]
    fn test_data_evolution_group_matches_or_keeps_when_any_child_matches() {
        let fields = int_field();
        let file = test_data_file_meta(
            int_stats_row(Some(10)),
            int_stats_row(Some(20)),
            vec![Some(0)],
            5,
        );
        let pb = PredicateBuilder::new(&fields);
        let predicate = Predicate::or(vec![
            pb.less_than("id", Datum::Int(15)).unwrap(),
            pb.greater_than("id", Datum::Int(25)).unwrap(),
        ]);

        assert!(data_evolution_group_matches_predicates(
            &[file],
            &[predicate],
            &fields,
        ));
    }

    #[test]
    fn test_data_file_matches_corrupt_stats_fails_open() {
        let fields = int_field();
        let file = test_data_file_meta(Vec::new(), Vec::new(), vec![Some(0)], 5);
        let predicate = PredicateBuilder::new(&fields)
            .equal("id", Datum::Int(30))
            .unwrap();

        assert!(data_file_matches_predicates(
            &file,
            &[predicate],
            TEST_SCHEMA_ID,
            &test_schema_fields(),
        ));
    }

    #[test]
    fn test_data_file_matches_schema_mismatch_fails_open() {
        let fields = int_field();
        let file = test_data_file_meta_with_schema(
            int_stats_row(Some(10)),
            int_stats_row(Some(20)),
            vec![Some(0)],
            5,
            5,
        );
        let predicate = PredicateBuilder::new(&fields)
            .equal("id", Datum::Int(30))
            .unwrap();

        assert!(data_file_matches_predicates(
            &file,
            &[predicate],
            TEST_SCHEMA_ID,
            &test_schema_fields(),
        ));
    }

    #[test]
    fn test_data_file_matches_always_false_prunes_despite_schema_mismatch() {
        let file = test_data_file_meta_with_schema(
            int_stats_row(Some(10)),
            int_stats_row(Some(20)),
            vec![Some(0)],
            5,
            99,
        );

        assert!(!data_file_matches_predicates(
            &file,
            &[Predicate::AlwaysFalse],
            TEST_SCHEMA_ID,
            &test_schema_fields(),
        ));
    }

    #[test]
    fn test_data_file_matches_always_true_keeps_file_despite_schema_mismatch() {
        let file = test_data_file_meta_with_schema(
            int_stats_row(Some(10)),
            int_stats_row(Some(20)),
            vec![Some(0)],
            5,
            99,
        );

        assert!(data_file_matches_predicates(
            &file,
            &[Predicate::AlwaysTrue],
            TEST_SCHEMA_ID,
            &test_schema_fields(),
        ));
    }

    // ======================== Bucket predicate filtering ========================

    fn bucket_key_fields() -> Vec<DataField> {
        vec![DataField::new(
            0,
            "id".to_string(),
            DataType::Int(IntType::new()),
        )]
    }

    #[test]
    fn test_extract_predicate_for_keys_eq() {
        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(
                1,
                "name".to_string(),
                DataType::VarChar(VarCharType::default()),
            ),
        ];
        let pb = PredicateBuilder::new(&fields);
        let filter = Predicate::and(vec![
            pb.equal("id", Datum::Int(42)).unwrap(),
            pb.equal("name", Datum::String("alice".into())).unwrap(),
        ]);

        let keys = vec!["id".to_string()];
        let extracted = extract_predicate_for_keys(&filter, &fields, &keys);
        assert!(extracted.is_some());
        match extracted.unwrap() {
            Predicate::Leaf {
                column, index, op, ..
            } => {
                assert_eq!(column, "id");
                assert_eq!(index, 0); // remapped to key index
                assert_eq!(op, PredicateOperator::Eq);
            }
            other => panic!("expected Leaf, got {other:?}"),
        }
    }

    #[test]
    fn test_extract_predicate_for_keys_no_match() {
        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(
                1,
                "name".to_string(),
                DataType::VarChar(VarCharType::default()),
            ),
        ];
        let pb = PredicateBuilder::new(&fields);
        let filter = pb.equal("name", Datum::String("alice".into())).unwrap();

        let keys = vec!["id".to_string()];
        let extracted = extract_predicate_for_keys(&filter, &fields, &keys);
        assert!(extracted.is_none());
    }

    #[test]
    fn test_compute_target_buckets_single_eq() {
        let fields = bucket_key_fields();
        // Build a bucket predicate (already projected to bucket key space, index=0)
        let pred = Predicate::Leaf {
            column: "id".into(),
            index: 0,
            data_type: DataType::Int(IntType::new()),
            op: PredicateOperator::Eq,
            literals: vec![Datum::Int(42)],
        };

        let buckets = compute_target_buckets(&pred, &fields, BucketFunctionType::Default, 4);
        assert!(buckets.is_some());
        let buckets = buckets.unwrap();
        assert_eq!(buckets.len(), 1);
        // The bucket should be deterministic
        let bucket = *buckets.iter().next().unwrap();
        assert!((0..4).contains(&bucket));
    }

    #[test]
    fn test_compute_target_buckets_in_predicate() {
        let fields = bucket_key_fields();
        let pred = Predicate::Leaf {
            column: "id".into(),
            index: 0,
            data_type: DataType::Int(IntType::new()),
            op: PredicateOperator::In,
            literals: vec![Datum::Int(1), Datum::Int(2), Datum::Int(3)],
        };

        let buckets = compute_target_buckets(&pred, &fields, BucketFunctionType::Default, 4);
        assert!(buckets.is_some());
        let buckets = buckets.unwrap();
        // Should have at most 3 buckets (could be fewer if some hash to the same bucket)
        assert!(!buckets.is_empty());
        assert!(buckets.len() <= 3);
        for &b in &buckets {
            assert!((0..4).contains(&b));
        }
    }

    #[test]
    fn test_compute_target_buckets_range_returns_none() {
        let fields = bucket_key_fields();
        let pred = Predicate::Leaf {
            column: "id".into(),
            index: 0,
            data_type: DataType::Int(IntType::new()),
            op: PredicateOperator::Gt,
            literals: vec![Datum::Int(10)],
        };

        let buckets = compute_target_buckets(&pred, &fields, BucketFunctionType::Default, 4);
        assert!(
            buckets.is_none(),
            "Range predicates cannot determine target buckets"
        );
    }

    #[test]
    fn test_compute_target_buckets_composite_key() {
        let fields = vec![
            DataField::new(0, "a".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "b".to_string(), DataType::Int(IntType::new())),
        ];
        let pred = Predicate::And(vec![
            Predicate::Leaf {
                column: "a".into(),
                index: 0,
                data_type: DataType::Int(IntType::new()),
                op: PredicateOperator::Eq,
                literals: vec![Datum::Int(1)],
            },
            Predicate::Leaf {
                column: "b".into(),
                index: 1,
                data_type: DataType::Int(IntType::new()),
                op: PredicateOperator::Eq,
                literals: vec![Datum::Int(2)],
            },
        ]);

        let buckets = compute_target_buckets(&pred, &fields, BucketFunctionType::Default, 8);
        assert!(buckets.is_some());
        let buckets = buckets.unwrap();
        assert_eq!(buckets.len(), 1);
        let bucket = *buckets.iter().next().unwrap();
        assert!((0..8).contains(&bucket));
    }

    #[test]
    fn test_compute_target_buckets_partial_key_returns_none() {
        // Only one of two bucket key fields has an eq predicate
        let fields = vec![
            DataField::new(0, "a".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "b".to_string(), DataType::Int(IntType::new())),
        ];
        let pred = Predicate::Leaf {
            column: "a".into(),
            index: 0,
            data_type: DataType::Int(IntType::new()),
            op: PredicateOperator::Eq,
            literals: vec![Datum::Int(1)],
        };

        let buckets = compute_target_buckets(&pred, &fields, BucketFunctionType::Default, 8);
        assert!(
            buckets.is_none(),
            "Partial bucket key should not determine target buckets"
        );
    }

    #[test]
    fn test_compute_target_buckets_string_key() {
        let fields = vec![DataField::new(
            0,
            "name".to_string(),
            DataType::VarChar(VarCharType::default()),
        )];
        let pred = Predicate::Leaf {
            column: "name".into(),
            index: 0,
            data_type: DataType::VarChar(VarCharType::default()),
            op: PredicateOperator::Eq,
            literals: vec![Datum::String("alice".into())],
        };

        let buckets = compute_target_buckets(&pred, &fields, BucketFunctionType::Default, 4);
        assert!(buckets.is_some());
        let buckets = buckets.unwrap();
        assert_eq!(buckets.len(), 1);
        let bucket = *buckets.iter().next().unwrap();
        assert!((0..4).contains(&bucket));
    }

    #[test]
    fn test_compute_target_buckets_mod_function() {
        let fields = bucket_key_fields();
        let pred = Predicate::Leaf {
            column: "id".into(),
            index: 0,
            data_type: DataType::Int(IntType::new()),
            op: PredicateOperator::Eq,
            literals: vec![Datum::Int(-3)],
        };

        let buckets = compute_target_buckets(&pred, &fields, BucketFunctionType::Mod, 5);
        assert_eq!(buckets, Some(HashSet::from([2])));
    }

    #[test]
    fn test_compute_target_buckets_hive_function() {
        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(
                1,
                "name".to_string(),
                DataType::VarChar(VarCharType::default()),
            ),
        ];
        let pred = Predicate::And(vec![
            Predicate::Leaf {
                column: "id".into(),
                index: 0,
                data_type: DataType::Int(IntType::new()),
                op: PredicateOperator::Eq,
                literals: vec![Datum::Int(7)],
            },
            Predicate::Leaf {
                column: "name".into(),
                index: 1,
                data_type: DataType::VarChar(VarCharType::default()),
                op: PredicateOperator::Eq,
                literals: vec![Datum::String("hello".into())],
            },
        ]);

        let buckets = compute_target_buckets(&pred, &fields, BucketFunctionType::Hive, 8);
        assert_eq!(buckets, Some(HashSet::from([3])));
    }

    #[test]
    fn test_compute_target_buckets_is_null() {
        let fields = bucket_key_fields();
        let pred = Predicate::Leaf {
            column: "id".into(),
            index: 0,
            data_type: DataType::Int(IntType::new()),
            op: PredicateOperator::IsNull,
            literals: vec![],
        };

        let buckets = compute_target_buckets(&pred, &fields, BucketFunctionType::Default, 4);
        assert!(buckets.is_some(), "IsNull should determine a target bucket");
        let buckets = buckets.unwrap();
        assert_eq!(buckets.len(), 1);
        let bucket = *buckets.iter().next().unwrap();
        assert!((0..4).contains(&bucket));

        // Verify it matches the expected bucket from a null BinaryRow
        let mut builder = BinaryRowBuilder::new(1);
        builder.set_null_at(0);
        let expected = (builder.build().hash_code() % 4).abs();
        assert_eq!(bucket, expected);
    }

    #[test]
    fn test_compute_target_buckets_composite_key_with_null() {
        let fields = vec![
            DataField::new(0, "a".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "b".to_string(), DataType::Int(IntType::new())),
        ];
        // a = 1 AND b IS NULL
        let pred = Predicate::And(vec![
            Predicate::Leaf {
                column: "a".into(),
                index: 0,
                data_type: DataType::Int(IntType::new()),
                op: PredicateOperator::Eq,
                literals: vec![Datum::Int(1)],
            },
            Predicate::Leaf {
                column: "b".into(),
                index: 1,
                data_type: DataType::Int(IntType::new()),
                op: PredicateOperator::IsNull,
                literals: vec![],
            },
        ]);

        let buckets = compute_target_buckets(&pred, &fields, BucketFunctionType::Default, 8);
        assert!(
            buckets.is_some(),
            "Composite key with IsNull should determine a target bucket"
        );
        let buckets = buckets.unwrap();
        assert_eq!(buckets.len(), 1);
        let bucket = *buckets.iter().next().unwrap();
        assert!((0..8).contains(&bucket));
    }

    #[tokio::test]
    async fn test_plan_fails_closed_when_query_auth_enabled() {
        // Every scan-planning path must fail closed, including `with_scan_all_files`
        // (read-facing system tables like `files` use it to expose metadata).
        let table = crate::table::query_auth_table();
        let rb = table.new_read_builder();
        for scan in [rb.new_scan(), rb.new_scan().with_scan_all_files()] {
            let err = scan.plan().await.unwrap_err();
            assert!(
                matches!(err, crate::Error::Unsupported { ref message } if message.contains("query-auth.enabled")),
                "scan planning must fail closed (scan_all_files or not)"
            );
        }
    }

    #[tokio::test]
    async fn test_dynamic_option_cannot_disable_query_auth_at_plan() {
        // Copying the table with the option off must not weaken a stored `true`.
        let table =
            crate::table::query_auth_table().copy_with_options(std::collections::HashMap::from([
                ("query-auth.enabled".to_string(), "false".to_string()),
            ]));
        let err = table
            .new_read_builder()
            .new_scan()
            .plan()
            .await
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::Unsupported { ref message } if message.contains("query-auth.enabled")),
            "a dynamic override must not disable query-auth"
        );
    }
}
