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

use super::bucket_filter::compute_target_buckets;
use super::kv_file_reader::retain_primary_key_conjuncts;
use super::partition_filter::PartitionFilter;
use super::stats_filter::{data_file_matches_predicates, FileStatsRows};
use super::{ScanTrace, Table};
use crate::io::FileIO;
use crate::spec::{
    avro::SharedSchemaCache, BinaryRow, BucketFunctionType, CoreOptions, DataField, FileKind,
    ManifestEntry, Predicate, Snapshot,
};
use futures::{StreamExt, TryStreamExt};
use std::collections::{HashMap, HashSet};

const MANIFEST_DIR: &str = "manifest";

#[derive(Debug, Default)]
struct ManifestReadCounters {
    entries_read: usize,
    pruned_by_bucket: usize,
    pruned_by_partition: usize,
    after_entry_pruning: usize,
    pruned_by_level: usize,
    pruned_by_data_stats: usize,
    after_manifest_filters: usize,
}

impl ManifestReadCounters {
    fn merge(&mut self, other: Self) {
        self.entries_read += other.entries_read;
        self.pruned_by_bucket += other.pruned_by_bucket;
        self.pruned_by_partition += other.pruned_by_partition;
        self.after_entry_pruning += other.after_entry_pruning;
        self.pruned_by_level += other.pruned_by_level;
        self.pruned_by_data_stats += other.pruned_by_data_stats;
        self.after_manifest_filters += other.after_manifest_filters;
    }
}

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
    mut trace: Option<&mut ScanTrace>,
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
        trace.as_deref_mut(),
    )
    .await?;
    let merged = merge_manifest_entries(entries);
    if let Some(trace) = trace {
        trace.manifest_entries_after_merge = merged.len();
    }
    Ok(merged)
}

/// The predicate set that may prune WHOLE FILES by their stats.
///
/// For primary-key tables read by merging, only key conjuncts are safe: a
/// key's versions agree on the key columns but not on value columns, so a
/// value conjunct could prune the file holding the newest version and
/// resurrect an older one from a surviving file. The dropped conjuncts
/// are still enforced exactly by the post-merge residual filter in
/// `KeyValueFileReader`.
///
/// Exempt (full predicates kept):
/// - Deletion-vector tables: they read raw with per-row masks, stats are
///   a superset of live rows, full pruning stays safe.
/// - `merge-engine=first-row`: planned with `skip_level_zero` and read
///   via `DataFileReader` (see `TableRead::to_arrow`), no merge on the
///   read path - pruning a file drops exactly the rows the raw path's
///   exact residual filter would drop anyway. If first-row ever gains a
///   merge read path, this exemption must be revisited.
pub(super) fn stats_pruning_predicates(
    table: &Table,
    data_predicates: &[Predicate],
) -> Vec<Predicate> {
    let has_primary_keys = !table.schema().primary_keys().is_empty();
    let core_options = CoreOptions::new(table.schema().options());
    let deletion_vectors_enabled = core_options.deletion_vectors_enabled();
    // An unknown merge engine stays conservative (key-only pruning); the
    // read side fails on it anyway before returning rows.
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

/// Reads a manifest list file (Avro) and returns manifest file metas.
async fn read_manifest_list(
    file_io: &FileIO,
    table_path: &str,
    list_name: &str,
) -> crate::Result<Vec<crate::spec::ManifestFileMeta>> {
    if list_name.is_empty() {
        return Ok(Vec::new());
    }
    let path = format!(
        "{}/{}/{}",
        table_path.trim_end_matches('/'),
        MANIFEST_DIR,
        list_name
    );
    let input = file_io.new_input(&path)?;
    let bytes = input.read().await?;
    crate::spec::avro::from_avro_bytes_fast::<crate::spec::ManifestFileMeta>(&bytes)
}

/// Reads all manifest entries for a snapshot (base + delta manifest lists, then each manifest file).
/// Applies filters during concurrent manifest reading to reduce entries early:
/// - Manifest-file-level partition stats pruning (skip entire manifest files)
/// - Level-0 filtering per entry (DV mode or FirstRow engine)
/// - Partition predicate filtering per entry
/// - Data-level stats pruning per entry (current schema only, cross-schema fail-open)
#[allow(clippy::too_many_arguments)]
async fn read_all_manifest_entries(
    file_io: &FileIO,
    table_path: &str,
    snapshot: &Snapshot,
    skip_level_zero: bool,
    scan_all_files: bool,
    has_primary_keys: bool,
    partition_filter: Option<&PartitionFilter>,
    partition_fields: &[DataField],
    data_predicates: &[Predicate],
    current_schema_id: i64,
    schema_fields: &[DataField],
    bucket_predicate: Option<&Predicate>,
    bucket_key_fields: &[DataField],
    bucket_function_type: BucketFunctionType,
    trace: Option<&mut ScanTrace>,
) -> crate::Result<Vec<ManifestEntry>> {
    let (mut manifest_files, delta) = futures::try_join!(
        read_manifest_list(file_io, table_path, snapshot.base_manifest_list()),
        read_manifest_list(file_io, table_path, snapshot.delta_manifest_list()),
    )?;
    let mut trace = trace;
    if let Some(trace) = trace.as_deref_mut() {
        trace.record_manifest_lists(manifest_files.len(), delta.len());
    }
    manifest_files.extend(delta);

    let manifest_files_before_partition_pruning = manifest_files.len();
    if let Some(pf) = partition_filter {
        if !partition_fields.is_empty() {
            manifest_files.retain(|meta| {
                let stats = meta.partition_stats();
                let min_values = BinaryRow::from_serialized_bytes(stats.min_values()).ok();
                let max_values = BinaryRow::from_serialized_bytes(stats.max_values()).ok();
                let null_counts = stats.null_counts().clone();
                let file_stats = FileStatsRows::for_manifest_partition(
                    meta.num_added_files() + meta.num_deleted_files(),
                    min_values,
                    max_values,
                    null_counts,
                );
                pf.matches_manifest(&file_stats, partition_fields)
            });
        }
    }
    if let Some(trace) = trace.as_deref_mut() {
        trace.manifest_files_before_partition_pruning = manifest_files_before_partition_pruning;
        trace.manifest_files_after_partition_pruning = manifest_files.len();
    }

    let manifest_path_prefix = format!("{}/{}", table_path.trim_end_matches('/'), MANIFEST_DIR);
    let shared_cache = SharedSchemaCache::new();
    let manifest_results: Vec<(Vec<ManifestEntry>, ManifestReadCounters)> =
        futures::stream::iter(manifest_files)
            .map(|meta| {
                let path = format!("{}/{}", manifest_path_prefix, meta.file_name());
                let cache = shared_cache.clone();
                async move {
                    let input_file = file_io.new_input(&path)?;
                    let content = input_file.read().await?;

                    let mut bucket_cache: HashMap<i32, Option<HashSet<i32>>> = HashMap::new();
                    let mut counters = ManifestReadCounters::default();

                    let entries = crate::spec::avro::from_manifest_bytes_filtered_shared(
                        &content,
                        &cache,
                        &mut |_kind, partition_bytes, bucket, total_buckets| {
                            counters.entries_read += 1;
                            if has_primary_keys && !scan_all_files && bucket < 0 {
                                counters.pruned_by_bucket += 1;
                                return false;
                            }
                            if let Some(pred) = bucket_predicate {
                                let targets =
                                    bucket_cache.entry(total_buckets).or_insert_with(|| {
                                        compute_target_buckets(
                                            pred,
                                            bucket_key_fields,
                                            bucket_function_type,
                                            total_buckets,
                                        )
                                    });
                                if let Some(targets) = targets {
                                    if !targets.contains(&bucket) {
                                        counters.pruned_by_bucket += 1;
                                        return false;
                                    }
                                }
                            }

                            if let Some(pf) = partition_filter {
                                match pf.matches_entry(partition_bytes) {
                                    Ok(false) => {
                                        counters.pruned_by_partition += 1;
                                        return false;
                                    }
                                    Ok(true) => {}
                                    Err(_) => {}
                                }
                            }

                            true
                        },
                    )?;
                    counters.after_entry_pruning = entries.len();

                    let mut filtered = Vec::with_capacity(entries.len());
                    for entry in entries {
                        if skip_level_zero && has_primary_keys && entry.file().level == 0 {
                            counters.pruned_by_level += 1;
                            continue;
                        }
                        if !data_predicates.is_empty()
                            && !data_file_matches_predicates(
                                entry.file(),
                                data_predicates,
                                current_schema_id,
                                schema_fields,
                            )
                        {
                            counters.pruned_by_data_stats += 1;
                            continue;
                        }
                        filtered.push(entry);
                    }
                    counters.after_manifest_filters = filtered.len();
                    Ok::<_, crate::Error>((filtered, counters))
                }
            })
            .buffered(64)
            .try_collect::<Vec<_>>()
            .await?;

    let mut counters = ManifestReadCounters::default();
    let mut all_entries = Vec::new();
    for (entries, manifest_counters) in manifest_results {
        counters.merge(manifest_counters);
        all_entries.extend(entries);
    }
    if let Some(trace) = trace {
        trace.manifest_entries_read = counters.entries_read;
        trace.manifest_entries_pruned_by_bucket = counters.pruned_by_bucket;
        trace.manifest_entries_pruned_by_partition = counters.pruned_by_partition;
        trace.manifest_entries_after_entry_pruning = counters.after_entry_pruning;
        trace.manifest_entries_pruned_by_level = counters.pruned_by_level;
        trace.manifest_entries_pruned_by_data_stats = counters.pruned_by_data_stats;
        trace.manifest_entries_after_manifest_filters = counters.after_manifest_filters;
    }
    Ok(all_entries)
}

/// Nets add/delete manifest entries for a scan, returning only the live ADD set.
///
/// Mirrors Java `AbstractFileStoreScan.readAndMergeFileEntries`: first collect
/// the full [`Identifier`] of every DELETE entry, then keep the ADD entries
/// whose identifier is not in that set. The identity is the complete Paimon file
/// identity (`partition, bucket, level, file_name, extra_files, embedded_index,
/// external_path`, matching Java `FileEntry.Identifier`).
///
/// Keying on file name alone is wrong: a single-run compaction upgrades a file
/// *in place* - `DELETE f@oldLevel` plus `ADD f@newLevel` with the same file
/// name (`PojoDataFileMeta.upgrade` reuses the name, only changing `level`). An
/// identity without `level` lets the DELETE cancel the upgraded ADD, dropping
/// the file from the scan and silently losing its rows on read.
///
/// Collecting deletes first (rather than insert/remove while iterating) makes
/// the result independent of ADD/DELETE ordering, matching the Java scan path.
fn merge_manifest_entries(entries: Vec<ManifestEntry>) -> Vec<ManifestEntry> {
    use crate::spec::Identifier;
    let deleted: HashSet<Identifier> = entries
        .iter()
        .filter(|e| *e.kind() == FileKind::Delete)
        .map(|e| e.identifier())
        .collect();
    entries
        .into_iter()
        .filter(|e| *e.kind() == FileKind::Add && !deleted.contains(&e.identifier()))
        .collect()
}

/// Skip level-0 files for PK scans when the read path can rely on higher-level
/// compaction or deletion vectors instead of merge-reading raw level-0 files.
pub(super) fn should_skip_level_zero_for_scan(
    scan_all_files: bool,
    has_primary_keys: bool,
    deletion_vectors_enabled: bool,
    merge_engine: crate::Result<crate::spec::MergeEngine>,
) -> bool {
    if scan_all_files {
        return false;
    }
    if !has_primary_keys {
        return false;
    }

    deletion_vectors_enabled || merge_engine.is_ok_and(|e| e == crate::spec::MergeEngine::FirstRow)
}

#[cfg(test)]
mod tests {
    use crate::spec::{stats::BinaryTableStats, DataFileMeta, FileKind, ManifestEntry};

    fn manifest_entry(kind: FileKind, name: &str, level: i32) -> ManifestEntry {
        ManifestEntry::new(
            kind,
            Vec::new(),
            0,
            1,
            DataFileMeta {
                file_name: name.to_string(),
                file_size: 1,
                row_count: 1,
                min_key: Vec::new(),
                max_key: Vec::new(),
                key_stats: BinaryTableStats::empty(),
                value_stats: BinaryTableStats::empty(),
                min_sequence_number: 1,
                max_sequence_number: 1,
                schema_id: 1,
                level,
                extra_files: Vec::new(),
                creation_time: None,
                delete_row_count: None,
                embedded_index: None,
                file_source: None,
                value_stats_cols: None,
                external_path: None,
                first_row_id: None,
                write_cols: None,
            },
            2,
        )
    }

    #[test]
    fn manifest_planner_exposes_merge_entry_behavior() {
        let _ = super::merge_manifest_entries;
    }

    #[test]
    fn test_merge_manifest_entries_keeps_in_place_upgraded_file() {
        let entries = vec![
            manifest_entry(FileKind::Add, "f.parquet", 0),
            manifest_entry(FileKind::Delete, "f.parquet", 0),
            manifest_entry(FileKind::Add, "f.parquet", 5),
            manifest_entry(FileKind::Add, "g.parquet", 0),
        ];

        let mut live: Vec<(String, i32)> = super::merge_manifest_entries(entries)
            .into_iter()
            .map(|entry| (entry.file().file_name.clone(), entry.file().level))
            .collect();
        live.sort();
        assert_eq!(
            live,
            vec![("f.parquet".to_string(), 5), ("g.parquet".to_string(), 0)],
            "upgraded file (f@L5) must survive; only f@L0 is cancelled by the DELETE"
        );
    }
}
