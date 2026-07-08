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

use super::bin_pack::split_for_batch;
use super::merge_tree_split_generator::{merge_tree_split_for_batch, KeyComparator, SplitGroup};
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
    bucket_dir_name, BinaryRow, CoreOptions, DataField, DataFileMeta, FileKind,
    GlobalIndexSearchMode, IndexManifest, IndexManifestEntry, ManifestEntry, PartitionComputer,
    Predicate, Snapshot, ROW_ID_FIELD_ID, ROW_ID_FIELD_NAME, SEQUENCE_NUMBER_FIELD_ID,
    SEQUENCE_NUMBER_FIELD_NAME, VALUE_KIND_FIELD_ID, VALUE_KIND_FIELD_NAME,
};
use crate::table::schema_manager::SchemaManager;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

const MANIFEST_DIR: &str = "manifest";
const INDEX_DIR: &str = "index";

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
        let _scan_all_files = self.input.scan_all_files;
        let mut trace = self.trace;

        let file_io = table.file_io();
        let table_path = table.location();
        let table_schema_id = table.schema().id();
        let table_fields = table.schema().fields();
        let schema_manager = table.schema_manager();
        let core_options = CoreOptions::new(table.schema().options());
        let data_evolution_enabled = core_options.data_evolution_enabled();
        let deletion_vectors_enabled = core_options.deletion_vectors_enabled();
        let target_split_size = core_options.source_split_target_size();
        let open_file_cost = core_options.source_split_open_file_cost();
        let partition_keys = table.schema().partition_keys();

        // For non-data-evolution tables, cross-schema files were kept (fail-open)
        // by the pushdown. Apply the full schema-aware filter for those files.
        let stats_pruning_predicates = stats_pruning_predicates(table, data_predicates);
        let entries = if stats_pruning_predicates.is_empty() || data_evolution_enabled {
            entries
        } else {
            let current_schema_id = table.schema().id();
            let has_cross_schema = entries
                .iter()
                .any(|e| e.file().schema_id != current_schema_id);
            if !has_cross_schema {
                if let Some(trace) = trace.as_deref_mut() {
                    trace.manifest_entries_after_cross_schema_stats = entries.len();
                }
                entries
            } else {
                let before = entries.len();
                let mut kept = Vec::with_capacity(entries.len());
                let mut schema_cache: HashMap<i64, Option<Arc<ResolvedStatsSchema>>> =
                    HashMap::new();
                for entry in entries {
                    if entry.file().schema_id == current_schema_id
                        || data_file_matches_predicates_for_table(
                            table,
                            entry.file(),
                            &stats_pruning_predicates,
                            &mut schema_cache,
                        )
                        .await
                    {
                        kept.push(entry);
                    }
                }
                if let Some(trace) = trace.as_deref_mut() {
                    trace.manifest_entries_pruned_by_cross_schema_stats += before - kept.len();
                    trace.manifest_entries_after_cross_schema_stats = kept.len();
                }
                kept
            }
        };
        if entries.is_empty() {
            if let Some(trace) = trace {
                trace.record_final_plan(0, 0, 0);
            }
            return Ok(Plan::new(Vec::new()));
        } else if let Some(trace) = trace.as_deref_mut() {
            if trace.manifest_entries_after_cross_schema_stats == 0 {
                trace.manifest_entries_after_cross_schema_stats = entries.len();
            }
        }

        if matches!(limit, Some(0)) {
            if let Some(trace) = trace {
                trace.record_final_plan_with_limit(0, 0, 0, 0, true);
            }
            return Ok(Plan::new(Vec::new()));
        }

        let mut groups: BucketDataFileGroups = HashMap::with_capacity(entries.len());
        for e in entries {
            let (partition, bucket, total_buckets, file) = e.into_parts();
            let entry = groups
                .entry((partition, bucket))
                .or_insert_with(|| (total_buckets, Vec::new()));
            entry.1.push(file);
        }

        let global_index_search_mode = if data_evolution_enabled
            && core_options.global_index_enabled()
            && !data_predicates.is_empty()
        {
            Some(core_options.global_index_search_mode()?)
        } else {
            None
        };
        let global_index_detail_data_ranges = if matches!(
            global_index_search_mode,
            Some(GlobalIndexSearchMode::Detail)
        ) {
            global_index_detail_data_ranges(&groups)
        } else {
            Vec::new()
        };
        let btree_index_fallback_scan_max_size =
            core_options.btree_index_fallback_scan_max_size()?;
        let bitmap_index_fallback_scan_max_size =
            core_options.bitmap_index_fallback_scan_max_size()?;

        let snapshot_id = snapshot.id();
        let base_path = table_path.trim_end_matches('/');
        let mut splits = Vec::with_capacity(groups.len());

        let partition_computer = if !partition_keys.is_empty() {
            Some(PartitionComputer::new(
                partition_keys,
                table.schema().fields(),
                core_options.partition_default_name(),
                core_options.legacy_partition_name(),
            )?)
        } else {
            None
        };

        let read_merges_overlapping_keys = !core_options.deletion_vectors_enabled()
            && !matches!(
                core_options.merge_engine(),
                Ok(crate::spec::MergeEngine::FirstRow)
            );
        let pk_comparator = if read_merges_overlapping_keys {
            KeyComparator::from_table_schema(table.schema())
        } else {
            None
        };

        let (deletion_files_map, effective_row_ranges) =
            if let Some(index_manifest_name) = snapshot.index_manifest() {
                let index_manifest_path = format!("{base_path}/{MANIFEST_DIR}");
                let path = format!("{index_manifest_path}/{index_manifest_name}");
                let index_entries = IndexManifest::read(file_io, &path).await?;
                let dv_map = build_deletion_files_map(&index_entries, base_path);

                let row_ranges = if row_ranges.is_some() {
                    row_ranges.clone()
                } else if let Some(search_mode) = global_index_search_mode {
                    super::global_index_scanner::evaluate_global_index(
                        super::global_index_scanner::GlobalIndexEvaluation {
                            file_io,
                            table_path: base_path,
                            index_entries: &index_entries,
                            predicates: data_predicates,
                            schema_fields: table.schema().fields(),
                            search_mode,
                            btree_fallback_scan_max_size: btree_index_fallback_scan_max_size,
                            bitmap_fallback_scan_max_size: bitmap_index_fallback_scan_max_size,
                            next_row_id: snapshot.next_row_id(),
                            data_ranges: &global_index_detail_data_ranges,
                        },
                    )
                    .await?
                } else {
                    None
                };

                (Some(dv_map), row_ranges)
            } else {
                (None, row_ranges.clone())
            };

        let mut data_file_field_ids_cache = DataFileFieldIdsCache::new();
        let can_push_down_limit =
            can_push_down_limit_hint_for_scan(data_predicates, effective_row_ranges.as_deref());
        let mut limit_accumulator = match limit {
            Some(limit) if limit > 0 && can_push_down_limit => {
                Some(LimitPushdownAccumulator::new(limit))
            }
            _ => None,
        };

        'groups: for ((partition, bucket), (total_buckets, data_files)) in groups {
            let partition_row = BinaryRow::from_serialized_bytes(&partition)?;
            let bucket_path = if let Some(ref computer) = partition_computer {
                let partition_path = computer.generate_partition_path(&partition_row)?;
                format!("{base_path}/{partition_path}{}", bucket_dir_name(bucket))
            } else {
                format!("{base_path}/{}", bucket_dir_name(bucket))
            };

            let per_bucket_deletion_map = deletion_files_map
                .as_ref()
                .and_then(|map| map.get(&PartitionBucket::new(partition, bucket)));

            let file_groups: Vec<SplitGroup> = if data_evolution_enabled {
                let row_id_groups = group_by_overlapping_row_id(data_files);
                if let Some(trace) = trace.as_deref_mut() {
                    trace.data_evolution_groups_before_stats += row_id_groups.len();
                }

                let row_id_groups: Vec<Vec<DataFileMeta>> = if data_predicates.is_empty() {
                    row_id_groups
                } else {
                    let before = row_id_groups.len();
                    let groups = row_id_groups
                        .into_iter()
                        .filter(|group| {
                            data_evolution_group_matches_predicates(
                                group,
                                data_predicates,
                                table.schema().fields(),
                            )
                        })
                        .collect::<Vec<_>>();
                    if let Some(trace) = trace.as_deref_mut() {
                        trace.data_evolution_groups_pruned_by_stats += before - groups.len();
                    }
                    groups
                };

                let row_id_groups = if let Some(ref ranges) = effective_row_ranges {
                    let before = row_id_groups.len();
                    let groups = row_id_groups
                        .into_iter()
                        .filter(|group| group.iter().any(|f| any_range_overlaps_file(ranges, f)))
                        .collect::<Vec<_>>();
                    if let Some(trace) = trace.as_deref_mut() {
                        trace.data_evolution_groups_pruned_by_row_ranges += before - groups.len();
                    }
                    groups
                } else {
                    row_id_groups
                };

                let row_id_groups = if let Some(read_field_ids) = projected_read_field_ids {
                    if read_field_ids.is_empty() {
                        row_id_groups
                    } else {
                        let mut pruned = Vec::with_capacity(row_id_groups.len());
                        for group in row_id_groups {
                            pruned.push(
                                prune_data_evolution_group_by_read_fields(
                                    group,
                                    read_field_ids,
                                    deletion_vectors_enabled,
                                    table_schema_id,
                                    table_fields,
                                    schema_manager,
                                    &mut data_file_field_ids_cache,
                                )
                                .await?,
                            );
                        }
                        pruned
                    }
                } else {
                    row_id_groups
                };

                let (singles, multis): (Vec<_>, Vec<_>) = row_id_groups
                    .into_iter()
                    .partition(|group| group.len() == 1);

                let mut result = Vec::new();
                for group in multis {
                    result.push(SplitGroup {
                        files: group,
                        raw_convertible: false,
                    });
                }

                let single_files: Vec<DataFileMeta> = singles.into_iter().flatten().collect();
                for file_group in split_for_batch(single_files, target_split_size, open_file_cost) {
                    result.push(SplitGroup {
                        files: file_group,
                        raw_convertible: true,
                    });
                }

                result
            } else if let Some(ref comparator) = pk_comparator {
                let file_keys_unique = matches!(
                    core_options.merge_engine(),
                    Ok(crate::spec::MergeEngine::Deduplicate)
                        | Ok(crate::spec::MergeEngine::FirstRow)
                );
                merge_tree_split_for_batch(
                    data_files,
                    comparator,
                    target_split_size,
                    open_file_cost,
                    file_keys_unique,
                )
            } else {
                split_for_batch(data_files, target_split_size, open_file_cost)
                    .into_iter()
                    .map(|files| SplitGroup {
                        files,
                        raw_convertible: true,
                    })
                    .collect()
            };

            for group in file_groups {
                let SplitGroup {
                    files: file_group,
                    raw_convertible,
                } = group;
                let data_deletion_files = per_bucket_deletion_map.map(|per_bucket| {
                    file_group
                        .iter()
                        .map(|f| per_bucket.get(&f.file_name).cloned())
                        .collect::<Vec<Option<DeletionFile>>>()
                });

                let split_row_ranges = if let Some(ref ranges) = effective_row_ranges {
                    let mut split_ranges = Vec::new();
                    for file in &file_group {
                        split_ranges.extend(intersect_ranges_with_file(ranges, file));
                    }
                    let split_ranges = merge_row_ranges(split_ranges);
                    if split_ranges.is_empty() {
                        None
                    } else {
                        Some(split_ranges)
                    }
                } else {
                    None
                };

                let mut builder = DataSplitBuilder::new()
                    .with_snapshot(snapshot_id)
                    .with_partition(partition_row.clone())
                    .with_bucket(bucket)
                    .with_bucket_path(bucket_path.clone())
                    .with_total_buckets(total_buckets)
                    .with_data_files(file_group)
                    .with_raw_convertible(raw_convertible);
                if let Some(files) = data_deletion_files {
                    builder = builder.with_data_deletion_files(files);
                }
                if let Some(row_ranges) = split_row_ranges {
                    builder = builder.with_row_ranges(row_ranges);
                }
                let split = builder.build()?;
                if let Some(accumulator) = limit_accumulator.as_mut() {
                    if accumulator.push(split) {
                        break 'groups;
                    }
                } else {
                    splits.push(split);
                }
            }
        }

        let (splits, split_candidates_built, limit_early_stopped) =
            if let Some(accumulator) = limit_accumulator {
                let result = accumulator.finish();
                (
                    result.splits,
                    result.split_candidates_built,
                    result.limit_early_stopped,
                )
            } else {
                let split_candidates_built = splits.len();
                (splits, split_candidates_built, false)
            };
        let splits_before_limit = split_candidates_built;
        if let Some(trace) = trace {
            let final_files = splits.iter().map(|split| split.data_files().len()).sum();
            trace.record_final_plan_with_limit(
                split_candidates_built,
                splits_before_limit,
                splits.len(),
                final_files,
                limit_early_stopped,
            );
        }

        Ok(Plan::new(splits))
    }
}

fn can_push_down_limit_hint_for_scan(
    data_predicates: &[Predicate],
    row_ranges: Option<&[RowRange]>,
) -> bool {
    data_predicates.is_empty() && row_ranges.is_none()
}

#[derive(Debug)]
struct LimitPushdownResult {
    splits: Vec<DataSplit>,
    split_candidates_built: usize,
    limit_early_stopped: bool,
}

#[derive(Debug)]
struct LimitPushdownAccumulator {
    limit: usize,
    fallback_splits: Vec<DataSplit>,
    limited_splits: Vec<DataSplit>,
    scanned_row_count: i64,
    limit_early_stopped: bool,
}

impl LimitPushdownAccumulator {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            fallback_splits: Vec::new(),
            limited_splits: Vec::new(),
            scanned_row_count: 0,
            limit_early_stopped: limit == 0,
        }
    }

    fn push(&mut self, split: DataSplit) -> bool {
        if self.limit_early_stopped {
            return true;
        }

        if let Some(merged_count) = split.merged_row_count() {
            self.fallback_splits.push(split.clone());
            self.limited_splits.push(split);
            self.scanned_row_count += merged_count;
            self.limit_early_stopped = self.scanned_row_count >= self.limit as i64;
        } else {
            self.fallback_splits.push(split);
        }

        self.limit_early_stopped
    }

    fn finish(self) -> LimitPushdownResult {
        let split_candidates_built = self.fallback_splits.len();
        let splits = if self.limit_early_stopped {
            self.limited_splits
        } else {
            self.fallback_splits
        };

        LimitPushdownResult {
            splits,
            split_candidates_built,
            limit_early_stopped: self.limit_early_stopped,
        }
    }
}

type BucketDataFileGroups = HashMap<(Vec<u8>, i32), (i32, Vec<DataFileMeta>)>;

fn global_index_detail_data_ranges(groups: &BucketDataFileGroups) -> Vec<RowRange> {
    let mut ranges = Vec::new();
    for (_, data_files) in groups.values() {
        for file in data_files {
            if let Some((from, to)) = file.row_id_range() {
                ranges.push(RowRange::new(from, to));
            }
        }
    }
    merge_row_ranges(ranges)
}

fn is_system_field_id(field_id: i32) -> bool {
    matches!(
        field_id,
        ROW_ID_FIELD_ID | SEQUENCE_NUMBER_FIELD_ID | VALUE_KIND_FIELD_ID
    )
}

fn is_system_field_name(name: &str) -> bool {
    matches!(
        name,
        ROW_ID_FIELD_NAME | SEQUENCE_NUMBER_FIELD_NAME | VALUE_KIND_FIELD_NAME
    )
}

fn is_vector_store_file_name(file_name: &str) -> bool {
    file_name.to_ascii_lowercase().contains(".vector.")
}

fn is_normal_data_file(file: &DataFileMeta) -> bool {
    !crate::table::blob_file_writer::is_blob_file_name(&file.file_name)
        && !is_vector_store_file_name(&file.file_name)
}

type DataFileFieldIdsCache = HashMap<(i64, Option<Vec<String>>), HashSet<i32>>;

fn data_evolution_representative_file(group: &[DataFileMeta]) -> crate::Result<usize> {
    let mut representative: Option<usize> = None;
    for (idx, file) in group.iter().enumerate() {
        if !is_normal_data_file(file) {
            continue;
        }
        let should_replace = match representative {
            None => true,
            Some(current_idx) => {
                let current = &group[current_idx];
                (file.max_sequence_number, file.file_name.as_str())
                    < (current.max_sequence_number, current.file_name.as_str())
            }
        };
        if should_replace {
            representative = Some(idx);
        }
    }
    representative.ok_or_else(|| crate::Error::DataInvalid {
        message: "Data-evolution row range group requires at least one normal data file."
            .to_string(),
        source: None,
    })
}

async fn resolve_data_file_field_ids(
    table_schema_id: i64,
    table_fields: &[DataField],
    schema_manager: &SchemaManager,
    file: &DataFileMeta,
) -> crate::Result<HashSet<i32>> {
    let schema;
    let fields = if file.schema_id == table_schema_id {
        table_fields
    } else {
        schema = schema_manager.schema(file.schema_id).await?;
        schema.fields()
    };

    let field_id_by_name = fields
        .iter()
        .map(|field| (field.name(), field.id()))
        .collect::<HashMap<_, _>>();

    let mut field_ids = HashSet::new();
    match file.write_cols.as_ref() {
        None => {
            field_ids.extend(
                fields
                    .iter()
                    .filter(|field| !is_system_field_id(field.id()))
                    .map(|field| field.id()),
            );
        }
        Some(write_cols) => {
            for col in write_cols {
                if is_system_field_name(col) {
                    continue;
                }
                let Some(field_id) = field_id_by_name.get(col.as_str()) else {
                    return Err(crate::Error::DataInvalid {
                        message: format!(
                            "Cannot find write column '{}' in schema {}.",
                            col, file.schema_id
                        ),
                        source: None,
                    });
                };
                if !is_system_field_id(*field_id) {
                    field_ids.insert(*field_id);
                }
            }
        }
    }
    Ok(field_ids)
}

async fn data_file_field_ids(
    table_schema_id: i64,
    table_fields: &[DataField],
    schema_manager: &SchemaManager,
    file: &DataFileMeta,
    field_ids_cache: &mut DataFileFieldIdsCache,
) -> crate::Result<HashSet<i32>> {
    let key = (file.schema_id, file.write_cols.clone());
    if let Some(field_ids) = field_ids_cache.get(&key) {
        return Ok(field_ids.clone());
    }

    let field_ids =
        resolve_data_file_field_ids(table_schema_id, table_fields, schema_manager, file).await?;
    field_ids_cache.insert(key, field_ids.clone());
    Ok(field_ids)
}

async fn prune_data_evolution_group_by_read_fields(
    group: Vec<DataFileMeta>,
    read_field_ids: &HashSet<i32>,
    deletion_vectors_enabled: bool,
    table_schema_id: i64,
    table_fields: &[DataField],
    schema_manager: &SchemaManager,
    field_ids_cache: &mut DataFileFieldIdsCache,
) -> crate::Result<Vec<DataFileMeta>> {
    if read_field_ids.is_empty() || group.len() <= 1 {
        return Ok(group);
    }

    let anchor_idx = if deletion_vectors_enabled {
        Some(data_evolution_representative_file(&group)?)
    } else {
        None
    };

    let mut keep = Vec::with_capacity(group.len());
    for (idx, file) in group.iter().enumerate() {
        let file_field_ids = data_file_field_ids(
            table_schema_id,
            table_fields,
            schema_manager,
            file,
            field_ids_cache,
        )
        .await?;
        if file_field_ids
            .iter()
            .any(|field_id| read_field_ids.contains(field_id))
        {
            keep.push(idx);
        }
    }
    if let Some(anchor_idx) = anchor_idx {
        if !keep.contains(&anchor_idx) {
            keep.push(anchor_idx);
        }
    }

    if keep.is_empty() {
        keep.push(data_evolution_representative_file(&group)?);
    } else if keep.iter().any(|idx| !is_normal_data_file(&group[*idx]))
        && !keep.iter().any(|idx| is_normal_data_file(&group[*idx]))
    {
        let representative_idx = data_evolution_representative_file(&group)?;
        if !keep.contains(&representative_idx) {
            keep.push(representative_idx);
        }
    }

    let mut files = group.into_iter().map(Some).collect::<Vec<_>>();
    Ok(keep
        .into_iter()
        .filter_map(|idx| files.get_mut(idx).and_then(Option::take))
        .collect())
}

fn build_deletion_files_map(
    index_entries: &[IndexManifestEntry],
    table_path: &str,
) -> HashMap<PartitionBucket, HashMap<String, DeletionFile>> {
    let table_path = table_path.trim_end_matches('/');
    let index_path_prefix = format!("{table_path}/{INDEX_DIR}");
    let mut map: HashMap<PartitionBucket, HashMap<String, DeletionFile>> =
        HashMap::with_capacity(index_entries.len());
    for entry in index_entries {
        if entry.kind != FileKind::Add {
            continue;
        }
        if entry.index_file.index_type != "DELETION_VECTORS" {
            continue;
        }
        let ranges = match &entry.index_file.deletion_vectors_ranges {
            Some(r) if !r.is_empty() => r,
            _ => continue,
        };
        let key = PartitionBucket::new(entry.partition.clone(), entry.bucket);
        let dv_path = format!("{}/{}", index_path_prefix, entry.index_file.file_name);
        let per_bucket = map.entry(key).or_default();
        for (data_file_name, meta) in ranges {
            per_bucket.insert(
                data_file_name.clone(),
                DeletionFile::new(
                    dv_path.clone(),
                    meta.offset as i64,
                    meta.length as i64,
                    meta.cardinality,
                ),
            );
        }
    }
    map
}

#[cfg(test)]
mod tests {
    use super::{build_deletion_files_map, prune_data_evolution_group_by_read_fields};
    use super::{LimitPushdownAccumulator, Table};
    use crate::catalog::Identifier;
    use crate::io::FileIOBuilder;
    use crate::spec::{
        stats::BinaryTableStats, BinaryRow, BinaryRowBuilder, DataFileMeta, DataType,
        DeletionVectorMeta, FileKind, IndexFileMeta, IndexManifestEntry, IntType, Schema,
        TableSchema,
    };
    use crate::table::source::{DataSplit, DataSplitBuilder, DeletionFile, PartitionBucket};
    use crate::table::{CommitMessage, TableCommit};
    use crate::Error;
    use bytes::Bytes;
    use chrono::{DateTime, Utc};
    use std::collections::{HashMap, HashSet};

    #[test]
    fn split_planner_exposes_limit_pushdown_behavior() {
        let _ = LimitPushdownAccumulator::new;
    }

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

    fn make_evo_file_with_cols(
        name: &str,
        row_count: i64,
        max_seq: i64,
        first_row_id: i64,
        write_cols: &[&str],
    ) -> DataFileMeta {
        let mut file = make_evo_file(name, 10, row_count, max_seq, Some(first_row_id));
        file.write_cols = Some(write_cols.iter().map(|col| (*col).to_string()).collect());
        file
    }

    fn data_evolution_test_table(table_path: &str, schema: TableSchema) -> Table {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let schema = schema.copy_with_options(HashMap::from([(
            "data-evolution.enabled".to_string(),
            "true".to_string(),
        )]));
        Table::new(
            file_io,
            Identifier::new("test_db", "de_table"),
            table_path.to_string(),
            schema,
            None,
        )
    }

    fn two_column_schema(id: i64, left: &str, right: &str) -> TableSchema {
        TableSchema::new(
            id,
            &Schema::builder()
                .column(left, DataType::Int(IntType::new()))
                .column(right, DataType::Int(IntType::new()))
                .build()
                .unwrap(),
        )
    }

    async fn write_schema_file(table: &Table, schema: &TableSchema) {
        let path = table.schema_manager().schema_path(schema.id());
        let dir = path.rsplit_once('/').map(|(dir, _)| dir).unwrap();
        table.file_io().mkdirs(dir).await.unwrap();
        let json = serde_json::to_vec(schema).unwrap();
        table
            .file_io()
            .new_output(&path)
            .unwrap()
            .write(Bytes::from(json))
            .await
            .unwrap();
    }

    fn file_names_from_files(files: &[DataFileMeta]) -> Vec<&str> {
        files.iter().map(|file| file.file_name.as_str()).collect()
    }

    fn int_stats_row(value: Option<i32>) -> Vec<u8> {
        let mut builder = BinaryRowBuilder::new(1);
        match value {
            Some(value) => builder.write_int(0, value),
            None => builder.set_null_at(0),
        }
        builder.build_serialized()
    }

    fn test_data_file_meta(
        min_values: Vec<u8>,
        max_values: Vec<u8>,
        null_counts: Vec<Option<i64>>,
        row_count: i64,
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
            schema_id: 0,
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
        let schema = Schema::builder()
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

    fn limit_test_split(file_name: &str, row_count: i64) -> DataSplit {
        let mut file = test_data_file_meta(Vec::new(), Vec::new(), Vec::new(), row_count);
        file.file_name = file_name.to_string();

        DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(format!("file:/tmp/{file_name}"))
            .with_total_buckets(1)
            .with_data_files(vec![file])
            .build()
            .unwrap()
    }

    fn limit_test_split_with_unknown_merged_row_count(
        file_name: &str,
        row_count: i64,
    ) -> DataSplit {
        let mut file = test_data_file_meta(Vec::new(), Vec::new(), Vec::new(), row_count);
        file.file_name = file_name.to_string();

        DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(format!("file:/tmp/{file_name}"))
            .with_total_buckets(1)
            .with_data_files(vec![file])
            .with_data_deletion_files(vec![Some(DeletionFile::new(
                format!("file:/tmp/{file_name}.dv"),
                0,
                0,
                None,
            ))])
            .build()
            .unwrap()
    }

    fn split_file_names(splits: &[DataSplit]) -> Vec<&str> {
        splits
            .iter()
            .map(|split| split.data_files()[0].file_name.as_str())
            .collect()
    }

    #[test]
    fn test_incremental_limit_accumulator_stops_after_known_count_reaches_limit() {
        let mut accumulator = LimitPushdownAccumulator::new(3);

        assert!(!accumulator.push(limit_test_split("a.parquet", 2)));
        assert!(
            !accumulator.push(limit_test_split_with_unknown_merged_row_count(
                "b.parquet",
                4
            ))
        );
        assert!(accumulator.push(limit_test_split("c.parquet", 3)));

        let result = accumulator.finish();
        assert!(result.limit_early_stopped);
        assert_eq!(result.split_candidates_built, 3);
        assert_eq!(
            split_file_names(&result.splits),
            vec!["a.parquet", "c.parquet"]
        );
    }

    #[test]
    fn test_incremental_limit_accumulator_returns_fallback_when_limit_not_reached() {
        let mut accumulator = LimitPushdownAccumulator::new(100);

        assert!(!accumulator.push(limit_test_split("a.parquet", 2)));
        assert!(
            !accumulator.push(limit_test_split_with_unknown_merged_row_count(
                "b.parquet",
                4
            ))
        );
        assert!(!accumulator.push(limit_test_split("c.parquet", 3)));

        let result = accumulator.finish();
        assert!(!result.limit_early_stopped);
        assert_eq!(result.split_candidates_built, 3);
        assert_eq!(
            split_file_names(&result.splits),
            vec!["a.parquet", "b.parquet", "c.parquet"]
        );
    }

    #[tokio::test]
    async fn test_data_evolution_prunes_files_without_projected_columns() {
        let table =
            data_evolution_test_table("memory:/de_prune_cols", two_column_schema(0, "id", "name"));
        let read_field_ids = HashSet::from([1]);
        let files = vec![
            make_evo_file_with_cols("id.parquet", 10, 1, 0, &["id"]),
            make_evo_file_with_cols("name.parquet", 10, 2, 0, &["name"]),
        ];
        let mut field_ids_cache = HashMap::new();

        let pruned = prune_data_evolution_group_by_read_fields(
            files,
            &read_field_ids,
            false,
            table.schema().id(),
            table.schema().fields(),
            table.schema_manager(),
            &mut field_ids_cache,
        )
        .await
        .unwrap();

        assert_eq!(file_names_from_files(&pruned), vec!["name.parquet"]);
    }

    #[tokio::test]
    async fn test_data_evolution_pruning_keeps_dv_anchor() {
        let table =
            data_evolution_test_table("memory:/de_prune_dv", two_column_schema(0, "id", "name"));
        let read_field_ids = HashSet::from([1]);
        let files = vec![
            make_evo_file_with_cols("new-name.parquet", 10, 5, 0, &["name"]),
            make_evo_file_with_cols("old-id.parquet", 10, 1, 0, &["id"]),
        ];
        let mut field_ids_cache = HashMap::new();

        let pruned = prune_data_evolution_group_by_read_fields(
            files,
            &read_field_ids,
            true,
            table.schema().id(),
            table.schema().fields(),
            table.schema_manager(),
            &mut field_ids_cache,
        )
        .await
        .unwrap();

        assert_eq!(
            file_names_from_files(&pruned),
            vec!["new-name.parquet", "old-id.parquet"]
        );
    }

    #[tokio::test]
    async fn test_data_evolution_pruning_keeps_row_count_representative() {
        let table = data_evolution_test_table(
            "memory:/de_prune_representative",
            two_column_schema(0, "id", "name"),
        );
        let read_field_ids = HashSet::from([2]);
        let files = vec![
            make_evo_file_with_cols("new-name.parquet", 10, 5, 0, &["name"]),
            make_evo_file_with_cols("old-id.parquet", 10, 1, 0, &["id"]),
        ];
        let mut field_ids_cache = HashMap::new();

        let pruned = prune_data_evolution_group_by_read_fields(
            files,
            &read_field_ids,
            false,
            table.schema().id(),
            table.schema().fields(),
            table.schema_manager(),
            &mut field_ids_cache,
        )
        .await
        .unwrap();

        assert_eq!(file_names_from_files(&pruned), vec!["old-id.parquet"]);
    }

    #[tokio::test]
    async fn test_data_evolution_pruning_matches_renamed_columns_by_field_id() {
        let schema_v0 = two_column_schema(0, "id", "old_name");
        let schema_v1 = two_column_schema(1, "id", "new_name");
        let table = data_evolution_test_table("memory:/de_prune_rename", schema_v1);
        write_schema_file(&table, &schema_v0).await;

        let read_field_ids = HashSet::from([1]);
        let mut file = make_evo_file_with_cols("renamed.parquet", 10, 1, 0, &["old_name"]);
        file.schema_id = 0;
        let pruned = prune_data_evolution_group_by_read_fields(
            vec![
                make_evo_file_with_cols("id.parquet", 10, 2, 0, &["id"]),
                file,
            ],
            &read_field_ids,
            false,
            table.schema().id(),
            table.schema().fields(),
            table.schema_manager(),
            &mut HashMap::new(),
        )
        .await
        .unwrap();

        assert_eq!(file_names_from_files(&pruned), vec!["renamed.parquet"]);
    }

    #[tokio::test]
    async fn test_data_evolution_pruning_keeps_normal_representative_for_vector_file() {
        let table =
            data_evolution_test_table("memory:/de_prune_vector", two_column_schema(0, "id", "emb"));
        let read_field_ids = HashSet::from([1]);
        let files = vec![
            make_evo_file_with_cols("data.parquet", 10, 1, 0, &["id"]),
            make_evo_file_with_cols("emb.vector.parquet", 10, 2, 0, &["emb"]),
        ];
        let mut field_ids_cache = HashMap::new();

        let pruned = prune_data_evolution_group_by_read_fields(
            files,
            &read_field_ids,
            false,
            table.schema().id(),
            table.schema().fields(),
            table.schema_manager(),
            &mut field_ids_cache,
        )
        .await
        .unwrap();

        assert_eq!(
            file_names_from_files(&pruned),
            vec!["emb.vector.parquet", "data.parquet"]
        );
    }

    #[tokio::test]
    async fn test_data_evolution_pruning_rejects_group_without_normal_representative() {
        let table = data_evolution_test_table(
            "memory:/de_prune_no_normal",
            two_column_schema(0, "id", "emb"),
        );
        let read_field_ids = HashSet::from([1]);
        let files = vec![
            make_evo_file_with_cols("emb-1.vector.parquet", 10, 1, 0, &["emb"]),
            make_evo_file_with_cols("emb-2.vector.parquet", 10, 2, 0, &["emb"]),
        ];
        let mut field_ids_cache = HashMap::new();

        let err = prune_data_evolution_group_by_read_fields(
            files,
            &read_field_ids,
            false,
            table.schema().id(),
            table.schema().fields(),
            table.schema_manager(),
            &mut field_ids_cache,
        )
        .await
        .unwrap_err();

        match err {
            Error::DataInvalid { message, .. } => {
                assert!(message.contains("requires at least one normal data file"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_plan_with_trace_records_limit_early_stop_during_split_construction() {
        let table_path =
            "memory:/test_plan_with_trace_records_limit_early_stop_during_split_construction";
        let table = scan_trace_small_split_table(table_path);
        setup_scan_trace_dirs(&table).await;

        TableCommit::new(table.clone(), "scan-trace-limit-test".to_string())
            .commit(vec![CommitMessage::new(
                BinaryRowBuilder::new(0).build_serialized(),
                0,
                vec![
                    stats_trace_file("limit-1.parquet", 1, 1),
                    stats_trace_file("limit-2.parquet", 2, 2),
                    stats_trace_file("limit-3.parquet", 3, 3),
                ],
            )])
            .await
            .unwrap();

        let (_full_plan, full_trace) = table
            .new_read_builder()
            .new_scan()
            .plan_with_trace()
            .await
            .unwrap();
        let mut limited_reader = table.new_read_builder();
        limited_reader.with_limit(2);
        let (_limited_plan, limited_trace) =
            limited_reader.new_scan().plan_with_trace().await.unwrap();

        assert_eq!(
            full_trace.final_splits, 3,
            "fixture should build three splits: {full_trace:?}"
        );
        assert!(!full_trace.limit_early_stopped);
        assert_eq!(full_trace.split_candidates_built, full_trace.final_splits);
        assert!(limited_trace.limit_early_stopped);
        assert!(
            limited_trace.split_candidates_built < full_trace.split_candidates_built,
            "limited trace should show construction-time stop: full={full_trace:?}, limited={limited_trace:?}"
        );
        assert_eq!(limited_trace.final_splits, 1);
    }

    #[test]
    fn test_build_deletion_files_map_preserves_cardinality() {
        let entries = vec![IndexManifestEntry {
            version: 1,
            kind: FileKind::Add,
            partition: vec![1, 2, 3],
            bucket: 7,
            index_file: IndexFileMeta {
                index_type: "DELETION_VECTORS".into(),
                file_name: "index-file".into(),
                file_size: 128,
                row_count: 1,
                deletion_vectors_ranges: Some(indexmap::IndexMap::from([(
                    "data-file.parquet".into(),
                    DeletionVectorMeta {
                        offset: 11,
                        length: 22,
                        cardinality: Some(33),
                    },
                )])),
                global_index_meta: None,
            },
        }];

        let map = build_deletion_files_map(&entries, "file:/tmp/table");

        let by_bucket = map
            .get(&PartitionBucket::new(vec![1, 2, 3], 7))
            .expect("partition bucket should exist");
        let deletion_file = by_bucket
            .get("data-file.parquet")
            .expect("deletion file should exist");

        assert_eq!(
            deletion_file,
            &DeletionFile::new("file:/tmp/table/index/index-file".into(), 11, 22, Some(33))
        );
    }
}
