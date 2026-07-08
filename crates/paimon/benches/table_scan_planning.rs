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

use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use paimon::catalog::Identifier;
use paimon::io::{FileIO, FileIOBuilder};
use paimon::spec::{
    DataType, Datum, IntType, Predicate, PredicateBuilder, Schema, TableSchema, VarCharType,
};
use paimon::table::Table;
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

criterion_group!(benches, bench_table_scan_planning);
criterion_main!(benches);

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
