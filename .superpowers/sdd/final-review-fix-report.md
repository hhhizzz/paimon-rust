# Final review fix report

Status: DONE

Files changed:
- `crates/paimon/src/table/scan_split_planner.rs`
- `crates/paimon/src/table/table_scan.rs`
- `crates/paimon/benches/table_scan_planning.rs`

Summary:
- Removed the unused `scan_all_files` field from `SplitPlanningInput` and the corresponding dead local binding in split planning.
- Stopped forwarding `scan_all_files` from `TableScan` into split planning, leaving manifest planning as the only consumer.
- Introduced shared benchmark constants for commit count and rows per commit, and reused the commit-count constant in setup, benchmark IDs, and trace labels.

Verification:
- `rtk cargo test -p paimon table_scan`
  - Result: `cargo test: 47 passed, 1276 filtered out (4 suites, 0.03s)`
- `rtk cargo bench -p paimon --bench table_scan_planning --no-run`
  - Result: `Finished 'bench' profile [optimized] target(s) in 29.94s`
  - Artifact: `Executable benches/table_scan_planning.rs (target/release/deps/table_scan_planning-6a85573985bdc3c5)`

Concerns:
- None.
