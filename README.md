# Hash join build map outlives its memory reservation (join dynamic filter)

A minimal reproduction for Apache DataFusion. A hash join reserves memory for its build-side
hash map, and the reservation is freed when the join's stream ends. The join dynamic filter
holds the same map (`Arc<Map>` in `HashTableLookupExpr`) as part of the physical plan, so the
map stays in memory, unreserved, until the whole plan is dropped.

In a query with several stages, every finished join keeps its map while later stages
reserve the full pool on top of it. With a memory limit near the machine limit, the
process can be OOM-killed instead of returning `ResourcesExhausted`.

## Run

```sh
cargo run --release
```

The program writes about 0.5 GB of synthetic Parquet to a temp directory (one `Int64`
column, distinct values) and finishes in under a minute. It counts live heap bytes with a
global allocator and compares them with `MemoryPool::reserved()`. Each scenario runs with
`datafusion.optimizer.enable_join_dynamic_filter_pushdown` set to `false` and to `true`.

- **Scenario 1:** `SELECT COUNT(*) FROM probe1 JOIN build1 ON k`. Read the stream to the
  end, drop the stream, keep the physical plan, then measure.
- **Scenario 2:** one query with two joins in a chain. The inner join's output is the
  build side of the outer join, so the inner join finishes before the outer join probes.
  The program measures `live − reserved` in the last third of the run (median).

Build sides have 4,000,000 distinct keys, above the InList limits (150 values / 128 KiB),
so the dynamic filter uses the hash-map strategy.

## Results (Apple M-series, macOS; the numbers are allocator-independent byte counts)

| DataFusion | filter | Scenario 1: live after stream drop | reserved | live after plan drop | Scenario 2: live − reserved, last third |
|---|---|---|---|---|---|
| 55.1.0 | false | 0.2 MiB | 0.0 MiB | 0.0 MiB | 18.7–22.3 MiB |
| 55.1.0 | **true** | **61.3 MiB** | **0.0 MiB** | 0.0 MiB | **79.6–82.2 MiB** |
| 55.2.0 | false | 0.2 MiB | 0.0 MiB | 0.0 MiB | 19.1 MiB |
| 55.2.0 | **true** | **61.3 MiB** | **0.0 MiB** | 0.0 MiB | **82.0 MiB** |
| main @ 454ca08b43 | false | 0.2 MiB | 0.0 MiB | 0.0 MiB | 21.8 MiB |
| main @ 454ca08b43 | **true** | **61.3 MiB** | **0.0 MiB** | 0.0 MiB | **56.7 MiB** (short run, 7 samples) |

To test another version, change the `datafusion` dependency in `Cargo.toml`, for example
`datafusion = { git = "https://github.com/apache/datafusion", rev = "<sha>" }`.

## Where it happens (55.1.0)

`datafusion/physical-plan/src/joins/hash_join/exec.rs`, `collect_left_input`:

1. `reservation.try_grow(estimated_hashtable_size)`, then `JoinHashMapU32::with_capacity`.
2. The map goes into an `Arc`. For a large build side, `membership =
   PushdownStrategy::Map(Arc::clone(&map))`.
3. The reservation moves into `JoinLeftData::_reservation`.

`shared_bounds.rs` turns `PushdownStrategy::Map` into a `HashTableLookupExpr` that holds
the `Arc<Map>`, and calls `DynamicFilterPhysicalExpr::update` with it. The probe-side scan
holds the dynamic filter. `mark_complete()` does not clear the expression. So when
`JoinLeftData` is dropped, the reservation is freed, but the map is not.

On `main`, `JoinBuildData` holds the map and the reservation together, and the map is
still an `Arc<Map>` "also shared with dynamic filters". The result is the same.

## Possible fixes

- Keep a reservation together with the map in the `Arc`, so that the memory stays counted
  while any holder keeps the map.
- When the join completes, update the dynamic filter to bounds only (or `true`). The
  probe-side scan is done at that point, so the membership predicate has no further use.

We tested the second idea in a patched build: the leak goes away (61 MiB → 0 MiB here), and
in our production-shape workload the unreserved memory after a stage change fell from
10–12 GiB to 1–2 GiB, and OOM kills stopped.

## Related

- apache/datafusion#24768 (spilling hash join epic): item F4 in
  https://github.com/apache/datafusion/issues/24768#issuecomment-5552303741 names this
  mechanism for a future fallback path. This reproduction shows it in normal execution.
