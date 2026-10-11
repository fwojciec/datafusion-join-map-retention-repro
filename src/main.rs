//! Reproduction: a hash join's build-side hash map stays alive after the join's memory
//! reservation is freed, because the join dynamic filter holds it until the physical plan
//! is dropped.
//!
//! Run: `cargo run --release` (about 0.5 GB of temp Parquet, under a minute).
//!
//! The binary counts live heap bytes with a global allocator and compares them with
//! `MemoryPool::reserved()`, each scenario with `enable_join_dynamic_filter_pushdown`
//! false and true.
//!
//! Scenario 1: run a join, read its stream to the end, drop the stream, keep the plan.
//! Scenario 2: one query with two joins in a chain: the inner join's output is the build
//!             side of the outer join. The inner join finishes while the outer join builds;
//!             its hash map then stays live, unreserved, for the rest of the query.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use futures::StreamExt;

// ---- live-heap counter ------------------------------------------------------------------

struct Counting;
static LIVE: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(l) };
        if !p.is_null() {
            LIVE.fetch_add(l.size(), Ordering::Relaxed);
        }
        p
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc_zeroed(l) };
        if !p.is_null() {
            LIVE.fetch_add(l.size(), Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) };
        LIVE.fetch_sub(l.size(), Ordering::Relaxed);
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        let q = unsafe { System.realloc(p, l, n) };
        if !q.is_null() {
            LIVE.fetch_sub(l.size(), Ordering::Relaxed);
            LIVE.fetch_add(n, Ordering::Relaxed);
        }
        q
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

fn mib(b: isize) -> f64 {
    b as f64 / (1024.0 * 1024.0)
}

// ---- data -------------------------------------------------------------------------------

/// One Int64 column `k` with the distinct values `0..rows`, in `files` Parquet files.
fn write_table(dir: &std::path::Path, name: &str, rows: i64, files: i64) {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let table = dir.join(name);
    std::fs::create_dir_all(&table).unwrap();
    let per = rows / files;
    for f in 0..files {
        let file = std::fs::File::create(table.join(format!("part-{f}.parquet"))).unwrap();
        let mut w = ArrowWriter::try_new(file, Arc::clone(&schema), None).unwrap();
        let mut start = f * per;
        while start < (f + 1) * per {
            let end = (start + 65_536).min((f + 1) * per);
            let col = Int64Array::from_iter_values(start..end);
            w.write(&RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(col)]).unwrap())
                .unwrap();
            start = end;
        }
        w.close().unwrap();
    }
}

async fn context(dir: &std::path::Path, dynamic_filter: bool) -> (SessionContext, Arc<dyn MemoryPool>) {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(8 << 30));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool))
        .build_arc()
        .unwrap();
    let mut config = SessionConfig::new().with_target_partitions(4);
    config.options_mut().optimizer.enable_join_dynamic_filter_pushdown = dynamic_filter;
    let ctx = SessionContext::new_with_config_rt(config, runtime);
    for t in ["build1", "probe1", "probe2"] {
        ctx.register_parquet(t, dir.join(t).to_str().unwrap(), ParquetReadOptions::default())
            .await
            .unwrap();
    }
    (ctx, pool)
}

// ---- scenario 1 -------------------------------------------------------------------------

async fn scenario1(dir: &std::path::Path, dynamic_filter: bool) {
    let (ctx, pool) = context(dir, dynamic_filter).await;
    let base = LIVE.load(Ordering::Relaxed) as isize;
    let plan = ctx
        .sql("SELECT COUNT(*) FROM probe1 p JOIN build1 b ON p.k = b.k")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let mut stream = execute_stream(Arc::clone(&plan), ctx.task_ctx()).unwrap();
    while let Some(b) = stream.next().await {
        b.unwrap();
    }
    drop(stream);
    let held = LIVE.load(Ordering::Relaxed) as isize - base;
    let reserved = pool.reserved() as isize;
    drop(plan);
    let after = LIVE.load(Ordering::Relaxed) as isize - base;
    println!(
        "  dynamic_filter={dynamic_filter:<5}  after stream drop: live {:>6.1} MiB, reserved {:>6.1} MiB | after plan drop: live {:>6.1} MiB",
        mib(held),
        mib(reserved),
        mib(after)
    );
}

// ---- scenario 2 -------------------------------------------------------------------------

async fn scenario2(dir: &std::path::Path, dynamic_filter: bool) {
    let (ctx, pool) = context(dir, dynamic_filter).await;
    let base = LIVE.load(Ordering::Relaxed) as isize;
    let sql = "SELECT COUNT(*) FROM probe2 p \
               JOIN (SELECT b.k FROM probe1 p1 JOIN build1 b ON p1.k = b.k) x ON p.k = x.k";
    let plan = ctx.sql(sql).await.unwrap().create_physical_plan().await.unwrap();

    // Sample (live - base) and reserved every 2 ms while the query runs.
    let stop = Arc::new(AtomicBool::new(false));
    let (stop2, pool2) = (Arc::clone(&stop), Arc::clone(&pool));
    let sampler = std::thread::spawn(move || {
        let mut samples = Vec::new();
        while !stop2.load(Ordering::Relaxed) {
            let live = LIVE.load(Ordering::Relaxed) as isize - base;
            samples.push((live, pool2.reserved() as isize));
            std::thread::sleep(Duration::from_millis(2));
        }
        samples
    });
    let mut stream = execute_stream(Arc::clone(&plan), ctx.task_ctx()).unwrap();
    while let Some(b) = stream.next().await {
        b.unwrap();
    }
    stop.store(true, Ordering::Relaxed);
    let samples = sampler.join().unwrap();

    let peak_reserved = samples.iter().map(|s| s.1).max().unwrap_or(0);
    let peak_live = samples.iter().map(|s| s.0).max().unwrap_or(0);
    // Unreserved live memory in the last third of the run (the outer join's probe phase,
    // after the inner join is done). Median, so short-lived copies do not dominate.
    let tail = &samples[samples.len() * 2 / 3..];
    let mut gaps: Vec<isize> = tail.iter().map(|s| s.0 - s.1).collect();
    gaps.sort_unstable();
    let median_gap = gaps.get(gaps.len() / 2).copied().unwrap_or(0);
    let median_reserved = {
        let mut r: Vec<isize> = tail.iter().map(|s| s.1).collect();
        r.sort_unstable();
        r.get(r.len() / 2).copied().unwrap_or(0)
    };
    println!(
        "  dynamic_filter={dynamic_filter:<5}  peak live {:>6.1} MiB, peak reserved {:>6.1} MiB | last third of the run (median): reserved {:>6.1} MiB, live - reserved {:>6.1} MiB ({} samples)",
        mib(peak_live),
        mib(peak_reserved),
        mib(median_reserved),
        mib(median_gap),
        tail.len()
    );
    drop(stream);
    drop(plan);
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let dir = tempfile::tempdir().unwrap();
    // Build sides far above the InList limits (150 distinct values / 128 KiB), so the
    // dynamic filter uses the hash-map strategy (`HashTableLookupExpr`).
    write_table(dir.path(), "build1", 4_000_000, 4);
    write_table(dir.path(), "probe1", 8_000_000, 8);
    write_table(dir.path(), "probe2", 48_000_000, 16);

    println!("Scenario 1: join stream drained and dropped, physical plan still alive");
    scenario1(dir.path(), false).await;
    scenario1(dir.path(), true).await;
    println!("Scenario 2: one query, two joins in a chain (inner join output = outer build side)");
    scenario2(dir.path(), false).await;
    scenario2(dir.path(), true).await;
}
