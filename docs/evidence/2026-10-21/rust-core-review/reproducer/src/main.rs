use std::sync::{Arc, Barrier};
use std::thread;

use fastk::{FastKStore, KlineRecord};

fn main() {
    let root = std::path::PathBuf::from(std::env::args().nth(1).expect("existing store root"));
    let mode = std::env::args().nth(2).unwrap_or_else(|| "shared".into());
    if mode == "invalid-input" {
        assert!(!root.exists(), "fixture must be new");
        let mut store = FastKStore::open(&root).unwrap();
        store.init().unwrap();
        store
            .register_kline_series("AUDIT", "1m", 60_000, 1, 1)
            .unwrap();
        store.put_kline_chunk("AUDIT", "1m", &[record(0)]).unwrap();
        let rejected = store.put_kline_chunk("AUDIT", "1m", &[record(2), record(1)]);
        assert!(rejected.is_err());
        drop(store);
        let read_only_open = FastKStore::open_read_only(&root);
        println!(
            "{}",
            serde_json::json!({
                "input_error":rejected.unwrap_err().to_string(),
                "subsequent_read_only_open":read_only_open.as_ref().map(|_|"ok").map_err(|e|e.to_string()),
                "chunks":std::fs::read_dir(root.join("series/AUDIT/kline/1m/chunks")).unwrap().count()
            })
        );
        return;
    }
    if mode == "compaction" {
        assert!(!root.exists(), "fixture must be new");
        let mut store = FastKStore::open(&root).unwrap();
        store.init().unwrap();
        store
            .register_kline_series("AUDIT", "1m", 60_000, 1, 1)
            .unwrap();
        for i in 0..5 {
            store.put_kline_chunk("AUDIT", "1m", &[record(i)]).unwrap();
        }
        let before = store.kline_compaction_advice("AUDIT", "1m").unwrap();
        store.merge_kline_month("AUDIT", "1m", "2024-01").unwrap();
        let after = store.kline_compaction_advice("AUDIT", "1m").unwrap();
        store.put_kline_chunk("AUDIT", "1m", &[record(5)]).unwrap();
        let next = store.kline_compaction_advice("AUDIT", "1m").unwrap();
        let mut policy = store.compaction_policy().clone();
        policy.auto_merge = true;
        store.set_compaction_policy(policy);
        store.put_kline_chunk("AUDIT", "1m", &[record(6)]).unwrap();
        let automatic = store.kline_compaction_advice("AUDIT", "1m").unwrap();
        println!(
            "{}",
            serde_json::json!({
                "before_merge":format!("{before:?}"),"after_merge":format!("{after:?}"),
                "after_one_append":format!("{next:?}"),"after_next_append_with_auto_merge":format!("{automatic:?}")
            })
        );
        return;
    }
    let store = Arc::new(FastKStore::open_read_only(&root).unwrap());
    let cases: Vec<(i64, i64, Vec<KlineRecord>)> = [1_000, 5_000, 15_000, 30_000]
        .into_iter()
        .map(|offset| {
            let start = 1_704_067_260_000 + offset * 60_000;
            let end = start + 126 * 60_000;
            let expected = store.get_kline_range("S00", "1m", start, end).unwrap();
            assert!(!expected.is_empty());
            (start, end, expected)
        })
        .collect();
    if mode == "allocation" {
        let start = cases[0].2[0].ts;
        let mut samples = Vec::new();
        for steps in [0, 126, 2047, 2048, 4095] {
            store.reset_metrics();
            let rows = store
                .get_kline_range("S00", "1m", start, start + steps * 60_000)
                .unwrap();
            assert!(rows
                .iter()
                .all(|r| r.ts >= start && r.ts <= start + steps * 60_000));
            samples.push(serde_json::json!({"span_slots":steps+1,"rows":rows.len(),"capacity":rows.capacity(),"bytes_read":store.metrics_snapshot().bytes_read}));
        }
        println!("{}", serde_json::json!({"mode":mode,"samples":samples}));
        return;
    }
    for (start, end, expected) in &cases {
        for _ in 0..100 {
            assert_eq!(
                store.get_kline_range("S00", "1m", *start, *end).unwrap(),
                *expected
            );
        }
    }
    let cases = Arc::new(cases);
    let barrier = Arc::new(Barrier::new(8));
    let mut handles = Vec::new();
    for id in 0..8 {
        let reader = if mode == "independent" {
            Arc::new(FastKStore::open_read_only(&root).unwrap())
        } else {
            store.clone()
        };
        let (start, end, expected) = &cases[id % cases.len()];
        assert_eq!(
            reader.get_kline_range("S00", "1m", *start, *end).unwrap(),
            *expected
        );
        let cases = cases.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            let (start, end, expected) = &cases[id % cases.len()];
            barrier.wait();
            let (mut wrong, mut errors, mut first) = (0, 0, None);
            for _ in 0..2_000 {
                match reader.get_kline_range("S00", "1m", *start, *end) {
                    Ok(rows) if rows == *expected => {}
                    Ok(rows) => {
                        wrong += 1;
                        if first.is_none() {
                            first = Some(serde_json::json!({
                                "requested_start": start, "requested_end": end,
                                "expected_rows": expected.len(), "actual_rows": rows.len(),
                                "expected_first_ts": expected.first().map(|r|r.ts),
                                "actual_first_ts": rows.first().map(|r|r.ts)
                            }));
                        }
                    }
                    Err(error) => {
                        errors += 1;
                        if first.is_none() { first = Some(serde_json::json!({"error":error.to_string()})); }
                    }
                }
            }
            serde_json::json!({"thread":id,"calls":2000,"wrong":wrong,"errors":errors,"first_failure":first})
        }));
    }
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    println!(
        "{}",
        serde_json::json!({
            "mode":mode, "serial_controls":400,"serial_controls_passed":true,
            "parallel_calls":16000,"results":results,
            "serial_reference": cases.iter().map(|(start,end,expected)|serde_json::json!({
                "start_ts":start,"end_ts":end,"rows":expected.iter().map(|r|[r.ts,r.open,r.high,r.low,r.close,r.volume]).collect::<Vec<_>>()
            })).collect::<Vec<_>>()
        })
    );
}

fn record(i: i64) -> KlineRecord {
    KlineRecord {
        ts: 1_704_067_260_000 + i * 60_000,
        open: i,
        high: i + 1,
        low: i - 1,
        close: i,
        volume: 1,
    }
}
