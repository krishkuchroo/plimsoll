//! Regression suite: replay every trace in `tests/data/traces/` through
//! `WindowLog`, check it against the brute-force model on every step, and
//! compare the full output with `tests/golden/traces/<name>.txt`.
//!
//! The trace files are the dataset. Hand-written ones target single edge
//! cases; `gen-*` ones come from `generate_traces` below. Stage 2 of
//! TESTING.md will replay the same files against Redis.
//!
//! ```sh
//! cargo test -p plimsoll-core --test regression_traces
//! PLIMSOLL_BLESS=1 cargo test -p plimsoll-core --test regression_traces   # accept new output
//! cargo test -p plimsoll-core --test regression_traces -- --ignored        # regenerate gen-* inputs
//! ```

mod common;

use common::{Model, Rng, Trace, assert_golden, tests_dir};
use plimsoll_core::WindowLog;

fn load_traces() -> Vec<Trace> {
    let dir = tests_dir().join("data").join("traces");
    let mut paths: Vec<_> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", dir.display()))
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "trace"))
        .collect();
    paths.sort();
    paths
        .iter()
        .map(|p| {
            let name = p.file_stem().unwrap().to_string_lossy();
            Trace::parse(&name, &std::fs::read_to_string(p).unwrap())
        })
        .collect()
}

/// Replay one trace, asserting the model invariants, and render one line per
/// step: `now cost -> A|D used retry reset`.
fn replay(trace: &Trace) -> String {
    let (window, limit) = (trace.window, trace.limit);
    let mut log = WindowLog::new();
    let mut model = Model::default();
    let mut out = format!("# {} (window {window}, limit {limit})\n", trace.name);
    let (mut allowed, mut denied) = (0, 0);

    for (i, &(t, cost)) in trace.steps.iter().enumerate() {
        // Same clamp as WindowLog: never behind the newest recorded entry.
        let clock = model.allowed.last().map_or(t, |&(newest, _)| newest.max(t));
        let ctx = || format!("{} step {} (t={t}, cost={cost})", trace.name, i + 1);
        let used_before = model.used(clock, window);
        let res = log.check(t, window, limit, cost);

        assert_eq!(
            res.allowed,
            used_before + cost <= limit,
            "{}: decision differs from model",
            ctx()
        );
        if res.allowed {
            model.allowed.push((clock, cost));
            allowed += 1;
        } else {
            denied += 1;
        }
        let used = model.used(clock, window);
        assert_eq!(res.used, used, "{}: used differs from model", ctx());
        assert!(res.used <= limit, "{}: over-admission", ctx());

        let retry = res
            .retry_after
            .map_or("-".into(), |d| d.as_micros().to_string());
        out.push_str(&format!(
            "{t} {cost} -> {} used={} retry={retry} reset={}\n",
            if res.allowed { "A" } else { "D" },
            res.used,
            res.reset_after.as_micros(),
        ));
    }
    out.push_str(&format!("# allowed {allowed}, denied {denied}\n"));
    out
}

#[test]
fn traces_match_golden() {
    let traces = load_traces();
    assert!(!traces.is_empty(), "no traces found");
    let mut summary = String::new();
    for trace in &traces {
        let out = replay(trace);
        summary.push_str(out.lines().last().unwrap());
        summary.push_str(&format!("  {}\n", trace.name));
        assert_golden(&format!("traces/{}", trace.name), &out);
    }
    // One file listing every trace, so adding or dropping one shows up too.
    assert_golden("traces/_summary", &summary);
}

const WINDOW: u64 = 1_000;
const LIMIT: u64 = 5;
/// A realistic Unix-epoch microsecond value, as Redis `TIME` would return.
const EPOCH_US: u64 = 1_792_000_000_000_000;

/// Regenerate the `gen-*.trace` inputs. Deterministic: same seed, same file.
#[test]
#[ignore = "rewrites input files; run explicitly"]
fn generate_traces() {
    type Shape = (&'static str, &'static str, fn(&mut Rng) -> Vec<(u64, u64)>);
    let shapes: [Shape; 8] = [
        (
            "gen-steady",
            "one request every 180us: just over the 200us rate",
            |_| (0..120).map(|i| (EPOCH_US + i * 180, 1)).collect(),
        ),
        ("gen-uniform", "uniform random gaps 0..400us", |r| {
            walk(r, 200, |r| r.range(0, 400), |_| 1)
        }),
        (
            "gen-bursty",
            "on/off: bursts of 3-12 back-to-back, then 0.5-2 windows idle",
            |r| {
                let mut t = EPOCH_US;
                let mut v = Vec::new();
                while v.len() < 200 {
                    for _ in 0..r.range(3, 12) {
                        t += r.range(0, 5);
                        v.push((t, 1));
                    }
                    t += r.range(WINDOW / 2, WINDOW * 2);
                }
                v
            },
        ),
        (
            "gen-edge-bursts",
            "full bursts straddling window edges: k*W-1, k*W, k*W+1",
            |_| {
                let mut v = Vec::new();
                for k in 1..=10 {
                    let edge = EPOCH_US + k * WINDOW;
                    for t in [edge - 1, edge, edge + 1] {
                        v.extend(std::iter::repeat_n((t, 1), LIMIT as usize));
                    }
                }
                v
            },
        ),
        (
            "gen-heavy-tail",
            "Pareto-like gaps: mostly tiny, occasionally several windows",
            |r| {
                walk(
                    r,
                    200,
                    |r| ((20.0 / (1.0 - r.unit()).powf(1.5)) as u64).min(WINDOW * 5),
                    |_| 1,
                )
            },
        ),
        (
            "gen-mixed-cost",
            "random gaps, cost 1..=limit+1 (includes cost > limit)",
            |r| walk(r, 200, |r| r.range(0, 300), |r| r.range(1, LIMIT + 1)),
        ),
        (
            "gen-clock-jitter",
            "random gaps with 20% of steps going back up to 300us",
            |r| {
                let mut t = EPOCH_US;
                (0..200)
                    .map(|_| {
                        if r.range(0, 4) == 0 {
                            t -= r.range(1, 300);
                        } else {
                            t += r.range(0, 400);
                        }
                        (t, 1)
                    })
                    .collect()
            },
        ),
        (
            "gen-near-zero",
            "starts at t=0, like a fresh LocalStore",
            |r| {
                let mut t = 0;
                (0..120)
                    .map(|_| {
                        let s = (t, r.range(1, 2));
                        t += r.range(0, 60);
                        s
                    })
                    .collect()
            },
        ),
    ];

    let dir = tests_dir().join("data").join("traces");
    std::fs::create_dir_all(&dir).unwrap();
    for (seed, (name, desc, f)) in shapes.iter().enumerate() {
        let trace = Trace {
            name: (*name).into(),
            window: WINDOW,
            limit: LIMIT,
            steps: f(&mut Rng::new(seed as u64 + 1)),
        };
        let header = format!("{desc}\ngenerated by regression_traces::generate_traces");
        std::fs::write(dir.join(format!("{name}.trace")), trace.render(&header)).unwrap();
    }
}

fn walk(
    r: &mut Rng,
    n: usize,
    gap: fn(&mut Rng) -> u64,
    cost: fn(&mut Rng) -> u64,
) -> Vec<(u64, u64)> {
    let mut t = EPOCH_US;
    (0..n)
        .map(|_| {
            t += gap(r);
            (t, cost(r))
        })
        .collect()
}
