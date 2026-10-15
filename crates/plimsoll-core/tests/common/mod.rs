//! Helpers shared by the regression tests.
//!
//! Golden files live in `tests/golden/`. To accept a deliberate change in
//! output, rerun with `PLIMSOLL_BLESS=1` and review the diff in git.

#![allow(dead_code)]

use std::path::{Path, PathBuf};

pub fn tests_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests")
}

/// Compare `actual` with `tests/golden/<name>.txt`, or write it when
/// `PLIMSOLL_BLESS` is set.
pub fn assert_golden(name: &str, actual: &str) {
    let path = tests_dir().join("golden").join(format!("{name}.txt"));
    if std::env::var_os("PLIMSOLL_BLESS").is_some() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, actual).unwrap();
        return;
    }
    let expected = std::fs::read_to_string(&path).unwrap_or_else(|_| {
        panic!(
            "missing golden file {}; run with PLIMSOLL_BLESS=1",
            path.display()
        )
    });
    if expected == actual {
        return;
    }
    let (line, exp, act) = expected
        .lines()
        .zip(actual.lines())
        .enumerate()
        .find(|(_, (e, a))| e != a)
        .map(|(i, (e, a))| (i + 1, e.to_owned(), a.to_owned()))
        .unwrap_or_else(|| {
            let n = expected.lines().count().min(actual.lines().count()) + 1;
            (n, "<line count differs>".into(), String::new())
        });
    panic!(
        "{name}: output changed at line {line}\n  expected: {exp}\n  actual:   {act}\n\
         If the change is intended, rerun with PLIMSOLL_BLESS=1 and review the diff."
    );
}

/// One request sequence for a single `(rule, key)`.
#[derive(Debug, Clone)]
pub struct Trace {
    pub name: String,
    pub window: u64,
    pub limit: u64,
    /// `(now_us, cost)` in arrival order. `now` may go backwards.
    pub steps: Vec<(u64, u64)>,
}

impl Trace {
    /// Parse the `.trace` format:
    ///
    /// ```text
    /// # comment
    /// window 1000
    /// limit 5
    /// 0 1        # now_us cost
    /// ```
    pub fn parse(name: &str, text: &str) -> Trace {
        let mut window = None;
        let mut limit = None;
        let mut steps = Vec::new();
        for (i, raw) in text.lines().enumerate() {
            let line = raw.split('#').next().unwrap().trim();
            if line.is_empty() {
                continue;
            }
            let parts: Vec<&str> = line.split_whitespace().collect();
            let num = |s: &str| -> u64 {
                s.parse()
                    .unwrap_or_else(|_| panic!("{name}:{}: bad number {s:?}", i + 1))
            };
            match parts.as_slice() {
                ["window", v] => window = Some(num(v)),
                ["limit", v] => limit = Some(num(v)),
                [t, c] => steps.push((num(t), num(c))),
                _ => panic!("{name}:{}: cannot parse {raw:?}", i + 1),
            }
        }
        Trace {
            name: name.to_owned(),
            window: window.unwrap_or_else(|| panic!("{name}: missing `window`")),
            limit: limit.unwrap_or_else(|| panic!("{name}: missing `limit`")),
            steps,
        }
    }

    pub fn render(&self, header: &str) -> String {
        let mut out = String::new();
        for line in header.lines() {
            out.push_str(&format!("# {line}\n"));
        }
        out.push_str(&format!("window {}\nlimit {}\n", self.window, self.limit));
        for (t, c) in &self.steps {
            out.push_str(&format!("{t} {c}\n"));
        }
        out
    }
}

/// Brute-force model: keeps every allowed `(t, cost)` and re-sums the window
/// on each check with signed maths. Deliberately naive.
#[derive(Debug, Default)]
pub struct Model {
    pub allowed: Vec<(u64, u64)>,
}

impl Model {
    pub fn used(&self, now: u64, window: u64) -> u64 {
        self.allowed
            .iter()
            .filter(|&&(t, _)| (t as i128) > now as i128 - window as i128)
            .map(|&(_, c)| c)
            .sum()
    }
}

/// Small deterministic PRNG (xorshift64*) so generated traces never change
/// between runs or platforms.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed.max(1))
    }

    pub fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    /// Uniform in `lo..=hi`.
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next() % (hi - lo + 1)
    }

    /// Uniform in `[0, 1)`.
    pub fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}
