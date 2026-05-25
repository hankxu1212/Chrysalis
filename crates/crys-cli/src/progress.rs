use std::sync::{Arc, Mutex};

use crys_core::sync::{Progress,ProgressHandle};
use indicatif::{HumanBytes, MultiProgress, ProgressBar, ProgressStyle};

/// CLI progress reporter. Renders one progress bar per phase (chunks → files
/// → trees → commits) into a `MultiProgress` so they stack vertically and
/// don't clobber each other when phases overlap.
///
/// Tracks total bytes per phase so we can print a final summary.
pub struct IndicatifProgress {
    multi: MultiProgress,
    state: Mutex<IndicatifState>,
}

#[derive(Default)]
pub struct IndicatifState {
    /// Current bar per phase name.
    bars: std::collections::HashMap<String, ProgressBar>,
    /// Total bytes copied per phase.
    bytes: std::collections::HashMap<String, u64>,
    /// Total objects copied per phase.
    counts: std::collections::HashMap<String, u64>,
    /// Optional label per phase (e.g., the path currently being staged).
    labels: std::collections::HashMap<String, String>,
}

impl IndicatifProgress {
    fn new() -> Self {
        Self {
            multi: MultiProgress::new(),
            state: Mutex::new(IndicatifState::default()),
        }
    }

    fn summary(&self) -> (u64, u64) {
        let state = self.state.lock().unwrap();
        // Walking is the discovery phase; it doesn't transfer anything, so
        // exclude it from the "transferred" tally.
        let bytes: u64 = state
            .bytes
            .iter()
            .filter(|(k, _)| k.as_str() != "walking")
            .map(|(_, v)| *v)
            .sum();
        let count: u64 = state
            .counts
            .iter()
            .filter(|(k, _)| k.as_str() != "walking")
            .map(|(_, v)| *v)
            .sum();
        (count, bytes)
    }
}

fn phase_style() -> ProgressStyle {
    ProgressStyle::with_template("{prefix:>8} [{bar:30.cyan/blue}] {pos}/{len} {msg}")
        .unwrap()
        .progress_chars("=> ")
}

fn spinner_style() -> ProgressStyle {
    ProgressStyle::with_template("{prefix:>8} {spinner} {pos} objects {msg}")
        .unwrap()
        .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"])
}

impl Progress for IndicatifProgress {
    fn start_phase(&self, kind: &str, total: usize) {
        let pb = if total == 0 {
            // Indeterminate: discovery phase. Render as a spinner that ticks
            // every 100 ms so the bar stays alive even if we don't `inc`
            // for a while (e.g. waiting on a single GET).
            let pb = self.multi.add(ProgressBar::new_spinner());
            pb.set_style(spinner_style());
            pb.enable_steady_tick(std::time::Duration::from_millis(100));
            pb
        } else {
            let pb = self.multi.add(ProgressBar::new(total as u64));
            pb.set_style(phase_style());
            pb
        };
        pb.set_prefix(kind.to_string());
        let mut state = self.state.lock().unwrap();
        state.bars.insert(kind.to_string(), pb);
    }

    fn object_copied(&self, kind: &str, bytes: u64) {
        let mut state = self.state.lock().unwrap();
        *state.bytes.entry(kind.to_string()).or_insert(0) += bytes;
        *state.counts.entry(kind.to_string()).or_insert(0) += 1;
        let total_bytes = state.bytes[kind];
        let label = state.labels.get(kind).cloned();
        if let Some(bar) = state.bars.get(kind) {
            bar.inc(1);
            // Suppress bytes message for the walking phase — we don't copy
            // anything there, so reporting bytes would be misleading.
            let body = if kind == "walking" {
                String::new()
            } else {
                format!("{}", HumanBytes(total_bytes))
            };
            bar.set_message(compose_msg(label.as_deref(), &body));
        }
    }

    fn finish_phase(&self, kind: &str) {
        let state = self.state.lock().unwrap();
        if let Some(bar) = state.bars.get(kind) {
            let bytes = state.bytes.get(kind).copied().unwrap_or(0);
            let label = state.labels.get(kind).map(String::as_str);
            let body = if kind == "walking" {
                let count = state.counts.get(kind).copied().unwrap_or(0);
                format!("({count} objects discovered)")
            } else {
                format!("done • {}", HumanBytes(bytes))
            };
            bar.finish_with_message(compose_msg(label, &body));
        }
    }

    fn set_phase_label(&self, kind: &str, label: &str) {
        let mut state = self.state.lock().unwrap();
        state.labels.insert(kind.to_string(), label.to_string());
        // Reflect immediately so the bar shows the path even before any
        // object_copied tick lands.
        if let Some(bar) = state.bars.get(kind) {
            bar.set_message(compose_msg(Some(label), ""));
        }
    }
}

fn compose_msg(label: Option<&str>, body: &str) -> String {
    match (label, body.is_empty()) {
        (Some(l), true) => l.to_string(),
        (Some(l), false) => format!("{l} • {body}"),
        (None, _) => body.to_string(),
    }
}

/// Holds both a typed handle (for the post-run summary) and the trait-object
/// view (for sync.rs). Both point at the same `IndicatifProgress`.
pub struct ProgressBundle {
    inner: Arc<IndicatifProgress>,
    pub handle: ProgressHandle,
}

impl ProgressBundle {
    pub fn new() -> Self {
        let inner = Arc::new(IndicatifProgress::new());
        let handle: ProgressHandle = inner.clone();
        Self { inner, handle }
    }
}

/// After a transfer command, print "transferred N objects (X.X)" with
/// totals accumulated by the indicatif reporter.
pub fn print_progress_summary(bundle: &ProgressBundle) {
    let (count, bytes) = bundle.inner.summary();
    if count > 0 {
        println!("transferred {count} object(s) ({})", HumanBytes(bytes));
    }
}
