use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

/// Collected metrics from a simulation run
#[derive(Debug)]
pub struct Metrics {
    pub strategy: String,
    pub scenario: String,
    pub total_requests: AtomicU64,
    pub successful_requests: AtomicU64,
    pub failed_requests: AtomicU64,
    latencies: Mutex<Vec<u64>>, // in microseconds
}

impl Metrics {
    pub fn new(strategy: &str, scenario: &str) -> Self {
        Self {
            strategy: strategy.to_string(),
            scenario: scenario.to_string(),
            total_requests: AtomicU64::new(0),
            successful_requests: AtomicU64::new(0),
            failed_requests: AtomicU64::new(0),
            latencies: Mutex::new(Vec::with_capacity(100_000)),
        }
    }

    pub fn record_success(&self, latency: Duration) {
        self.total_requests.fetch_add(1, Ordering::SeqCst);
        self.successful_requests.fetch_add(1, Ordering::SeqCst);

        let latency_us = latency.as_micros() as u64;
        if let Ok(mut latencies) = self.latencies.lock() {
            latencies.push(latency_us);
        }
    }

    pub fn record_failure(&self) {
        self.total_requests.fetch_add(1, Ordering::SeqCst);
        self.failed_requests.fetch_add(1, Ordering::SeqCst);
    }

    pub fn total(&self) -> u64 {
        self.total_requests.load(Ordering::SeqCst)
    }

    pub fn successes(&self) -> u64 {
        self.successful_requests.load(Ordering::SeqCst)
    }

    pub fn failures(&self) -> u64 {
        self.failed_requests.load(Ordering::SeqCst)
    }

    pub fn error_rate(&self) -> f64 {
        let total = self.total();
        if total == 0 {
            return 0.0;
        }
        self.failures() as f64 / total as f64 * 100.0
    }

    fn sorted_latencies(&self) -> Vec<u64> {
        let mut latencies = self.latencies.lock().unwrap().clone();
        latencies.sort_unstable();
        latencies
    }

    pub fn percentile(&self, p: f64) -> Duration {
        let latencies = self.sorted_latencies();
        if latencies.is_empty() {
            return Duration::ZERO;
        }

        let idx = ((latencies.len() as f64 * p / 100.0) as usize).min(latencies.len() - 1);
        Duration::from_micros(latencies[idx])
    }

    pub fn p50(&self) -> Duration {
        self.percentile(50.0)
    }

    pub fn p90(&self) -> Duration {
        self.percentile(90.0)
    }

    pub fn p99(&self) -> Duration {
        self.percentile(99.0)
    }

    pub fn p999(&self) -> Duration {
        self.percentile(99.9)
    }

    pub fn mean(&self) -> Duration {
        let latencies = self.latencies.lock().unwrap();
        if latencies.is_empty() {
            return Duration::ZERO;
        }

        let sum: u64 = latencies.iter().sum();
        Duration::from_micros(sum / latencies.len() as u64)
    }

    pub fn max(&self) -> Duration {
        let latencies = self.latencies.lock().unwrap();
        Duration::from_micros(*latencies.iter().max().unwrap_or(&0))
    }
}

/// Summary of a simulation run
#[derive(Debug, Clone)]
pub struct MetricsSummary {
    pub strategy: String,
    pub scenario: String,
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub error_rate: f64,
    pub mean_ms: f64,
    pub p50_ms: f64,
    pub p90_ms: f64,
    pub p99_ms: f64,
    pub p999_ms: f64,
    pub max_ms: f64,
}

impl MetricsSummary {
    pub fn from_metrics(metrics: &Metrics) -> Self {
        Self {
            strategy: metrics.strategy.clone(),
            scenario: metrics.scenario.clone(),
            total_requests: metrics.total(),
            successful_requests: metrics.successes(),
            failed_requests: metrics.failures(),
            error_rate: metrics.error_rate(),
            mean_ms: metrics.mean().as_secs_f64() * 1000.0,
            p50_ms: metrics.p50().as_secs_f64() * 1000.0,
            p90_ms: metrics.p90().as_secs_f64() * 1000.0,
            p99_ms: metrics.p99().as_secs_f64() * 1000.0,
            p999_ms: metrics.p999().as_secs_f64() * 1000.0,
            max_ms: metrics.max().as_secs_f64() * 1000.0,
        }
    }

    /// CSV header
    pub fn csv_header() -> &'static str {
        "scenario,strategy,total,success,failed,error_rate,mean_ms,p50_ms,p90_ms,p99_ms,p999_ms,max_ms"
    }

    /// Format as CSV row
    pub fn to_csv_row(&self) -> String {
        format!(
            "{},{},{},{},{},{:.4},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3}",
            self.scenario,
            self.strategy,
            self.total_requests,
            self.successful_requests,
            self.failed_requests,
            self.error_rate,
            self.mean_ms,
            self.p50_ms,
            self.p90_ms,
            self.p99_ms,
            self.p999_ms,
            self.max_ms,
        )
    }
}

/// Write all summaries to a CSV file
pub fn write_csv<W: Write>(writer: &mut W, summaries: &[MetricsSummary]) -> std::io::Result<()> {
    writeln!(writer, "{}", MetricsSummary::csv_header())?;
    for summary in summaries {
        writeln!(writer, "{}", summary.to_csv_row())?;
    }
    Ok(())
}

/// Print a nice table to stdout
pub fn print_table(summaries: &[MetricsSummary]) {
    // Group by scenario
    let mut scenarios: Vec<&str> = summaries.iter().map(|s| s.scenario.as_str()).collect();
    scenarios.sort();
    scenarios.dedup();

    for scenario in scenarios {
        let scenario_summaries: Vec<_> = summaries
            .iter()
            .filter(|s| s.scenario == scenario)
            .collect();

        println!("\n{}", "=".repeat(100));
        println!("Scenario: {}", scenario);
        println!("{}", "=".repeat(100));
        println!(
            "{:<18} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8}",
            "Strategy", "Total", "Errors%", "Mean", "p50", "p90", "p99", "p99.9"
        );
        println!("{}", "-".repeat(100));

        for s in scenario_summaries {
            println!(
                "{:<18} {:>8} {:>7.2}% {:>7.2} {:>7.2} {:>7.2} {:>7.2} {:>7.2}",
                s.strategy,
                s.total_requests,
                s.error_rate,
                s.mean_ms,
                s.p50_ms,
                s.p90_ms,
                s.p99_ms,
                s.p999_ms,
            );
        }
    }
    println!();
}
