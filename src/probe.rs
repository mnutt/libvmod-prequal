use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use crate::backend::Backend;

const MAX_PROBE_AGE: Duration = Duration::from_secs(5);
pub const PROBE_TABLE_SIZE: usize = 16;
const MAX_USES_BEFORE_EXPIRE: usize = 3;

#[derive(Debug)]
pub struct ProbeResult {
    pub timestamp: SystemTime,
    pub rif: usize,         // requests in flight
    pub est_latency: usize, // estimated latency
    pub used_count: AtomicUsize,
    pub backend: Backend,
}

impl ProbeResult {
    pub fn new(timestamp: SystemTime, rif: usize, est_latency: usize, backend: Backend) -> Self {
        Self {
            timestamp,
            rif,
            est_latency,
            used_count: AtomicUsize::new(0),
            backend,
        }
    }

    pub fn increment_used(&self) -> usize {
        self.used_count.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub fn is_over_used(&self) -> bool {
        self.used_count.load(Ordering::SeqCst) >= MAX_USES_BEFORE_EXPIRE
    }
}

impl Clone for ProbeResult {
    fn clone(&self) -> Self {
        Self {
            timestamp: self.timestamp,
            rif: self.rif,
            est_latency: self.est_latency,
            used_count: AtomicUsize::new(self.used_count.load(Ordering::SeqCst)),
            backend: self.backend.clone(),
        }
    }
}

#[derive(Debug)]
pub struct ProbeTable {
    results: Mutex<Vec<ProbeResult>>,
    max_rif: AtomicUsize,
}

pub fn remove_stale_and_over_used(results: &mut Vec<ProbeResult>) {
    let now = SystemTime::now();
    results
        .retain(|p| !p.is_over_used() && now.duration_since(p.timestamp).unwrap() <= MAX_PROBE_AGE);
}

pub fn remove_worst_probe(results: &mut Vec<ProbeResult>) {
    // todo: better algorithm
    results.remove(0);
}

impl ProbeTable {
    pub fn new() -> Self {
        Self {
            results: Mutex::new(Vec::with_capacity(PROBE_TABLE_SIZE * 2)),
            max_rif: AtomicUsize::new(0),
        }
    }

    pub fn add_result(&self, result: ProbeResult) {
        if let Ok(mut results) = self.results.lock() {
            remove_stale_and_over_used(&mut results);

            // remove probe result's backend if it was already in the table
            results.retain(|p| p.backend != result.backend);

            results.push(result);
            while results.len() > PROBE_TABLE_SIZE {
                remove_worst_probe(&mut results);
            }

            let max_rif = results.iter().map(|p| p.rif).max().unwrap_or(0);
            self.max_rif.store(max_rif, Ordering::SeqCst);
        }
    }

    pub fn find_best(&self) -> Option<Backend> {
        let probes: Vec<ProbeResult> = {
            let mut results = self.results.lock().ok()?;
            if results.is_empty() {
                return None;
            }
            remove_stale_and_over_used(&mut results);
            results.iter().cloned().collect()
        };

        // Normalize rif values against the max rif
        let max_rif = self.max_rif.load(Ordering::SeqCst);
        let threshold = (max_rif as f64 * 0.8) as usize;

        // Partition probes into cold and hot, based on rif threshold
        let (cold_probes, hot_probes): (Vec<_>, Vec<_>) = probes
            .iter()
            .enumerate()
            .partition(|(_, probe)| probe.rif <= threshold);

        // Prefer cold probe with lowest latency
        // Fall back to hot probe with lowest rif if no cold probes available
        let best = cold_probes
            .iter()
            .min_by_key(|(_, probe)| probe.est_latency)
            .or_else(|| hot_probes.iter().min_by_key(|(_, probe)| probe.rif))
            .map(|(_, probe)| probe)?;

        // Increment the atomic counter directly - no lock needed since it's atomic
        best.increment_used();
        Some(best.backend.clone())
    }

    pub fn remove_backend(&self, backend: Backend) {
        if let Ok(mut results) = self.results.lock() {
            results.retain(|p| p.backend != backend);
        }
    }

    pub fn display_results(&self) -> Option<String> {
        let results = self.results.lock().ok()?;

        let mut output = String::new();
        for (idx, probe) in results.iter().enumerate() {
            output.push_str(&format!(
                "probe[{}]: backend={} ({}) in_flight={}, latency={}, used={}, age={}\n",
                idx,
                probe.backend.name,
                probe.backend.address,
                probe.rif,
                probe.est_latency,
                probe.used_count.load(Ordering::SeqCst),
                SystemTime::now()
                    .duration_since(probe.timestamp)
                    .unwrap()
                    .as_secs()
            ));
        }

        Some(output)
    }

    pub fn has_probes(&self) -> bool {
        !self.results.lock().unwrap().is_empty()
    }

    pub fn remove_stale(&self) {
        if let Ok(mut results) = self.results.lock() {
            let now = SystemTime::now();
            results.retain(|p| now.duration_since(p.timestamp).unwrap() <= MAX_PROBE_AGE);
        }
    }

    pub fn len(&self) -> usize {
        self.results
            .lock()
            .map(|results| results.len())
            .unwrap_or(0)
    }

    pub fn has_enough_probes(&self) -> bool {
        // First remove any stale probes
        self.remove_stale();

        // If pool is less than half full, signal that we need more probes
        let pool_size = self.len();
        pool_size >= PROBE_TABLE_SIZE / 2
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use varnish::ffi::{director, VCL_BACKEND};

    use super::*;

    fn create_test_probe(
        idx: usize,
        name: &str,
        rif: usize,
        est_latency: usize,
        timestamp: SystemTime,
    ) -> ProbeResult {
        ProbeResult::new(
            timestamp,
            rif,
            est_latency,
            Backend {
                name: name.to_string(),
                address: SocketAddr::from(([127, 0, 0, 1], 8080)),
                vcl_backend: VCL_BACKEND(idx as *const director),
            },
        )
    }

    #[test]
    fn test_probe_table() {
        let table = ProbeTable::new();
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn test_probe_table_add_result() {
        let table = ProbeTable::new();
        let result = create_test_probe(0, "test", 10, 100, SystemTime::now());
        table.add_result(result);
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn test_probe_table_find_best() {
        let table = ProbeTable::new();
        let result = create_test_probe(0, "test", 10, 100, SystemTime::now());
        table.add_result(result.clone());
        assert_eq!(table.find_best(), Some(result.backend));
    }

    #[test]
    fn test_probe_table_remove_backend() {
        let table = ProbeTable::new();
        let result = create_test_probe(0, "test", 10, 100, SystemTime::now());
        table.add_result(result.clone());
        table.remove_backend(result.backend);
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn test_probe_table_remove_stale() {
        let table = ProbeTable::new();
        let result = create_test_probe(
            0,
            "test",
            10,
            100,
            SystemTime::now() - MAX_PROBE_AGE - Duration::from_secs(1),
        );
        table.add_result(result.clone());
        table.remove_stale();
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn test_probe_table_has_enough_probes() {
        let table = ProbeTable::new();
        table.add_result(create_test_probe(0, "test", 10, 100, SystemTime::now()));
        assert!(
            !table.has_enough_probes(),
            "Table should not yet have enough probes"
        );
        for idx in 0..PROBE_TABLE_SIZE / 2 {
            table.add_result(create_test_probe(
                idx + 1,
                &format!("test-{}", idx),
                10,
                100,
                SystemTime::now(),
            ));
        }
        assert!(
            table.has_enough_probes(),
            "Table should now have enough probes"
        );
    }
}
