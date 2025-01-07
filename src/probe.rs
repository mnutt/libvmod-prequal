use std::time::SystemTime;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use crate::backend::Backend;

const PROBE_TABLE_SIZE: usize = 16;
const MAX_USES_BEFORE_EXPIRE: usize = 3;

#[derive(Debug)]
pub struct ProbeResult {
    pub timestamp: SystemTime,
    pub in_flight: usize,
    pub est_latency: usize,
    pub used_count: AtomicUsize,
    pub backend: Backend,
}

impl ProbeResult {
    pub fn new(in_flight: usize, est_latency: usize, backend: Backend) -> Self {
        Self {
            timestamp: SystemTime::now(),
            in_flight,
            est_latency,
            used_count: AtomicUsize::new(0),
            backend,
        }
    }

    pub fn increment_used(&self) -> usize {
        self.used_count.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub fn is_expired(&self) -> bool {
        self.used_count.load(Ordering::SeqCst) >= MAX_USES_BEFORE_EXPIRE
    }
}

impl Clone for ProbeResult {
    fn clone(&self) -> Self {
        Self {
            timestamp: self.timestamp,
            in_flight: self.in_flight,
            est_latency: self.est_latency,
            used_count: AtomicUsize::new(self.used_count.load(Ordering::SeqCst)),
            backend: self.backend.clone(),
        }
    }
}

#[derive(Debug)]
pub struct ProbeTable {
    results: Mutex<Vec<Option<ProbeResult>>>,
    next_index: AtomicUsize,
}

impl ProbeTable {
    pub fn new() -> Self {
        Self {
            results: Mutex::new(vec![None; PROBE_TABLE_SIZE]),
            next_index: AtomicUsize::new(0),
        }
    }

    pub fn add_result(&self, result: ProbeResult) {
        let idx = self.next_index.fetch_add(1, Ordering::SeqCst) % PROBE_TABLE_SIZE;
        if let Ok(mut results) = self.results.lock() {
            results[idx] = Some(result);
            self.next_index.store(idx, Ordering::SeqCst);
        }
    }

    pub fn find_best(&self) -> Option<Backend> {
        let mut results = self.results.lock().ok()?;
        
        // Find the best non-expired probe
        let best_idx = results.iter()
            .enumerate()
            .filter_map(|(idx, probe)| {
                probe.as_ref().map(|p| (idx, p))
            })
            .filter(|(_, probe)| !probe.is_expired())
            .min_by_key(|(_, probe)| probe.in_flight)
            .map(|(idx, _)| idx)?;

        let probe = results[best_idx].as_mut()?;
        probe.increment_used();
        
        Some(probe.backend.clone())
    }

    pub fn remove_backend(&self, backend: Backend) {
        if let Ok(mut results) = self.results.lock() {
            for probe in results.iter_mut() {
                if let Some(p) = probe {
                    if p.backend == backend {
                        *probe = None;
                    }
                }
            }
        }
    }

    pub fn display_results(&self) -> Option<String> {
        let results = self.results.lock().ok()?;
        
        let mut output = String::new();
        for (idx, probe) in results.iter().enumerate() {
            if let Some(probe) = probe {
                output.push_str(&format!(
                    "probe[{}]: backend={} ({}) in_flight={}, latency={}\n",
                    idx,
                    probe.backend.name,
                    probe.backend.address,
                    probe.in_flight,
                    probe.est_latency
                ));
            }
        }

        Some(output)
    }

    pub fn has_probes(&self) -> bool {
        self.results
            .lock()
            .map(|results| results.iter().any(|p| p.is_some()))
            .unwrap_or(false)
    }
}