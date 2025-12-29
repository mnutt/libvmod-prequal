use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rand::seq::SliceRandom;

use super::backend::{BackendPool, SimulatedBackend};
use super::balancer::LoadBalancer;

const MAX_PROBE_AGE: Duration = Duration::from_millis(500);
const MAX_USES_BEFORE_EXPIRE: usize = 3;
const HOT_COLD_THRESHOLD: f64 = 0.8;

/// A probe result from a simulated backend
#[derive(Debug, Clone)]
struct ProbeResult {
    backend_id: usize,
    timestamp: Instant,
    rif: usize,
    est_latency: u64,
    used_count: usize,
}

impl ProbeResult {
    fn new(backend_id: usize, rif: usize, est_latency: u64) -> Self {
        Self {
            backend_id,
            timestamp: Instant::now(),
            rif,
            est_latency,
            used_count: 0,
        }
    }

    fn is_stale(&self) -> bool {
        self.timestamp.elapsed() > MAX_PROBE_AGE
    }

    fn is_over_used(&self) -> bool {
        self.used_count >= MAX_USES_BEFORE_EXPIRE
    }
}

/// Prequal load balancer with HCL rule
pub struct PrequalBalancer {
    probe_table: Mutex<Vec<ProbeResult>>,
    probe_table_size: usize,
    probes_per_request: usize,
    max_rif: AtomicUsize,
}

impl PrequalBalancer {
    pub fn new(probe_table_size: usize, probes_per_request: usize) -> Self {
        Self {
            probe_table: Mutex::new(Vec::with_capacity(probe_table_size * 2)),
            probe_table_size,
            probes_per_request,
            max_rif: AtomicUsize::new(0),
        }
    }

    /// Probe random backends and add results to the table
    fn probe_backends(&self, pool: &BackendPool) {
        let mut rng = rand::thread_rng();
        let sample: Vec<_> = pool
            .backends
            .choose_multiple(&mut rng, self.probes_per_request.min(pool.backends.len()))
            .collect();

        let mut table = self.probe_table.lock().unwrap();

        // Remove stale and overused probes
        table.retain(|p| !p.is_stale() && !p.is_over_used());

        for backend in sample {
            // Remove existing probe for this backend
            table.retain(|p| p.backend_id != backend.id);

            // Add new probe
            let probe = ProbeResult::new(
                backend.id,
                backend.get_rif(),
                backend.get_estimated_latency(),
            );
            table.push(probe);
        }

        // Calculate max RIF
        let max_rif = table.iter().map(|p| p.rif).max().unwrap_or(0);
        self.max_rif.store(max_rif, Ordering::SeqCst);

        // Remove worst probes if over capacity
        while table.len() > self.probe_table_size {
            self.remove_worst_probe(&mut table, max_rif);
        }
    }

    /// Remove the worst probe using inverse HCL logic
    fn remove_worst_probe(&self, probes: &mut Vec<ProbeResult>, max_rif: usize) {
        if probes.is_empty() {
            return;
        }

        let threshold = (max_rif as f64 * HOT_COLD_THRESHOLD) as usize;

        // Partition into cold and hot
        let (cold_indices, hot_indices): (Vec<_>, Vec<_>) = probes
            .iter()
            .enumerate()
            .partition(|(_, probe)| probe.rif <= threshold);

        // Prefer removing from hot probes (highest latency first)
        let worst_idx = hot_indices
            .iter()
            .max_by_key(|(_, probe)| probe.est_latency)
            .or_else(|| cold_indices.iter().max_by_key(|(_, probe)| probe.est_latency))
            .map(|(idx, _)| *idx);

        if let Some(idx) = worst_idx {
            probes.remove(idx);
        }
    }

    /// Find the best backend using HCL rule
    fn find_best(&self, pool: &BackendPool) -> Option<Arc<SimulatedBackend>> {
        let mut table = self.probe_table.lock().unwrap();

        // Remove stale and overused
        table.retain(|p| !p.is_stale() && !p.is_over_used());

        if table.is_empty() {
            return None;
        }

        let max_rif = self.max_rif.load(Ordering::SeqCst);
        let threshold = (max_rif as f64 * HOT_COLD_THRESHOLD) as usize;

        // Partition into cold and hot
        let (cold_probes, hot_probes): (Vec<_>, Vec<_>) = table
            .iter_mut()
            .enumerate()
            .partition(|(_, probe)| probe.rif <= threshold);

        // HCL: prefer cold probe with lowest latency, fallback to hot with lowest RIF
        let best_idx = cold_probes
            .iter()
            .min_by_key(|(_, probe)| probe.est_latency)
            .or_else(|| hot_probes.iter().min_by_key(|(_, probe)| probe.rif))
            .map(|(idx, _)| *idx);

        if let Some(idx) = best_idx {
            table[idx].used_count += 1;
            let backend_id = table[idx].backend_id;
            return pool.get(backend_id);
        }

        None
    }
}

impl LoadBalancer for PrequalBalancer {
    fn select(&self, pool: &BackendPool) -> Option<Arc<SimulatedBackend>> {
        // Probe backends (async in real implementation, synchronous here for simplicity)
        self.probe_backends(pool);

        // Try to find best from probe table
        if let Some(backend) = self.find_best(pool) {
            return Some(backend);
        }

        // Fallback to random
        let mut rng = rand::thread_rng();
        pool.backends.choose(&mut rng).cloned()
    }
}

/// Prequal with configurable parameters
pub struct PrequalBalancerConfig {
    pub probe_table_size: usize,
    pub probes_per_request: usize,
}

impl Default for PrequalBalancerConfig {
    fn default() -> Self {
        Self {
            probe_table_size: 16,
            probes_per_request: 3,
        }
    }
}

pub fn create_prequal_balancer(config: PrequalBalancerConfig) -> Box<dyn LoadBalancer> {
    Box::new(PrequalBalancer::new(
        config.probe_table_size,
        config.probes_per_request,
    ))
}
