use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::time::sleep;

#[derive(Debug, Clone, Copy)]
pub enum BackendError {
    Overloaded,
}

/// A simulated backend server with configurable latency characteristics
#[derive(Debug)]
pub struct SimulatedBackend {
    pub id: usize,
    /// Base processing time in milliseconds
    base_latency_ms: u64,
    /// Additional latency per concurrent request (simulates queueing)
    latency_per_rif_ms: u64,
    /// Current requests in flight
    current_rif: AtomicUsize,
    /// Maximum concurrent requests before shedding load
    capacity: usize,
    /// Antagonist load factor (0-100), adds percentage to latency
    antagonist_load: AtomicU64,
}

impl SimulatedBackend {
    pub fn new(
        id: usize,
        base_latency_ms: u64,
        latency_per_rif_ms: u64,
        capacity: usize,
    ) -> Self {
        Self {
            id,
            base_latency_ms,
            latency_per_rif_ms,
            current_rif: AtomicUsize::new(0),
            capacity,
            antagonist_load: AtomicU64::new(0),
        }
    }

    /// Process a request asynchronously, returning the actual latency
    pub async fn process_request(&self) -> Result<Duration, BackendError> {
        let rif = self.current_rif.fetch_add(1, Ordering::SeqCst);

        if rif >= self.capacity {
            self.current_rif.fetch_sub(1, Ordering::SeqCst);
            return Err(BackendError::Overloaded);
        }

        // Calculate latency based on current load
        let antagonist = self.antagonist_load.load(Ordering::SeqCst);
        let latency_ms = self.base_latency_ms
            + (rif as u64 * self.latency_per_rif_ms)
            + (self.base_latency_ms * antagonist / 100);

        // Simulate processing time
        sleep(Duration::from_millis(latency_ms)).await;

        self.current_rif.fetch_sub(1, Ordering::SeqCst);
        Ok(Duration::from_millis(latency_ms))
    }

    /// Get current requests in flight
    pub fn get_rif(&self) -> usize {
        self.current_rif.load(Ordering::SeqCst)
    }

    /// Get estimated latency based on current RIF
    pub fn get_estimated_latency(&self) -> u64 {
        let rif = self.current_rif.load(Ordering::SeqCst);
        let antagonist = self.antagonist_load.load(Ordering::SeqCst);
        self.base_latency_ms
            + (rif as u64 * self.latency_per_rif_ms)
            + (self.base_latency_ms * antagonist / 100)
    }

    /// Set antagonist load (0-100)
    pub fn set_antagonist_load(&self, load: u64) {
        self.antagonist_load.store(load.min(100), Ordering::SeqCst);
    }
}

/// A pool of simulated backends
pub struct BackendPool {
    pub backends: Vec<Arc<SimulatedBackend>>,
}

impl BackendPool {
    /// Create a pool with uniform backends
    pub fn uniform(count: usize, base_latency_ms: u64, capacity: usize) -> Self {
        let backends = (0..count)
            .map(|id| {
                Arc::new(SimulatedBackend::new(
                    id,
                    base_latency_ms,
                    2, // 2ms per RIF
                    capacity,
                ))
            })
            .collect();
        Self { backends }
    }

    /// Create a pool with heterogeneous backends (some slower)
    pub fn heterogeneous(
        count: usize,
        base_latency_ms: u64,
        capacity: usize,
        slow_fraction: f64,
        slow_multiplier: u64,
    ) -> Self {
        let slow_count = (count as f64 * slow_fraction) as usize;
        let backends = (0..count)
            .map(|id| {
                let latency = if id < slow_count {
                    base_latency_ms * slow_multiplier
                } else {
                    base_latency_ms
                };
                Arc::new(SimulatedBackend::new(id, latency, 2, capacity))
            })
            .collect();
        Self { backends }
    }

    pub fn len(&self) -> usize {
        self.backends.len()
    }

    pub fn get(&self, id: usize) -> Option<Arc<SimulatedBackend>> {
        self.backends.get(id).cloned()
    }
}
