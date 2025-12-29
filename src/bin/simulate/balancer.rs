use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use rand::seq::SliceRandom;
use rand::Rng;

use super::backend::{BackendPool, SimulatedBackend};

/// Trait for load balancing strategies
pub trait LoadBalancer: Send + Sync {
    /// Select a backend for the next request
    fn select(&self, pool: &BackendPool) -> Option<Arc<SimulatedBackend>>;
}

/// Random selection - baseline
pub struct RandomBalancer;

impl LoadBalancer for RandomBalancer {
    fn select(&self, pool: &BackendPool) -> Option<Arc<SimulatedBackend>> {
        if pool.backends.is_empty() {
            return None;
        }
        let idx = rand::thread_rng().gen_range(0..pool.backends.len());
        pool.get(idx)
    }
}

/// Round-robin selection
pub struct RoundRobinBalancer {
    next: AtomicUsize,
}

impl RoundRobinBalancer {
    pub fn new() -> Self {
        Self {
            next: AtomicUsize::new(0),
        }
    }
}

impl LoadBalancer for RoundRobinBalancer {
    fn select(&self, pool: &BackendPool) -> Option<Arc<SimulatedBackend>> {
        if pool.backends.is_empty() {
            return None;
        }
        let idx = self.next.fetch_add(1, Ordering::SeqCst) % pool.backends.len();
        pool.get(idx)
    }
}

/// Least connections (lowest RIF)
pub struct LeastConnectionsBalancer;

impl LoadBalancer for LeastConnectionsBalancer {
    fn select(&self, pool: &BackendPool) -> Option<Arc<SimulatedBackend>> {
        pool.backends.iter().min_by_key(|b| b.get_rif()).cloned()
    }
}

/// Lowest estimated latency
pub struct LowestLatencyBalancer;

impl LoadBalancer for LowestLatencyBalancer {
    fn select(&self, pool: &BackendPool) -> Option<Arc<SimulatedBackend>> {
        pool.backends
            .iter()
            .min_by_key(|b| b.get_estimated_latency())
            .cloned()
    }
}

/// Power of Two Choices - sample 2 random backends, pick best by RIF
pub struct PowerOfTwoBalancer;

impl LoadBalancer for PowerOfTwoBalancer {
    fn select(&self, pool: &BackendPool) -> Option<Arc<SimulatedBackend>> {
        if pool.backends.is_empty() {
            return None;
        }
        if pool.backends.len() == 1 {
            return pool.get(0);
        }

        let mut rng = rand::thread_rng();
        let sample: Vec<_> = pool.backends.choose_multiple(&mut rng, 2).collect();

        sample.into_iter().min_by_key(|b| b.get_rif()).cloned()
    }
}

/// Power of D Choices with RIF only (no HCL)
pub struct PowerOfDRifBalancer {
    pub d: usize,
}

impl LoadBalancer for PowerOfDRifBalancer {
    fn select(&self, pool: &BackendPool) -> Option<Arc<SimulatedBackend>> {
        if pool.backends.is_empty() {
            return None;
        }

        let mut rng = rand::thread_rng();
        let sample: Vec<_> = pool
            .backends
            .choose_multiple(&mut rng, self.d.min(pool.backends.len()))
            .collect();

        sample.into_iter().min_by_key(|b| b.get_rif()).cloned()
    }
}

/// Power of D Choices with latency only (no HCL)
pub struct PowerOfDLatencyBalancer {
    pub d: usize,
}

impl LoadBalancer for PowerOfDLatencyBalancer {
    fn select(&self, pool: &BackendPool) -> Option<Arc<SimulatedBackend>> {
        if pool.backends.is_empty() {
            return None;
        }

        let mut rng = rand::thread_rng();
        let sample: Vec<_> = pool
            .backends
            .choose_multiple(&mut rng, self.d.min(pool.backends.len()))
            .collect();

        sample
            .into_iter()
            .min_by_key(|b| b.get_estimated_latency())
            .cloned()
    }
}

/// Create a balancer by name
pub fn create_balancer(name: &str) -> Box<dyn LoadBalancer> {
    match name {
        "random" => Box::new(RandomBalancer),
        "round-robin" => Box::new(RoundRobinBalancer::new()),
        "least-conn" => Box::new(LeastConnectionsBalancer),
        "lowest-latency" => Box::new(LowestLatencyBalancer),
        "power-of-2" => Box::new(PowerOfTwoBalancer),
        "power-of-d-rif" => Box::new(PowerOfDRifBalancer { d: 5 }),
        "power-of-d-latency" => Box::new(PowerOfDLatencyBalancer { d: 5 }),
        _ => Box::new(RandomBalancer),
    }
}

/// List all available balancer names
pub fn balancer_names() -> Vec<&'static str> {
    vec![
        "random",
        "round-robin",
        "least-conn",
        "lowest-latency",
        "power-of-2",
        "power-of-d-rif",
        "power-of-d-latency",
    ]
}
