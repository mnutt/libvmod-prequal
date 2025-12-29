use std::sync::Arc;
use std::time::Duration;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::time::{interval, sleep};

use super::backend::BackendPool;

/// Antagonist load patterns that affect backend performance
#[derive(Debug, Clone)]
pub enum AntagonistPattern {
    /// No interference
    None,

    /// Random backends get hit with load spikes
    RandomSpikes {
        /// Probability per tick that a spike occurs (0.0-1.0)
        probability: f64,
        /// How long the spike lasts
        duration: Duration,
        /// Extra latency percentage (0-100)
        intensity: u64,
        /// Maximum backends affected per spike
        max_affected: usize,
    },

    /// Some backends are always slower (noisy neighbor)
    NoisyNeighbor {
        /// Fraction of backends affected (0.0-1.0)
        affected_fraction: f64,
        /// Extra latency percentage (0-100)
        intensity: u64,
    },

    /// Correlated failures - groups of backends slow down together
    CorrelatedSpike {
        /// Number of backends in each group
        group_size: usize,
        /// Time between spikes
        interval: Duration,
        /// How long each spike lasts
        duration: Duration,
        /// Extra latency percentage (0-100)
        intensity: u64,
    },

    /// Gradual ramp up and down of load on random backends
    Wave {
        /// Period of the wave
        period: Duration,
        /// Maximum intensity at peak
        max_intensity: u64,
        /// Fraction of backends affected
        affected_fraction: f64,
    },
}

/// Runs the antagonist pattern against the backend pool
pub async fn run_antagonist(pool: Arc<BackendPool>, pattern: AntagonistPattern, duration: Duration) {
    let start = std::time::Instant::now();

    match pattern {
        AntagonistPattern::None => {
            // Just wait for the duration
            sleep(duration).await;
        }

        AntagonistPattern::RandomSpikes {
            probability,
            duration: spike_duration,
            intensity,
            max_affected,
        } => {
            let mut ticker = interval(Duration::from_millis(100));
            let mut rng = StdRng::from_entropy();
            while start.elapsed() < duration {
                ticker.tick().await;

                if rng.gen::<f64>() < probability {
                    // Trigger a spike
                    let num_affected = rng.gen_range(1..=max_affected.min(pool.len()));
                    let affected: Vec<usize> = (0..pool.len())
                        .collect::<Vec<_>>()
                        .into_iter()
                        .take(num_affected)
                        .collect();

                    for id in &affected {
                        if let Some(backend) = pool.get(*id) {
                            backend.set_antagonist_load(intensity);
                        }
                    }

                    // Wait for spike duration then clear
                    sleep(spike_duration).await;

                    for id in affected {
                        if let Some(backend) = pool.get(id) {
                            backend.set_antagonist_load(0);
                        }
                    }
                }
            }
        }

        AntagonistPattern::NoisyNeighbor {
            affected_fraction,
            intensity,
        } => {
            // Set load on affected backends immediately
            let num_affected = (pool.len() as f64 * affected_fraction) as usize;
            for id in 0..num_affected {
                if let Some(backend) = pool.get(id) {
                    backend.set_antagonist_load(intensity);
                }
            }

            // Wait for duration
            sleep(duration).await;

            // Clear load
            for id in 0..num_affected {
                if let Some(backend) = pool.get(id) {
                    backend.set_antagonist_load(0);
                }
            }
        }

        AntagonistPattern::CorrelatedSpike {
            group_size,
            interval: spike_interval,
            duration: spike_duration,
            intensity,
        } => {
            let mut ticker = interval(spike_interval);
            let mut current_group = 0;

            while start.elapsed() < duration {
                ticker.tick().await;

                // Affect a group of backends
                let group_start = (current_group * group_size) % pool.len();
                let group_end = (group_start + group_size).min(pool.len());

                for id in group_start..group_end {
                    if let Some(backend) = pool.get(id) {
                        backend.set_antagonist_load(intensity);
                    }
                }

                sleep(spike_duration).await;

                // Clear the group
                for id in group_start..group_end {
                    if let Some(backend) = pool.get(id) {
                        backend.set_antagonist_load(0);
                    }
                }

                current_group += 1;
            }
        }

        AntagonistPattern::Wave {
            period,
            max_intensity,
            affected_fraction,
        } => {
            let num_affected = (pool.len() as f64 * affected_fraction) as usize;
            let mut ticker = interval(Duration::from_millis(50));

            while start.elapsed() < duration {
                ticker.tick().await;

                // Calculate current intensity based on sine wave
                let elapsed = start.elapsed().as_secs_f64();
                let phase = (elapsed / period.as_secs_f64()) * 2.0 * std::f64::consts::PI;
                let intensity = ((phase.sin() + 1.0) / 2.0 * max_intensity as f64) as u64;

                for id in 0..num_affected {
                    if let Some(backend) = pool.get(id) {
                        backend.set_antagonist_load(intensity);
                    }
                }
            }

            // Clear all
            for id in 0..num_affected {
                if let Some(backend) = pool.get(id) {
                    backend.set_antagonist_load(0);
                }
            }
        }
    }
}

/// Predefined scenarios for testing
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scenario {
    /// Uniform backends, constant load
    SteadyState,
    /// 20% of backends are 3x slower
    HeterogeneousBackends,
    /// Random spikes affecting backends
    AntagonistSpikes,
    /// Overload conditions (load > capacity)
    Overload,
    /// Bursty request patterns
    Bursty,
    /// Combined: heterogeneous + spikes
    Realistic,
    /// Persistent noisy neighbors (some backends always slower)
    NoisyNeighbor,
    /// Correlated failures (groups of backends fail together)
    CorrelatedFailure,
    /// Wave pattern (sinusoidal load variation)
    WaveLoad,
}

impl Scenario {
    pub fn all() -> Vec<Scenario> {
        vec![
            Scenario::SteadyState,
            Scenario::HeterogeneousBackends,
            Scenario::AntagonistSpikes,
            Scenario::Overload,
            Scenario::Bursty,
            Scenario::Realistic,
            Scenario::NoisyNeighbor,
            Scenario::CorrelatedFailure,
            Scenario::WaveLoad,
        ]
    }

    pub fn name(&self) -> &'static str {
        match self {
            Scenario::SteadyState => "steady-state",
            Scenario::HeterogeneousBackends => "heterogeneous",
            Scenario::AntagonistSpikes => "antagonist-spikes",
            Scenario::Overload => "overload",
            Scenario::Bursty => "bursty",
            Scenario::Realistic => "realistic",
            Scenario::NoisyNeighbor => "noisy-neighbor",
            Scenario::CorrelatedFailure => "correlated-failure",
            Scenario::WaveLoad => "wave-load",
        }
    }

    pub fn from_name(name: &str) -> Option<Scenario> {
        match name {
            "steady-state" => Some(Scenario::SteadyState),
            "heterogeneous" => Some(Scenario::HeterogeneousBackends),
            "antagonist-spikes" => Some(Scenario::AntagonistSpikes),
            "overload" => Some(Scenario::Overload),
            "bursty" => Some(Scenario::Bursty),
            "realistic" => Some(Scenario::Realistic),
            "noisy-neighbor" => Some(Scenario::NoisyNeighbor),
            "correlated-failure" => Some(Scenario::CorrelatedFailure),
            "wave-load" => Some(Scenario::WaveLoad),
            _ => None,
        }
    }

    /// Get the antagonist pattern for this scenario
    pub fn antagonist_pattern(&self) -> AntagonistPattern {
        match self {
            Scenario::SteadyState => AntagonistPattern::None,
            Scenario::HeterogeneousBackends => AntagonistPattern::None,
            Scenario::AntagonistSpikes => AntagonistPattern::RandomSpikes {
                probability: 0.1,
                duration: Duration::from_secs(2),
                intensity: 100,
                max_affected: 10,
            },
            Scenario::Overload => AntagonistPattern::None,
            Scenario::Bursty => AntagonistPattern::None,
            Scenario::Realistic => AntagonistPattern::RandomSpikes {
                probability: 0.05,
                duration: Duration::from_secs(1),
                intensity: 50,
                max_affected: 5,
            },
            Scenario::NoisyNeighbor => AntagonistPattern::NoisyNeighbor {
                affected_fraction: 0.1, // 10% of backends are slow
                intensity: 150,         // 150% extra latency
            },
            Scenario::CorrelatedFailure => AntagonistPattern::CorrelatedSpike {
                group_size: 10,                        // 10 backends fail together
                interval: Duration::from_secs(3),     // every 3 seconds
                duration: Duration::from_millis(500), // for 500ms
                intensity: 200,                       // 200% extra latency
            },
            Scenario::WaveLoad => AntagonistPattern::Wave {
                period: Duration::from_secs(5),
                max_intensity: 100, // 0-100% extra latency in sine wave
                affected_fraction: 0.3,
            },
        }
    }

    /// Whether this scenario uses heterogeneous backends
    pub fn is_heterogeneous(&self) -> bool {
        matches!(
            self,
            Scenario::HeterogeneousBackends | Scenario::Realistic
        )
    }

    /// Target utilization for this scenario (fraction of capacity)
    pub fn target_utilization(&self) -> f64 {
        match self {
            Scenario::Overload => 1.2, // 120% of capacity
            _ => 0.7,                  // 70% of capacity
        }
    }

    /// Whether requests arrive in bursts
    pub fn is_bursty(&self) -> bool {
        matches!(self, Scenario::Bursty | Scenario::Realistic)
    }
}
