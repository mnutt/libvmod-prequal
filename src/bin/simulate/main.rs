mod backend;
mod balancer;
mod metrics;
mod prequal;
mod scenario;

use std::fs::File;
use std::io::BufWriter;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use rand::Rng;
use rand_distr::{Distribution, Exp};
use tokio::sync::Semaphore;
use tokio::time::{sleep, Instant};

use backend::BackendPool;
use balancer::{balancer_names, create_balancer, LoadBalancer};
use metrics::{print_table, write_csv, Metrics, MetricsSummary};
use prequal::{create_prequal_balancer, PrequalBalancerConfig};
use scenario::{run_antagonist, Scenario};

#[derive(Parser, Debug)]
#[command(author, version, about = "Prequal load balancer simulation")]
struct Args {
    /// Number of backends to simulate
    #[arg(short, long, default_value_t = 100)]
    backends: usize,

    /// Number of requests to send
    #[arg(short = 'n', long, default_value_t = 10000)]
    requests: u64,

    /// Target requests per second
    #[arg(long, default_value_t = 500)]
    rps: u64,

    /// Base latency in milliseconds
    #[arg(short, long, default_value_t = 20)]
    latency: u64,

    /// Backend capacity (max RIF per backend)
    #[arg(short, long, default_value_t = 50)]
    capacity: usize,

    /// Scenarios to run (comma-separated, or "all")
    #[arg(short, long, default_value = "all")]
    scenarios: String,

    /// Strategies to test (comma-separated, or "all")
    #[arg(long, default_value = "all")]
    strategies: String,

    /// Output CSV file
    #[arg(short, long)]
    output: Option<String>,

    /// Maximum concurrent requests
    #[arg(long, default_value_t = 1000)]
    max_concurrent: usize,

    /// Prequal probe table size
    #[arg(long, default_value_t = 16)]
    probe_table_size: usize,

    /// Prequal probes per request
    #[arg(long, default_value_t = 3)]
    probes_per_request: usize,

    /// Quiet mode (only output CSV)
    #[arg(short, long)]
    quiet: bool,
}

fn parse_scenarios(s: &str) -> Vec<Scenario> {
    if s == "all" {
        return Scenario::all();
    }

    s.split(',')
        .filter_map(|name| Scenario::from_name(name.trim()))
        .collect()
}

fn parse_strategies(s: &str) -> Vec<String> {
    if s == "all" {
        let mut strategies: Vec<String> = balancer_names().iter().map(|s| s.to_string()).collect();
        strategies.push("prequal".to_string());
        return strategies;
    }

    s.split(',').map(|s| s.trim().to_string()).collect()
}

fn create_balancer_for_strategy(
    name: &str,
    probe_table_size: usize,
    probes_per_request: usize,
) -> Box<dyn LoadBalancer> {
    if name == "prequal" {
        create_prequal_balancer(PrequalBalancerConfig {
            probe_table_size,
            probes_per_request,
        })
    } else {
        create_balancer(name)
    }
}

async fn run_simulation(
    pool: Arc<BackendPool>,
    balancer: Arc<dyn LoadBalancer>,
    metrics: Arc<Metrics>,
    num_requests: u64,
    rps: u64,
    max_concurrent: usize,
    bursty: bool,
) {
    let semaphore = Arc::new(Semaphore::new(max_concurrent));

    // Calculate inter-arrival time
    let mean_interval = Duration::from_secs_f64(1.0 / rps as f64);

    let mut handles = Vec::with_capacity(num_requests as usize);

    for _ in 0..num_requests {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let pool = pool.clone();
        let balancer = balancer.clone();
        let metrics = metrics.clone();

        let handle = tokio::spawn(async move {
            let start = Instant::now();

            // Select backend
            if let Some(backend) = balancer.select(&pool) {
                match backend.process_request().await {
                    Ok(latency) => {
                        metrics.record_success(latency);
                    }
                    Err(_) => {
                        metrics.record_failure();
                    }
                }
            } else {
                metrics.record_failure();
            }

            drop(permit);
            start.elapsed()
        });

        handles.push(handle);

        // Wait between requests
        if bursty {
            // Exponential inter-arrival times for burstiness
            let exp = Exp::new(1.0 / mean_interval.as_secs_f64()).unwrap();
            let wait = Duration::from_secs_f64(exp.sample(&mut rand::thread_rng()));
            sleep(wait.min(Duration::from_millis(100))).await;
        } else {
            // Add some jitter to avoid perfect synchronization
            let jitter = rand::thread_rng().gen_range(0.8..1.2);
            sleep(Duration::from_secs_f64(
                mean_interval.as_secs_f64() * jitter,
            ))
            .await;
        }
    }

    // Wait for all requests to complete
    for handle in handles {
        let _ = handle.await;
    }
}

async fn run_scenario(scenario: Scenario, strategy: &str, args: &Args) -> MetricsSummary {
    // Create backend pool
    let pool = if scenario.is_heterogeneous() {
        Arc::new(BackendPool::heterogeneous(
            args.backends,
            args.latency,
            args.capacity,
            0.2, // 20% slow
            3,   // 3x slower
        ))
    } else {
        Arc::new(BackendPool::uniform(
            args.backends,
            args.latency,
            args.capacity,
        ))
    };

    // Create balancer
    let balancer: Arc<dyn LoadBalancer> = Arc::from(create_balancer_for_strategy(
        strategy,
        args.probe_table_size,
        args.probes_per_request,
    ));

    // Create metrics
    let metrics = Arc::new(Metrics::new(strategy, scenario.name()));

    // Calculate RPS based on utilization target
    let target_rps = if scenario == Scenario::Overload {
        // For overload, exceed capacity
        let capacity_rps = (args.backends * args.capacity) as f64 / (args.latency as f64 / 1000.0);
        (capacity_rps * scenario.target_utilization()) as u64
    } else {
        args.rps
    };

    // Run antagonist pattern in background
    let antagonist_pool = pool.clone();
    let pattern = scenario.antagonist_pattern();
    let duration = Duration::from_secs_f64(args.requests as f64 / target_rps as f64 + 5.0);

    let antagonist_handle = tokio::spawn(async move {
        run_antagonist(antagonist_pool, pattern, duration).await;
    });

    // Run simulation
    run_simulation(
        pool,
        balancer,
        metrics.clone(),
        args.requests,
        target_rps,
        args.max_concurrent,
        scenario.is_bursty(),
    )
    .await;

    // Wait for antagonist to finish
    antagonist_handle.abort();

    MetricsSummary::from_metrics(&metrics)
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let scenarios = parse_scenarios(&args.scenarios);
    let strategies = parse_strategies(&args.strategies);

    if !args.quiet {
        println!("Prequal Load Balancer Simulation");
        println!("================================");
        println!("Backends: {}", args.backends);
        println!("Requests per run: {}", args.requests);
        println!("Target RPS: {}", args.rps);
        println!("Base latency: {}ms", args.latency);
        println!("Backend capacity: {}", args.capacity);
        println!("Max concurrent: {}", args.max_concurrent);
        println!(
            "Scenarios: {:?}",
            scenarios.iter().map(|s| s.name()).collect::<Vec<_>>()
        );
        println!("Strategies: {:?}", strategies);
        println!();
    }

    let mut summaries = Vec::new();

    let total_runs = scenarios.len() * strategies.len();
    let mut current_run = 0;

    for scenario in &scenarios {
        for strategy in &strategies {
            current_run += 1;

            if !args.quiet {
                println!(
                    "[{}/{}] Running {} with {}...",
                    current_run,
                    total_runs,
                    scenario.name(),
                    strategy
                );
            }

            let summary = run_scenario(*scenario, strategy, &args).await;
            summaries.push(summary);
        }
    }

    // Output results
    if !args.quiet {
        print_table(&summaries);
    }

    // Write CSV if requested
    if let Some(output_path) = &args.output {
        let file = File::create(output_path).expect("Failed to create output file");
        let mut writer = BufWriter::new(file);
        write_csv(&mut writer, &summaries).expect("Failed to write CSV");

        if !args.quiet {
            println!("Results written to: {}", output_path);
        }
    }
}
