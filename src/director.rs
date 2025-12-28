use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use rand::seq::IteratorRandom;
use varnish::ffi::VCL_BACKEND;

use crate::backend::Backend;
use crate::probe::{ProbeResult, ProbeTable, PROBE_TABLE_SIZE};

#[derive(Debug)]
pub enum DirectorError {
    BackendLockError(String),
}

impl std::fmt::Display for DirectorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DirectorError::BackendLockError(msg) => write!(f, "Backend lock error: {}", msg),
        }
    }
}

impl std::error::Error for DirectorError {}

pub struct Director {
    backends: RwLock<Vec<Backend>>,
    probe_table: ProbeTable,
    probe_trigger: Sender<()>,
    probe_path: RwLock<String>,
}

const PROBE_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_PROBE_COUNT: usize = 3;

impl Director {
    /// Creates a new Director instance along with its probe loop closure.
    ///
    /// Returns a tuple containing:
    /// - An Arc-wrapped Director instance
    /// - A closure that runs the probe loop when spawned in a thread
    pub fn new() -> (Arc<Self>, impl FnOnce()) {
        let (tx, rx) = channel();

        let inner = Arc::new(Self {
            backends: RwLock::new(Vec::new()),
            probe_table: ProbeTable::new(),
            probe_trigger: tx,
            probe_path: RwLock::new("/probe".to_string()),
        });

        let probe_loop = {
            let inner = Arc::downgrade(&inner);
            move || {
                while let Some(director) = inner.upgrade() {
                    // Wait for trigger or timeout
                    if rx.recv_timeout(PROBE_INTERVAL).is_ok() {
                        director.probe_backends(DEFAULT_PROBE_COUNT);
                    } else {
                        // Ensure probe pool every interval
                        director.ensure_probe_pool();
                    }
                }
            }
        };

        (inner, probe_loop)
    }

    /// Sets the HTTP path used for health check probes.
    ///
    /// # Arguments
    /// * `path` - The URL path to use for probe requests (e.g. "/probe")
    pub fn set_probe_path(&self, path: &str) {
        if let Ok(mut probe_path) = self.probe_path.write() {
            *probe_path = path.to_string();
        }
    }

    /// Adds a backend to the director's pool.
    ///
    /// # Arguments
    /// * `backend` - The backend to add
    ///
    /// # Returns
    /// * `Ok(())` if the backend was added successfully
    /// * `Err(DirectorError)` if the backend could not be added
    pub fn add_backend(&self, backend: Backend) -> Result<(), DirectorError> {
        let mut backends = self
            .backends
            .write()
            .map_err(|e| DirectorError::BackendLockError(e.to_string()))?;

        backends.push(backend);
        Ok(())
    }

    /// Removes a backend from the pool by its VCL_BACKEND reference.
    /// Also removes any probe results for this backend.
    ///
    /// # Arguments
    /// * `vcl_backend` - The VCL_BACKEND reference to remove
    pub fn remove_backend(&self, vcl_backend: VCL_BACKEND) {
        if let Ok(mut backends) = self.backends.write() {
            if let Some(backend) = backends.iter().find(|b| **b == vcl_backend).cloned() {
                self.probe_table.remove_backend(backend);
                backends.retain(|b| *b != vcl_backend);
            }
        }
    }

    pub fn trigger_probe(&self) {
        let _ = self.probe_trigger.send(());
    }

    /// Returns a string representation of the probe table, for debugging.
    ///
    /// # Returns
    /// * `Some(String)` - The probe table as a string
    /// * `None` - If the probe table could not be locked
    pub fn debug_probe_table(&self) -> Option<String> {
        self.probe_table.display_results()
    }

    /// Gets the best available backend based on probe results.
    /// Falls back to random selection if no probe results are available.
    ///
    /// # Returns
    /// * `Ok(VCL_BACKEND)` - The selected backend
    /// * `Err(VclError)` - If no backends are available
    pub fn get_backend(&self) -> Result<Backend, DirectorError> {
        let backends = self
            .backends
            .read()
            .map_err(|e| DirectorError::BackendLockError(e.to_string()))?;

        if backends.is_empty() {
            return Err(DirectorError::BackendLockError(
                "No backends available".to_string(),
            ));
        }

        let _ = self.probe_trigger.send(());

        if let Some(backend) = self.probe_table.find_best() {
            Ok(backend)
        } else {
            // Fallback: random selection
            Ok(backends[rand::random::<usize>() % backends.len()].clone())
        }
    }

    /// Constructs a probe request for a backend.
    ///
    /// # Arguments
    /// * `backend` - The backend to probe
    ///
    /// # Returns
    /// A configured HTTP request ready to be sent
    fn construct_probe_request(&self, backend: &Backend) -> ureq::Request {
        let probe_path = self
            .probe_path
            .read()
            .map(|p| p.clone())
            .unwrap_or_else(|_| "/probe".to_string());

        let url = format!("http://{}{}", backend.address, probe_path);
        ureq::get(&url)
            .timeout(Duration::from_secs(5))
            .set("Host", &backend.name)
    }

    /// Randomly selects and probes a subset of backends.
    /// Updates the probe table with results from successful probes.
    fn probe_backends(&self, count: usize) {
        let backends_to_probe = if let Ok(backends) = self.backends.read() {
            let mut rng = rand::thread_rng();
            backends
                .iter()
                .choose_multiple(&mut rng, count)
                .into_iter()
                .cloned()
                .collect::<Vec<_>>()
        } else {
            return;
        };

        for backend in backends_to_probe {
            let request = self.construct_probe_request(&backend);

            match request.call() {
                Ok(response) => {
                    if response.status() != 200 {
                        continue;
                    }

                    let in_flight = match response
                        .header("X-In-Flight")
                        .and_then(|s| s.parse::<usize>().ok())
                    {
                        Some(val) => val,
                        None => continue,
                    };

                    let est_latency = match response
                        .header("X-Estimated-Latency")
                        .and_then(|s| s.parse::<usize>().ok())
                    {
                        Some(val) => val,
                        None => continue,
                    };

                    let now = SystemTime::now();
                    self.probe_table.add_result(ProbeResult::new(
                        now,
                        in_flight,
                        est_latency,
                        backend,
                    ));
                }
                Err(_) => continue,
            }
        }
    }

    /// Checks if the director has any valid probe results.
    ///
    /// # Returns
    /// `true` if there are valid probe results, `false` otherwise
    pub fn is_healthy(&self) -> bool {
        // Only healthy if we have valid probe results
        self.probe_table.has_probes()
    }

    fn ensure_probe_pool(&self) {
        if !self.probe_table.has_enough_probes() {
            self.probe_backends(PROBE_TABLE_SIZE / 2);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use std::io::{BufRead, BufReader, Write};
    use std::net::{SocketAddr, TcpListener};
    use std::thread;
    use std::time::Duration;

    use varnish::ffi::{director, VCL_BACKEND};

    use super::*;

    fn create_test_backend(name: &str, addr: SocketAddr, director_id: u32) -> Backend {
        Backend {
            name: name.to_string(),
            address: addr,
            vcl_backend: VCL_BACKEND(director_id as *const director), // fake VCL_BACKEND reference
        }
    }

    #[test]
    fn test_director_add_remove_backend() {
        let (director, _) = Director::new();

        let backend = create_test_backend("test1", SocketAddr::from(([127, 0, 0, 1], 8080)), 1);
        let backend1_ref = backend.vcl_backend;
        let backend2 = create_test_backend("test2", SocketAddr::from(([127, 0, 0, 2], 8081)), 2);

        // Add backend and verify
        director.add_backend(backend).unwrap();
        assert_eq!(director.backends.read().unwrap().len(), 1);

        // Verify the backend name
        assert_eq!(director.backends.read().unwrap()[0].name, "test1");

        // Add another backend
        director.add_backend(backend2).unwrap();
        assert_eq!(director.backends.read().unwrap().len(), 2);

        // Remove the first backend
        director.remove_backend(backend1_ref);
        assert_eq!(director.backends.read().unwrap().len(), 1);

        // Verify the remaining backend
        assert_eq!(director.backends.read().unwrap()[0].name, "test2");
        assert_eq!(
            director.backends.read().unwrap()[0].address,
            SocketAddr::from(([127, 0, 0, 2], 8081))
        );
    }

    #[test]
    fn test_director_get_backend() {
        let (director, _) = Director::new();
        let backend = create_test_backend("test1", SocketAddr::from(([127, 0, 0, 1], 8080)), 1);
        director.add_backend(backend).unwrap();
        let backend = director.get_backend().unwrap();
        assert_eq!(backend.name, "test1");
    }

    struct TestServer {
        addr: SocketAddr,
        in_flight: usize,
        latency: usize,
        _thread: thread::JoinHandle<()>,
    }

    impl TestServer {
        fn new(in_flight: usize, latency: usize) -> Self {
            // Bind to a random high port
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            let _thread = thread::spawn(move || {
                for stream in listener.incoming() {
                    let mut stream = stream.unwrap();
                    let mut reader = BufReader::new(&stream);
                    let mut line = String::new();

                    // Read the request
                    while let Ok(len) = reader.read_line(&mut line) {
                        if len == 0 || line == "\r\n" {
                            break;
                        }
                        line.clear();
                    }

                    // Send response
                    let response = format!(
                        "HTTP/1.1 200 OK\r\n\
                         X-In-Flight: {}\r\n\
                         X-Estimated-Latency: {}\r\n\
                         Content-Length: 2\r\n\
                         \r\n\
                         OK",
                        in_flight, latency
                    );
                    stream.write_all(response.as_bytes()).unwrap();
                }
            });

            Self {
                addr,
                in_flight,
                latency,
                _thread,
            }
        }
    }

    impl Debug for TestServer {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(
                f,
                "TestServer {{ addr: {:?}, in_flight: {}, latency: {} }}",
                self.addr, self.in_flight, self.latency
            )
        }
    }

    #[test]
    fn test_director_probing() {
        // Create test servers with different loads
        let servers = [
            TestServer::new(5, 100),  // Low load
            TestServer::new(10, 200), // Medium load
            TestServer::new(15, 300), // High load
        ];

        // Create and configure director
        let (director, probe_loop) = Director::new();
        let _probe_thread = thread::spawn(probe_loop);

        // Add backends
        for (idx, server) in servers.iter().enumerate() {
            let backend = create_test_backend(&format!("test{}", idx), server.addr, idx as u32);
            director.add_backend(backend).unwrap();
        }

        director.trigger_probe();

        // Wait for probes to complete
        thread::sleep(Duration::from_secs(2));

        // Verify probe results
        if let Some(table) = director.debug_probe_table() {
            println!("Probe table:\n{}", table);
        }

        // The director should prefer the backend with lowest in_flight count
        let selected = director.get_backend().unwrap();
        assert_eq!(selected.address, servers[0].addr); // Should select the least loaded server
    }

    #[test]
    fn test_director_probe_table_health() {
        let mut servers = Vec::new();
        for i in 0..100 {
            servers.push(TestServer::new(
                i % 20,         // RIF varies from 0-19
                100 + (i * 10), // Latency increases with index
            ));
        }

        let (director, probe_loop) = Director::new();
        let _probe_thread = thread::spawn(probe_loop);

        for (idx, server) in servers.iter().enumerate() {
            let backend = create_test_backend(&format!("test{}", idx), server.addr, idx as u32);
            director.add_backend(backend).unwrap();
        }

        director.trigger_probe();
        thread::sleep(Duration::from_secs(1));

        if let Some(table) = director.debug_probe_table() {
            println!("Probe table at beginning: \n{}", table);
        }

        for i in 0..1000 {
            let backend = director.get_backend().unwrap();
            assert!(
                backend.name.starts_with("test"),
                "Backend name should start with 'test'"
            );

            if i > 0 && i % 100 == 0 {
                if let Some(table) = director.debug_probe_table() {
                    println!("Probe table at request {}: \n{}", i, table);
                }

                assert!(
                    director.probe_table.has_enough_probes(),
                    "Probe table should be at least half full at request {} but had {}",
                    i,
                    director.probe_table.len()
                );
            }

            // sleep for a bit in between requests
            thread::sleep(Duration::from_millis(2));
        }

        assert!(
            director.probe_table.has_enough_probes(),
            "Probe table should be sufficiently full after test"
        );
    }
}
