use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::sync::mpsc::{channel, Sender};
use rand::seq::IteratorRandom;

use crate::backend::Backend;
use crate::probe::{ProbeTable, ProbeResult};

use varnish::ffi::VCL_BACKEND;

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
    backends: Mutex<Vec<Backend>>,
    probe_table: ProbeTable,
    probe_trigger: Sender<()>,
    probe_path: Mutex<String>,
}

impl Director {
    /// Creates a new Director instance along with its probe loop closure.
    /// 
    /// Returns a tuple containing:
    /// - An Arc-wrapped Director instance
    /// - A closure that runs the probe loop when spawned in a thread
    pub fn new() -> (Arc<Self>, impl FnOnce()) {
        let (tx, rx) = channel();

        let inner = Arc::new(Self {
            backends: Mutex::new(Vec::new()),
            probe_table: ProbeTable::new(),
            probe_trigger: tx,
            probe_path: Mutex::new("/probe".to_string()),
        });

        let probe_loop = {
            let inner = Arc::downgrade(&inner);
            move || {
                while let Some(director) = inner.upgrade() {
                    if rx.recv().is_ok() {
                        director.probe_backends();
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
        if let Ok(mut probe_path) = self.probe_path.lock() {
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
        let mut backends = self.backends.lock()
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
        if let Ok(mut backends) = self.backends.lock() {
            if let Some(backend) = backends.iter()
                .find(|b| **b == vcl_backend)
                .cloned() 
            {
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
        let _ = self.probe_trigger.send(());

        if let Some(backend) = self.probe_table.find_best() {
            Ok(backend)
        } else {
            // Fallback: random selection
            if let Ok(backends) = self.backends.lock() {
                if backends.is_empty() {
                    return Err(DirectorError::BackendLockError("No backends available".to_string()));
                }
    
                Ok(backends[rand::random::<usize>() % backends.len()].clone())
            } else {
                Err(DirectorError::BackendLockError("Unable to lock backends".to_string()))
            }
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
        let probe_path = self.probe_path.lock()
            .map(|p| p.clone())
            .unwrap_or_else(|_| "/probe".to_string());

        let url = format!("http://{}{}", backend.address, probe_path);
        ureq::get(&url)
            .timeout(Duration::from_secs(5))
            .set("Host", &backend.name)
    }

    /// Randomly selects and probes a subset of backends.
    /// Updates the probe table with results from successful probes.
    fn probe_backends(&self) {
        let backends_to_probe = if let Ok(backends) = self.backends.lock() {
            println!("Probing backends: {:?}", backends.len());
            let mut rng = rand::thread_rng();
            backends.iter()
                .enumerate()
                .choose_multiple(&mut rng, 3)
                .into_iter()
                .map(|(_, b)| b.clone())
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
                        .and_then(|s| s.parse::<usize>().ok()) {
                            Some(val) => val,
                            None => continue,
                    };

                    let est_latency = match response
                        .header("X-Estimated-Latency")
                        .and_then(|s| s.parse::<usize>().ok()) {
                            Some(val) => val,
                            None => continue,
                    };

                    self.probe_table.add_result(ProbeResult::new(in_flight, est_latency, backend));
                },
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use varnish::ffi::{VCL_BACKEND, director};   
    use std::thread;
    use std::net::{TcpListener, SocketAddr};
    use std::io::{Write, BufReader, BufRead};
    use std::time::Duration;
    use std::fmt::Debug;    

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
        let backend2 = create_test_backend("test2", SocketAddr::from(([127, 0, 0, 2], 8081)), 2);

        // Add backend and verify
        director.add_backend(backend).unwrap();
        assert_eq!(director.backends.lock().unwrap().len(), 1);
        
        // Verify the backend name
        assert_eq!(director.backends.lock().unwrap()[0].name, "test1");

        // Add another backend
        director.add_backend(backend2).unwrap();
        assert_eq!(director.backends.lock().unwrap().len(), 2);

        // Remove a backend
        director.remove_backend(VCL_BACKEND(1 as *const director));
        assert_eq!(director.backends.lock().unwrap().len(), 1);

        // Verify the remaining backend
        assert_eq!(director.backends.lock().unwrap()[0].name, "test2");
        assert_eq!(director.backends.lock().unwrap()[0].address, SocketAddr::from(([127, 0, 0, 2], 8081)));
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
                        if len == 0 || line == "\r\n" { break; }
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

            Self { addr, in_flight, latency, _thread }
        }
    }

    impl Debug for TestServer {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "TestServer {{ addr: {:?}, in_flight: {}, latency: {} }}", self.addr, self.in_flight, self.latency)
        }
    }

    #[test]
    fn test_director_probing() {
        // Create test servers with different loads
        let servers = vec![
            TestServer::new(5, 100),   // Low load
            TestServer::new(10, 200),  // Medium load
            TestServer::new(15, 300),  // High load
        ];

        // Create and configure director
        let (director, probe_loop) = Director::new();
        let _probe_thread = thread::spawn(probe_loop);

        // Add backends
        for (idx, server) in servers.iter().enumerate() {
            let backend = create_test_backend(
                &format!("test{}", idx),
                server.addr,
                idx as u32
            );
            director.add_backend(backend).unwrap();
        }

        println!("Triggering probe manually");
        director.trigger_probe();

        // Wait for probes to complete
        thread::sleep(Duration::from_secs(2));

        // Verify probe results
        if let Some(table) = director.debug_probe_table() {
            println!("Probe table:\n{}", table);
        }

        // The director should prefer the backend with lowest in_flight count
        let selected = director.get_backend().unwrap();
        assert_eq!(selected.address, servers[0].addr);  // Should select the least loaded server
    }
}
