use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::sync::mpsc::{channel, Sender};
use rand::seq::IteratorRandom;

use crate::backend::Backend;
use crate::probe::{ProbeTable, ProbeResult};

use varnish::ffi::VCL_BACKEND;
use varnish::vcl::{Ctx, VclError, LogTag};

const PROBE_INTERVAL: Duration = Duration::from_secs(5);

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
                    if rx.recv_timeout(PROBE_INTERVAL).is_ok() {
                        director.probe_backends();
                    }
                    director.probe_backends();
                }
            }
        };

        (inner, probe_loop)
    }

    pub fn set_probe_path(&self, path: &str) {
        if let Ok(mut probe_path) = self.probe_path.lock() {
            *probe_path = path.to_string();
        }
    }

    pub fn add_backend(&self, backend: Backend) -> Result<(), DirectorError> {
        if let Ok(mut backends) = self.backends.lock() {
            backends.push(backend);
            let _ = self.probe_trigger.send(());
            Ok(())
        } else {
            Err(DirectorError::BackendLockError("Failed to lock backends".to_string()))
        }        
    }

    pub fn remove_backend(&self, vcl_backend: VCL_BACKEND) {
        if let Ok(mut backends) = self.backends.lock() {
            if let Some(backend) = backends.iter()
                .find(|b| b.vcl_backend.0 == vcl_backend.0)
                .cloned() 
            {
                self.probe_table.remove_backend(backend);
                
                backends.retain(|b| b.vcl_backend.0 != vcl_backend.0);
            }
        }
    }
    
    pub fn log_probe_table(&self, ctx: &mut Ctx) {
        if let Some(table) = self.probe_table.display_results() {
            ctx.log(LogTag::Debug, &format!("Probe table state:{}", table));
        }
    }

    pub fn get_backend(&self, ctx: &mut Ctx) -> Result<VCL_BACKEND, VclError> {
        let backends = self.backends.lock().unwrap();
        if backends.is_empty() {
            return Err(VclError::new("No backends available".to_string()));
        }

        self.log_probe_table(ctx);

        let _ = self.probe_trigger.send(());

        if let Some(backend) = self.probe_table.find_best() {
            return Ok(backend.vcl_backend);
        }

        // Fallback: random selection
        Ok(backends[rand::random::<usize>() % backends.len()].vcl_backend)
    }

    fn construct_probe_request(&self, backend: &Backend) -> ureq::Request {
        let probe_path = self.probe_path.lock()
            .map(|p| p.clone())
            .unwrap_or_else(|_| "/probe".to_string());

        let url = format!("http://{}{}", backend.address, probe_path);
        ureq::get(&url)
            .timeout(Duration::from_secs(5))
            .set("Host", &backend.name)
    }

    fn probe_backends(&self) {
        if let Ok(backends) = self.backends.lock() {
            let mut rng = rand::thread_rng();
            let selected = (0..backends.len()).choose_multiple(&mut rng, 3);

            for &idx in &selected {
                let backend = &backends[idx];
                let request = self.construct_probe_request(backend);

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

                        self.probe_table
                            .add_result(ProbeResult::new(in_flight, est_latency, backend.clone()));
                    },
                    Err(_) => {
                        continue;
                    }
                }
            }
        }
    }

    pub fn is_healthy(&self) -> bool {
        // Only healthy if we have valid probe results
        self.probe_table.has_probes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use varnish::ffi::{VCL_BACKEND, director};   
    use std::net::SocketAddr;    

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
}