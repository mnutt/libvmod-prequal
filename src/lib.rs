use std::marker::Send;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};
use std::fmt;
use std::ffi::CStr;
use std::ffi::CString;
use std::net::SocketAddr;

use rand::seq::IteratorRandom;
use varnish::ffi::{VCL_BACKEND, BACKEND_MAGIC, DIRECTOR_MAGIC, backend};
use varnish::vcl::{Ctx, VclError, LogTag};

const PROBE_TABLE_SIZE: usize = 16;
const PROBE_INTERVAL: Duration = Duration::from_secs(5);
const MAX_USES_BEFORE_EXPIRE: usize = 3;

#[derive(Debug, Clone)]
struct Backend {
    name: String,
    address: SocketAddr,
    backend: VCL_BACKEND,
}

#[derive(Debug)]
pub enum BackendError {
    InvalidBackendMagic,
    InvalidDirectorMagic,
    NoProbe,
    InvalidProbe,
    NoProbeURL,
    InvalidProbeURL,
    InvalidAddress,
    InvalidEndpoint,
}

impl Backend {
    fn new(backend_director: VCL_BACKEND) -> Result<Self, BackendError> {
        unsafe {            
            if (*backend_director.0).magic != DIRECTOR_MAGIC {
                return Err(BackendError::InvalidDirectorMagic);
            }

            let backend = (*backend_director.0).priv_ as *const backend;            

            // While directors typically allow any director as a backend, we want to make sure
            // we are only dealing with real backends
            if (*backend).magic != BACKEND_MAGIC {
               return Err(BackendError::InvalidBackendMagic);
            }

            let name = Self::name_from_backend(backend);
            let address = Self::address_from_backend(backend)?;
                
            Ok(Self {
                name,
                address,
                backend: backend_director,
            })
        }
    }

    fn name_from_backend(backend: *const backend) -> String {
        unsafe {
            let name_ptr = (*backend).vcl_name;
            if !name_ptr.is_null() {
                CStr::from_ptr(name_ptr)
                    .to_str()
                    .map(String::from)
                    .unwrap_or_else(|_| format!("backend_{}", rand::random::<u32>()))
            } else {
                format!("backend_{}", rand::random::<u32>())
            }
        }
    }

    fn address_from_backend(backend: *const backend) -> Result<SocketAddr, BackendError> {
        //#[cfg(test)]
        //return Ok(SocketAddr::from(([127, 0, 0, 1], 8080)));

        unsafe {
            let endpoint = *(*backend).endpoint;
            Ok(Option::<SocketAddr>::from(endpoint.ipv4)
                .ok_or(BackendError::InvalidAddress)?)
        }
    }
}

impl fmt::Display for Backend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

// Update safety implementations
unsafe impl Send for Backend {}
unsafe impl Sync for Backend {}

#[derive(Debug)]
struct ProbeResult {
    timestamp: SystemTime,
    in_flight: usize,
    est_latency: usize,
    used_count: AtomicUsize,
    backend: Backend,
}

impl ProbeResult {
    fn new(in_flight: usize, est_latency: usize, backend: Backend) -> Self {
        Self {
            timestamp: SystemTime::now(),
            in_flight,
            est_latency,
            used_count: AtomicUsize::new(0),
            backend,
        }
    }

    fn increment_used(&self) -> usize {
        self.used_count.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn is_expired(&self) -> bool {
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
struct ProbeTable {
    results: Mutex<Vec<Option<ProbeResult>>>,
    next_index: AtomicUsize,
}

impl ProbeTable {
    fn new() -> Self {
        Self {
            results: Mutex::new(vec![None; PROBE_TABLE_SIZE]),
            next_index: AtomicUsize::new(0),
        }
    }

    fn add_result(&self, result: ProbeResult) {
        let idx = self.next_index.fetch_add(1, Ordering::SeqCst) % PROBE_TABLE_SIZE;
        if let Ok(mut results) = self.results.lock() {
            results[idx] = Some(result);
            self.next_index.store(idx, Ordering::SeqCst);
        }
    }

    fn find_best(&self) -> Option<VCL_BACKEND> {
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

        // Get the probe and increment its usage
        let probe = results[best_idx].as_mut()?;
        probe.increment_used();
        
        Some(probe.backend.backend)
    }

    fn remove_backend(&self, backend: VCL_BACKEND) {
        if let Ok(mut results) = self.results.lock() {
            for probe in results.iter_mut() {
                if let Some(p) = probe {
                    if p.backend.backend.0 == backend.0 {
                        *probe = None;
                    }
                }
            }
        }
    }
}

struct DirectorInner {
    backends: Mutex<Vec<Backend>>,
    probe_table: ProbeTable,
    probe_trigger: Sender<()>,
}

impl DirectorInner {
    fn new() -> (Arc<Self>, impl FnOnce()) {
        let (tx, rx) = channel();

        let inner = Arc::new(Self {
            backends: Mutex::new(Vec::new()),
            probe_table: ProbeTable::new(),
            probe_trigger: tx,
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

    fn probe_backends(&self) {
        if let Ok(backends) = self.backends.lock() {
            let mut rng = rand::thread_rng();
            let selected = (0..backends.len()).choose_multiple(&mut rng, 3);

            for &idx in &selected {
                // TODO: Implement actual probe request logic
                let in_flight = rand::random::<usize>() % 100;
                let est_latency = rand::random::<usize>() % 1000;
                self.probe_table
                    .add_result(ProbeResult::new(in_flight, est_latency, backends[idx].clone()));
            }
        }
    }
}

#[allow(non_camel_case_types)]
pub struct director {
    inner: Arc<DirectorInner>,
}

#[varnish::vmod]
mod prequal {
    use super::*;

    impl director {
        /// Create a new director
        ///
        /// This starts a background thread that periodically probes backends to determine their load.
        /// The thread will automatically clean up when the director is dropped.
        pub fn new(_ctx: &mut Ctx) -> Result<Self, VclError> {
            let (inner, probe_loop) = DirectorInner::new();
            thread::spawn(probe_loop);
            Ok(Self { inner })
        }

        /// Add a backend to the director's pool
        ///
        /// The backend will be included in future probe operations and can be selected
        /// for request handling.
        pub fn add_backend(&self, backend: VCL_BACKEND) -> Result<(), VclError> {
            match Backend::new(backend) {
                Ok(backend) => {
                    if let Ok(mut backends) = self.inner.backends.lock() {
                        backends.push(backend);
                    }
                    let _ = self.inner.probe_trigger.send(());
                    Ok(())
                }
                Err(e) => Err(VclError::new(format!("Invalid backend: {:?}", e)))
            }
        }

        /// Remove a backend from the director's pool
        ///
        /// The backend will no longer be considered for request handling.
        /// Any existing probe results for this backend will be allowed to expire naturally.
        pub fn remove_backend(&self, backend: VCL_BACKEND) {
            if let Ok(mut backends) = self.inner.backends.lock() {
                backends.retain(|b| b.backend.0 != backend.0);
            }
            self.inner.probe_table.remove_backend(backend);
        }

        /// Get the next backend for request handling
        ///
        /// # Safety
        ///
        /// This method is unsafe because:
        /// - It returns a raw pointer to a Varnish director (VCL_BACKEND)
        /// - Must only be called from within a Varnish VCL context
        pub unsafe fn backend(&self, ctx: &mut Ctx) -> Result<VCL_BACKEND, VclError> {
            let backends = self.inner.backends.lock().unwrap();
            if backends.is_empty() {
                return Err(VclError::new("No backends available".to_string()));
            }

            ctx.log(LogTag::Error, "probe table state");

            // Log the probe table state
            if let Ok(results) = self.inner.probe_table.results.lock() {
                for (idx, probe) in results.iter().enumerate() {
                    if let Some(probe) = probe {
                        ctx.log(LogTag::Error, format!(
                            "probe[{}]: backend={}, in_flight={}, latency={}",
                            idx,
                            probe.backend,
                            probe.in_flight,
                            probe.est_latency
                        ));
                    }
                }
            }

            let _ = self.inner.probe_trigger.send(());

            // find_best now handles usage tracking
            if let Some(backend) = self.inner.probe_table.find_best() {
                return Ok(backend);
            }

            // Fallback: random selection
            Ok(backends[rand::random::<usize>() % backends.len()].backend)
        }

        pub fn healthy(&self, _ctx: &mut Ctx) -> bool {
            // Only healthy if we have valid probe results
            self.inner
                .probe_table
                .results
                .lock()
                .map(|results| results.iter().any(|p| p.is_some()))
                .unwrap_or(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ptr;
    use varnish::vcl::TestCtx;
    use varnish::ffi::{VCL_IP, VRT_ENDPOINT_MAGIC, backend, vrt_endpoint, suckaddr};
    use std::ffi::c_void;
    use super::*;
    
    varnish::run_vtc_tests!("tests/*.vtc");

    // Size of suckaddr - large enough for both IPv4 and IPv6
    const SUCKADDR_SIZE: usize = 128;
    const VSA_MAGIC: u32 = 0x4b1e9335;

    // Our test-specific suckaddr implementation
    #[repr(C)]
    struct TestSuckaddr {
        magic: u32,
        data: [u8; SUCKADDR_SIZE - size_of::<u32>()],  // Reduced to account for magic number
    }

    fn create_suckaddr(addr: SocketAddr) -> Box<suckaddr> {
        let mut test_addr = Box::new(TestSuckaddr {
            magic: VSA_MAGIC,
            data: [0; SUCKADDR_SIZE - size_of::<u32>()],
        });

        // Set up the sockaddr fields
        unsafe {
            let bytes = test_addr.data.as_mut_ptr();
            match addr {
                SocketAddr::V4(addr4) => {
                    *bytes.add(0) = 4;              // len
                    *bytes.add(1) = 2;              // AF_INET
                    // Port needs to be in network byte order (big-endian)
                    let port = addr4.port();
                    *bytes.add(2) = ((port & 0xFF00) >> 8) as u8;  // High byte
                    *bytes.add(3) = (port & 0xFF) as u8;           // Low byte
                    let octets = addr4.ip().octets();
                    *bytes.add(4) = octets[0];
                    *bytes.add(5) = octets[1];
                    *bytes.add(6) = octets[2];
                    *bytes.add(7) = octets[3];
                },
                SocketAddr::V6(_) => todo!("IPv6 support"),
            }
        }

        unsafe { std::mem::transmute(test_addr) }
    }

    fn create_test_backend(name: &str, addr: SocketAddr) -> VCL_BACKEND {
        // Allocate and leak the strings
        let name_cstr = CString::new(name).unwrap();
        let name_ptr = name_cstr.into_raw();

        // Create suckaddr from SocketAddr
        let suckaddr = create_suckaddr(addr);

        let endpoint = Box::new(vrt_endpoint {
            magic: VRT_ENDPOINT_MAGIC,
            ipv4: VCL_IP(Box::into_raw(suckaddr) as *const _),
            ipv6: VCL_IP(ptr::null()),
            uds_path: ptr::null(),
            preamble: ptr::null(),
        });
        let endpoint_ptr = Box::into_raw(endpoint);

        // Create the backend structure
        let backend = Box::new(backend {
            magic: BACKEND_MAGIC,
            n_conn: 0,
            endpoint: endpoint_ptr,
            vcl_name: name_ptr,
            hosthdr: name_ptr,
            authority: ptr::null_mut(),
            connect_timeout: varnish::ffi::vtim_dur(3.5),
            first_byte_timeout: varnish::ffi::vtim_dur(15.0),
            between_bytes_timeout: varnish::ffi::vtim_dur(5.0),
            backend_wait_timeout: varnish::ffi::vtim_dur(10.0),
            max_connections: 100,
            proxy_header: 0,
            backend_wait_limit: 0,
            sick: 0,
            changed: varnish::ffi::vtim_real(0.0),
            probe: ptr::null_mut(),
            vsc_seg: ptr::null_mut(),
            vsc: ptr::null_mut(),
            conn_pool: ptr::null_mut(),
            director: VCL_BACKEND(ptr::null()),
            cw_head: unsafe { std::mem::zeroed() },  // Initialize VTAILQ_HEAD
            cw_count: 0,
        });
        let backend_ptr = Box::into_raw(backend);

        // Create the director structure
        let director = Box::new(varnish::ffi::director {
            magic: 0x3336351d,
            priv_: backend_ptr as *mut c_void,
            vcl_name: name_ptr,
            vdir: ptr::null_mut(),  // VCL director info - null for our use
            mtx: ptr::null_mut(),   // Lock - null since we handle locking in Rust
        });
        let director_ptr = Box::into_raw(director);

        VCL_BACKEND(director_ptr)
    }

    #[test]
    fn test_director_add_remove_backend() {
        let mut test_ctx = TestCtx::new(1);
        let ctx = &mut test_ctx.ctx();
        let director = director::new(ctx).unwrap();

        let addr1 = SocketAddr::from(([127, 0, 0, 1], 8080));
        let addr2 = SocketAddr::from(([127, 0, 0, 2], 8081));
        
        let backend = create_test_backend("test1", addr1);
        let backend2 = create_test_backend("test2", addr2);

        // Add backend and verify
        director.add_backend(backend).unwrap();
        assert_eq!(director.inner.backends.lock().unwrap().len(), 1);
        
        // Verify the backend name
        assert_eq!(director.inner.backends.lock().unwrap()[0].name, "test1");

        // Add another backend
        director.add_backend(backend2).unwrap();
        assert_eq!(director.inner.backends.lock().unwrap().len(), 2);

        // Remove a backend
        director.remove_backend(backend);
        assert_eq!(director.inner.backends.lock().unwrap().len(), 1);

        // Verify the remaining backend
        assert_eq!(director.inner.backends.lock().unwrap()[0].name, "test2");
        assert_eq!(director.inner.backends.lock().unwrap()[0].address, SocketAddr::from(([127, 0, 0, 2], 8081)));
    }

    // Update other tests to use create_test_backend
}