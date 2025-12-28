use std::ffi::CStr;
use std::fmt;
use std::net::SocketAddr;

use varnish::ffi::{backend, BACKEND_MAGIC, DIRECTOR_MAGIC, VCL_BACKEND};

#[derive(Debug, Clone)]
pub struct Backend {
    pub(crate) name: String,
    pub(crate) address: SocketAddr,
    pub(crate) vcl_backend: VCL_BACKEND,
}

impl PartialEq for Backend {
    fn eq(&self, other: &Self) -> bool {
        self.vcl_backend.0 == other.vcl_backend.0
    }
}

impl Eq for Backend {}

impl PartialEq<VCL_BACKEND> for Backend {
    fn eq(&self, other: &VCL_BACKEND) -> bool {
        self.vcl_backend.0 == other.0
    }
}

#[derive(Debug)]
pub enum BackendError {
    BackendMagic,
    DirectorMagic,
    Address,
}

impl std::fmt::Display for BackendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackendError::BackendMagic => write!(f, "Invalid backend magic number"),
            BackendError::DirectorMagic => write!(f, "Invalid director magic number"),
            BackendError::Address => write!(f, "Invalid or missing backend address"),
        }
    }
}

impl std::error::Error for BackendError {}

impl Backend {
    pub fn new(backend_director: VCL_BACKEND) -> Result<Self, BackendError> {
        unsafe {
            // Validate director first
            let director = backend_director
                .0
                .as_ref()
                .ok_or(BackendError::DirectorMagic)?;
            if director.magic != DIRECTOR_MAGIC {
                return Err(BackendError::DirectorMagic);
            }

            // Then validate backend
            let backend = (director.priv_ as *const backend)
                .as_ref()
                .ok_or(BackendError::BackendMagic)?;
            if backend.magic != BACKEND_MAGIC {
                return Err(BackendError::BackendMagic);
            }

            Ok(Self {
                name: Self::name_from_backend(backend),
                address: Self::address_from_backend(backend)?,
                vcl_backend: backend_director,
            })
        }
    }

    fn name_from_backend(backend: &backend) -> String {
        unsafe {
            if !backend.vcl_name.is_null() {
                CStr::from_ptr(backend.vcl_name)
                    .to_str()
                    .map(String::from)
                    .unwrap_or_else(|_| Self::generate_random_name())
            } else {
                Self::generate_random_name()
            }
        }
    }

    // We should always have a valid name, but if we don't, generate a random one
    fn generate_random_name() -> String {
        format!("backend_{}", rand::random::<u32>())
    }

    #[cfg(not(test))]
    fn address_from_backend(backend: &backend) -> Result<SocketAddr, BackendError> {
        unsafe {
            let endpoint = (*backend.endpoint).ipv4;
            Option::<SocketAddr>::from(endpoint).ok_or(BackendError::Address)
        }
    }

    /// Test-only implementation that parses VCL_IP without calling VSA_GetPtr/VSA_Port,
    /// which aren't exported from libvarnishapi on Linux.
    #[cfg(test)]
    fn address_from_backend(backend: &backend) -> Result<SocketAddr, BackendError> {
        use std::net::{IpAddr, Ipv4Addr};

        const VSA_MAGIC: u32 = 0x4b1e9335;

        unsafe {
            let vcl_ip = (*backend.endpoint).ipv4;
            if vcl_ip.0.is_null() {
                return Err(BackendError::Address);
            }

            let ptr = vcl_ip.0 as *const u8;
            let magic = *(ptr as *const u32);
            if magic != VSA_MAGIC {
                return Err(BackendError::Address);
            }

            // Layout matches create_test_vcl_ip: magic(4) + len(1) + family(1) + port(2) + addr(4)
            let data = ptr.add(4);
            let family = *data.add(1);

            if family == 2 {
                // AF_INET
                let port = ((*data.add(2) as u16) << 8) | (*data.add(3) as u16);
                let ip = Ipv4Addr::new(*data.add(4), *data.add(5), *data.add(6), *data.add(7));
                Ok(SocketAddr::new(IpAddr::V4(ip), port))
            } else {
                Err(BackendError::Address)
            }
        }
    }
}

impl fmt::Display for Backend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

unsafe impl Send for Backend {}
unsafe impl Sync for Backend {}

#[cfg(test)]
mod tests {
    use std::ffi::{c_void, CString};
    use std::net::SocketAddr;
    use std::ptr;

    use varnish::ffi::{backend, suckaddr, vrt_endpoint, VCL_IP, VRT_ENDPOINT_MAGIC};

    use super::*;

    // Normally we could use varnish helper functions to create a suckaddr,
    // but we can't link against varnishd.
    fn create_test_vcl_ip(addr: SocketAddr) -> VCL_IP {
        const SUCKADDR_SIZE: usize = 128;
        const VSA_MAGIC: u32 = 0x4b1e9335;

        #[repr(C)]
        struct TestSuckaddr {
            magic: u32,
            data: [u8; SUCKADDR_SIZE - size_of::<u32>()],
        }

        let mut test_addr = Box::new(TestSuckaddr {
            magic: VSA_MAGIC,
            data: [0; SUCKADDR_SIZE - size_of::<u32>()],
        });

        unsafe {
            let bytes = test_addr.data.as_mut_ptr();
            match addr {
                SocketAddr::V4(addr4) => {
                    *bytes.add(0) = 4; // length of address
                    *bytes.add(1) = 2; // AF_INET
                    let port = addr4.port();
                    *bytes.add(2) = ((port & 0xFF00) >> 8) as u8; // High byte of port
                    *bytes.add(3) = (port & 0xFF) as u8; // Low byte of port
                    let octets = addr4.ip().octets();
                    *bytes.add(4) = octets[0];
                    *bytes.add(5) = octets[1];
                    *bytes.add(6) = octets[2];
                    *bytes.add(7) = octets[3];
                }
                SocketAddr::V6(_) => todo!("IPv6 support"),
            }
        }

        let suckaddr: Box<suckaddr> = unsafe { std::mem::transmute(test_addr) };

        VCL_IP(Box::into_raw(suckaddr) as *const _)
    }

    fn create_test_vrt_endpoint(addr: SocketAddr) -> *mut vrt_endpoint {
        let vcl_ip = create_test_vcl_ip(addr);

        let endpoint = Box::new(vrt_endpoint {
            magic: VRT_ENDPOINT_MAGIC,
            ipv4: vcl_ip,
            ipv6: VCL_IP(ptr::null()),
            uds_path: ptr::null(),
            preamble: ptr::null(),
        });

        Box::into_raw(endpoint)
    }

    fn create_test_backend(name: &str, addr: SocketAddr) -> VCL_BACKEND {
        // Allocate and leak the strings
        let name_cstr = CString::new(name).unwrap();
        let name_ptr = name_cstr.into_raw();

        let endpoint = create_test_vrt_endpoint(addr);

        // Create the backend structure
        let backend = Box::new(backend {
            magic: BACKEND_MAGIC,
            n_conn: 0,
            endpoint,
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
            cw_head: unsafe { std::mem::zeroed() },
            cw_count: 0,
        });
        let backend_ptr = Box::into_raw(backend);

        // Create the director structure
        let director = Box::new(varnish::ffi::director {
            magic: DIRECTOR_MAGIC,
            priv_: backend_ptr as *mut c_void,
            vcl_name: name_ptr,
            vdir: ptr::null_mut(),
            mtx: ptr::null_mut(),
        });
        let director_ptr = Box::into_raw(director);

        VCL_BACKEND(director_ptr)
    }

    #[test]
    fn test_backend_parsing() {
        let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
        let backend = create_test_backend("test1", addr);

        let parsed = Backend::new(backend).unwrap();
        assert_eq!(parsed.name, "test1");
        assert_eq!(parsed.address, addr);
    }

    #[test]
    fn test_backend_parsing_invalid_backend() {
        let name_cstr = CString::new("test1").unwrap();
        let name_ptr = name_cstr.into_raw();

        let director = Box::new(varnish::ffi::director {
            magic: DIRECTOR_MAGIC,
            priv_: name_ptr as *mut c_void,
            vcl_name: name_ptr, // this is wrong
            vdir: ptr::null_mut(),
            mtx: ptr::null_mut(),
        });
        let director_ptr = Box::into_raw(director);
        let backend = VCL_BACKEND(director_ptr);

        let result = Backend::new(backend);

        assert!(matches!(result, Err(BackendError::BackendMagic)));
    }

    #[test]
    fn test_backend_parsing_invalid_director() {
        let name_cstr = CString::new("test1").unwrap();
        let name_ptr = name_cstr.into_raw();

        let director = Box::new(varnish::ffi::director {
            magic: 0,
            priv_: ptr::null_mut(),
            vcl_name: name_ptr, // this is wrong
            vdir: ptr::null_mut(),
            mtx: ptr::null_mut(),
        });
        let director_ptr = Box::into_raw(director);
        let backend = VCL_BACKEND(director_ptr);

        let result = Backend::new(backend);

        assert!(matches!(result, Err(BackendError::DirectorMagic)));
    }
}
