mod backend;
mod probe;

#[path = "director.rs"]
mod prequal_director;

use std::sync::Arc;
use std::thread;

use prequal_director::Director;
use backend::Backend;

use varnish::ffi::VCL_BACKEND;
use varnish::vcl::{Ctx, VclError, LogTag};

#[allow(non_camel_case_types)]
pub struct director {
    inner: Arc<Director>,
}

#[varnish::vmod]
mod prequal {
    use super::*;

    impl director {
        /// Creates a new director instance.
        /// 
        /// This spawns a background thread that periodically probes backends
        /// to determine their health and load status.
        pub fn new(_ctx: &mut Ctx) -> Result<Self, VclError> {
            let (inner, probe_loop) = Director::new();
            thread::spawn(probe_loop);
            Ok(Self { inner })
        }

        /// Sets the HTTP path used for health check probes.
        /// 
        /// # Arguments
        /// * `path` - The URL path to use for probe requests (e.g. "/probe")
        pub fn set_probe_path(&self, path: &str) {
            self.inner.set_probe_path(path);
        }

        /// Adds a backend to the director's pool.
        /// 
        /// # Arguments
        /// * `vcl_backend` - The VCL backend to add
        /// 
        /// # Returns
        /// * `Ok(())` if the backend was added successfully
        /// * `Err(VclError)` if the backend was invalid or could not be added
        pub fn add_backend(&self, vcl_backend: VCL_BACKEND) -> Result<(), VclError> {
            match Backend::new(vcl_backend) {
                Ok(backend) => {
                    self.inner.add_backend(backend).map_err(|e| VclError::new(format!("Failed to add backend: {:?}", e)))
                }
                Err(e) => {
                    return Err(VclError::new(format!("Invalid backend: {:?}", e)));
                }
            }
        }

        /// Removes a backend from the pool.
        /// 
        /// # Arguments
        /// * `backend` - The VCL backend to remove
        pub fn remove_backend(&self, backend: VCL_BACKEND) {
            self.inner.remove_backend(backend)
        }

        /// Selects the best backend for the current request.
        /// 
        /// The selection is based on probe results (in_flight requests and latency).
        /// Falls back to random selection if no probe results are available.
        /// 
        /// # Safety
        /// This function is marked unsafe because it returns a raw VCL_BACKEND pointer.
        pub unsafe fn backend(&self, ctx: &mut Ctx) -> Result<VCL_BACKEND, VclError> {
            self.log_probes(ctx); // just for now, for debugging

            let backend = self.inner.get_backend()?;
            Ok(backend.vcl_backend)
        }

        /// Checks if the director has any valid probe results.
        /// 
        /// # Returns
        /// `true` if there are valid probe results, `false` otherwise
        pub fn healthy(&self) -> bool {
            self.inner.is_healthy()
        }

        /// Logs the current state of the probe table for debugging.
        /// 
        /// # Arguments
        /// * `ctx` - The VCL context for logging
        pub fn log_probes(&self, ctx: &mut Ctx) {
            if let Some(table) = self.inner.debug_probe_table() {
                ctx.log(LogTag::Debug, &format!("Probe table state:{}", table));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    varnish::run_vtc_tests!("tests/*.vtc");
}