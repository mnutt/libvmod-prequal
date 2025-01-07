mod backend;
mod probe;

#[path = "director.rs"]
mod prequal_director;

use std::sync::Arc;
use std::thread;

use prequal_director::Director;
use backend::Backend;

use varnish::ffi::VCL_BACKEND;
use varnish::vcl::{Ctx, VclError};

#[allow(non_camel_case_types)]
pub struct director {
    inner: Arc<Director>,
}

#[varnish::vmod]
mod prequal {
    use super::*;

    impl director {
        pub fn new(_ctx: &mut Ctx) -> Result<Self, VclError> {
            let (inner, probe_loop) = Director::new();
            thread::spawn(probe_loop);
            Ok(Self { inner })
        }

        pub fn set_probe_path(&self, path: &str) {
            self.inner.set_probe_path(path);
        }

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

        pub fn remove_backend(&self, backend: VCL_BACKEND) {
            self.inner.remove_backend(backend)
        }

        pub unsafe fn backend(&self, ctx: &mut Ctx) -> Result<VCL_BACKEND, VclError> {
            self.inner.get_backend(ctx)
        }

        pub fn healthy(&self, _ctx: &mut Ctx) -> bool {
            self.inner.is_healthy()
        }
    }
}

#[cfg(test)]
mod tests {
    varnish::run_vtc_tests!("tests/*.vtc");
}