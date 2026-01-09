mod backend;
mod probe;

#[path = "director.rs"]
mod prequal_director;

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;

pub use backend::Backend;
pub use prequal_director::{Director, DirectorStats};
use varnish::ffi::VCL_BACKEND;
use varnish::vcl::{Ctx, LogTag, VclError};
use varnish::Vsc;

// director is a very thin wrapper around a Director, to expose it to VCL
#[allow(non_camel_case_types)]
pub struct director {
    inner: Arc<Director>,
    // Vsc exposes stats to varnishstat; we sync from Director's Arc<DirectorStats>
    vsc: Vsc<DirectorStats>,
}

impl director {
    /// Syncs stats from Director's Arc<DirectorStats> to Vsc for varnishstat visibility.
    /// Called on each request to keep varnishstat reasonably up-to-date.
    fn sync_stats(&self) {
        let src = self.inner.stats();

        // Sync counters
        self.vsc
            .req
            .store(src.req.load(Ordering::Relaxed), Ordering::Relaxed);
        self.vsc.selected_from_table.store(
            src.selected_from_table.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.vsc.fallback_random.store(
            src.fallback_random.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.vsc
            .probes_sent
            .store(src.probes_sent.load(Ordering::Relaxed), Ordering::Relaxed);
        self.vsc.probes_success.store(
            src.probes_success.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.vsc
            .probes_fail
            .store(src.probes_fail.load(Ordering::Relaxed), Ordering::Relaxed);
        self.vsc.probes_missing_headers.store(
            src.probes_missing_headers.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );

        // Sync gauges (computed in probe loop)
        self.vsc
            .backends
            .store(src.backends.load(Ordering::Relaxed), Ordering::Relaxed);
        self.vsc.probe_table_size.store(
            src.probe_table_size.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.vsc
            .probe_p50_rif
            .store(src.probe_p50_rif.load(Ordering::Relaxed), Ordering::Relaxed);
        self.vsc
            .probe_p80_rif
            .store(src.probe_p80_rif.load(Ordering::Relaxed), Ordering::Relaxed);
        self.vsc.probe_p50_latency.store(
            src.probe_p50_latency.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.vsc.probe_p80_latency.store(
            src.probe_p80_latency.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.vsc
            .probe_min_rif
            .store(src.probe_min_rif.load(Ordering::Relaxed), Ordering::Relaxed);
        self.vsc
            .probe_max_rif
            .store(src.probe_max_rif.load(Ordering::Relaxed), Ordering::Relaxed);
    }
}

#[varnish::vmod(docs = "README.md")]
mod prequal {
    use super::*;

    impl director {
        /// Creates a new director instance.
        ///
        /// # Arguments
        /// * `name` - A name for this director instance (used in stats naming)
        ///
        /// This spawns a background thread that periodically probes backends
        /// to determine their health and load status.
        pub fn new(_ctx: &mut Ctx, name: &str) -> Result<Self, VclError> {
            let stats = Arc::new(DirectorStats::default());
            let vsc = Vsc::<DirectorStats>::new("prequal", name);
            let (inner, probe_loop) = Director::new(stats);
            thread::spawn(probe_loop);
            Ok(Self { inner, vsc })
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
                Ok(backend) => self
                    .inner
                    .add_backend(backend)
                    .map_err(|e| VclError::new(format!("Failed to add backend: {:?}", e))),
                Err(e) => Err(VclError::new(format!("Invalid backend: {:?}", e))),
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

            let stats = self.inner.stats();

            // Increment request counter
            stats.req.fetch_add(1, Ordering::Relaxed);

            let (backend, from_table) = self
                .inner
                .get_backend()
                .map_err(|e| VclError::new(format!("Failed to get backend: {:?}", e)))?;

            // Track selection source
            if from_table {
                stats.selected_from_table.fetch_add(1, Ordering::Relaxed);
            } else {
                stats.fallback_random.fetch_add(1, Ordering::Relaxed);
            }

            // Sync to Vsc for varnishstat visibility
            self.sync_stats();

            Ok(backend.vcl_backend)
        }

        /// Checks if the director has any valid probe results.
        ///
        /// # Returns
        /// `true` if there are valid probe results, `false` otherwise
        pub fn healthy(&self) -> bool {
            self.inner.is_healthy()
        }

        /// Triggers a probe fetch for every backend.
        pub fn seed_probes(&self) {
            self.inner.trigger_probe();
        }

        /// Logs the current state of the probe table for debugging.
        ///
        /// # Arguments
        /// * `ctx` - The VCL context for logging
        pub fn log_probes(&self, ctx: &mut Ctx) {
            if let Some(table) = self.inner.debug_probe_table() {
                ctx.log(LogTag::Debug, format!("Probe table state:{}", table));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    varnish::run_vtc_tests!("tests/*.vtc");
}
