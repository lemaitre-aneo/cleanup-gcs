use std::sync::Arc;

use clap::Parser;
use google_cloud_gax::paginator::ItemPaginator;
use google_cloud_storage::{client::StorageControl, model::Object};
use google_cloud_wkt::FieldMask;
use indicatif::MultiProgress;
use indicatif_log_bridge::LogWrapper;
use tokio::sync::Semaphore;

mod args;
mod counter;
mod monitor;

pub use args::Configuration;
pub use counter::Counter;
pub use monitor::Monitor;

#[cfg(target_env = "musl")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Wait for termination signal (either SIGINT or SIGTERM)
#[cfg(unix)]
async fn wait_terminate() {
    use futures::{StreamExt, stream::FuturesUnordered};
    use tokio::signal::unix::{SignalKind, signal};
    let mut signals = Vec::new();

    // Register signal handlers
    for sig in [SignalKind::terminate(), SignalKind::interrupt()] {
        match signal(sig) {
            Ok(sig) => signals.push(sig),
            Err(err) => tracing::error!("Could not register signal handler: {err}"),
        }
    }

    // Wait for the first signal to trigger
    let mut signals = signals
        .iter_mut()
        .map(|sig| sig.recv())
        .collect::<FuturesUnordered<_>>();

    loop {
        match signals.next().await {
            // One of the signal triggered -> stop waiting
            Some(Some(())) => break,
            // One of the signal handler has been stopped -> continue waiting for the others
            Some(None) => (),
            // No more signal handlers are available, so wait indefinitely
            None => futures::future::pending::<()>().await,
        }
    }
}

#[cfg(windows)]
macro_rules! win_signal {
    ($($sig:ident),*$(,)?) => {
        $(
            let $sig = async {
                match tokio::signal::windows::$sig() {
                    Ok(mut $sig) => {
                        if $sig.recv().await.is_some() {
                            return;
                        }
                    }
                    Err(err) => tracing::error!(
                        "Could not register signal handler for {}: {err}",
                        stringify!($sig),
                    ),
                }
                futures::future::pending::<()>().await;
            };
        )*
        tokio::select! {
            $(
                _ = $sig => {}
            )*
        }
    }
}

/// Wait for termination signal (either SIGINT or SIGTERM)
#[cfg(windows)]
async fn wait_terminate() {
    win_signal!(ctrl_c, ctrl_close, ctrl_logoff, ctrl_shutdown);
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Configuration::parse();

    let multi = MultiProgress::new();
    let mut builder = pretty_env_logger::formatted_builder();

    if let Ok(s) = ::std::env::var("RUST_LOG") {
        builder.parse_filters(&s);
    } else {
        builder.parse_filters("info");
    }

    let logger = builder.build();
    let level = logger.filter();

    LogWrapper::new(multi.clone(), logger).try_init()?;
    log::set_max_level(level);

    let execution_context = Arc::new(ExecutionContext::new(args).await?);

    let mut listings = tokio::spawn(execution_context.clone().run());
    let monitoring = tokio::spawn(Monitor::new(execution_context.clone(), multi.clone())?);

    tokio::select! {
        listings = &mut listings => {
            match listings {
                Ok(Ok(())) => (),
                Ok(Err(err)) => log::error!("Listing error: {err:?}"),
                Err(err) => log::error!("Listing error: {err:?}"),
            }
        }
        _ = wait_terminate() => {
            tracing::error!("Application stopping");
            listings.abort();
            match listings.await {
                Ok(Ok(())) => (),
                Ok(Err(err)) => log::error!("Listing error: {err:?}"),
                Err(err) => {
                    if !err.is_cancelled() {
                        log::error!("Listing error: {err:?}");
                    }
                }
            }
        }
    }

    monitoring.abort();

    match monitoring.await {
        Ok(_) => (),
        Err(err) => {
            if !err.is_cancelled() {
                log::error!("Monitor error: {err:?}");
            }
        }
    }

    log::info!("Done");

    Ok(())
}

pub struct ExecutionContext {
    /// Gcs client
    client: StorageControl,
    /// Full name of the bucket
    bucket: String,
    /// Configuration
    pub(crate) config: Configuration,
    /// Number of objects that have been listed
    pub objects: Counter,
    /// Number of live objects that have been listed
    pub live_objects: Counter,
    /// Number of objects that have been deleted
    pub deleted_objects: Counter,
    /// Number of objects whose deletion had an error
    pub error_objects: Counter,
    /// Cumulative number of bytes for all the listed objects
    pub cum_bytes: Counter,
    /// Cumulative number of bytes for all the listed and live objects
    pub cum_live_bytes: Counter,
    /// Semaphore to limit the number of parallel deletion
    pub delete_semaphore: Arc<Semaphore>,
}

impl ExecutionContext {
    pub async fn new(conf: Configuration) -> anyhow::Result<Self> {
        let mut storage_control_builder = StorageControl::builder();
        if let Some(gcp_endpoint) = &conf.gcp_endpoint {
            storage_control_builder = storage_control_builder.with_endpoint(gcp_endpoint);
        }

        let storage_control = storage_control_builder.build().await?;

        Ok(Self {
            client: storage_control,
            bucket: format!("projects/{}/buckets/{}", conf.project, conf.bucket),
            objects: Default::default(),
            live_objects: Default::default(),
            deleted_objects: Default::default(),
            error_objects: Default::default(),
            cum_bytes: Default::default(),
            cum_live_bytes: Default::default(),
            delete_semaphore: Arc::new(Semaphore::const_new(conf.parallelism)),
            config: conf,
        })
    }

    pub async fn run(self: Arc<Self>) -> anyhow::Result<()> {
        let mut listing = self
            .client
            .list_objects()
            .set_parent(&self.bucket)
            .set_versions(true);
        if !self.config.fetch_all_metadata {
            listing = listing.set_read_mask(FieldMask::default().set_paths([
                "name",
                "size",
                "generation",
                "delete_time",
            ]))
        }
        if let Some(prefix) = &self.config.prefix {
            listing = listing.set_prefix(prefix);
        }
        if let Some(start) = &self.config.start {
            listing = listing.set_lexicographic_start(start);
        }
        if let Some(end) = &self.config.end {
            listing = listing.set_lexicographic_end(end);
        }
        let mut listing = listing.by_item();

        let mut joiner = tokio::task::JoinSet::new();

        let mut last_object = scopeguard::guard(Object::new(), |object| {
            log::error!(
                "Listing stopped after object `{}`#{}",
                object.name,
                object.generation
            );
        });

        while let Some(object) = listing.next().await {
            match object {
                Ok(object) => {
                    *last_object = object.clone();
                    if let Err(err) = self.process_object(object, &mut joiner).await {
                        log::error!(
                            "Delete failed for object `{}`#{}: {err:?}",
                            last_object.name,
                            last_object.generation
                        );
                    }
                }
                Err(err) => {
                    log::error!(
                        "Listing failed after object `{}`#{}: {err:?}",
                        last_object.name,
                        last_object.generation
                    );
                }
            }

            while let Some(tasks) = joiner.try_join_next() {
                if let Err(err) = tasks {
                    log::error!("Could not join delete task: {err:?}");
                }
            }
        }

        scopeguard::ScopeGuard::into_inner(last_object);

        log::debug!("Listing finished",);

        while let Some(tasks) = joiner.join_next().await {
            if let Err(err) = tasks {
                log::error!("Could not join delete task: {err:?}");
            }
        }
        Ok(())
    }

    async fn process_object(
        self: &Arc<Self>,
        object: Object,
        joiner: &mut tokio::task::JoinSet<()>,
    ) -> anyhow::Result<()> {
        self.objects.inc();
        self.cum_bytes.add(object.size as usize);

        log::trace!("Process {object:?}");

        if object.delete_time.is_some() {
            let object = scopeguard::guard(object, |object| {
                log::error!(
                    "Deletion of object `{}`#{} has been cancelled",
                    object.name,
                    object.generation
                );
            });

            log::debug!("Must delete `{}`#{}", object.name, object.generation);
            let ctx = Arc::clone(self);
            let sem_guard = self.delete_semaphore.clone().acquire_owned().await?;
            joiner.spawn(async move {
                let _sem_guard = sem_guard;

                if !ctx.config.dry_run
                    && let Err(err) = ctx
                        .client
                        .delete_object()
                        .set_bucket(&ctx.bucket)
                        .set_object(&object.name)
                        .set_generation(object.generation)
                        .send()
                        .await
                {
                    let object = scopeguard::ScopeGuard::into_inner(object);
                    log::error!(
                        "Failed to delete `{}`#{}: {err:?}",
                        object.name,
                        object.generation
                    );
                    ctx.error_objects.inc();
                    return;
                }

                let object = scopeguard::ScopeGuard::into_inner(object);

                log::info!(
                    "Deleted `{}`#{}{}",
                    object.name,
                    object.generation,
                    ctx.dry_run_suffix()
                );

                ctx.deleted_objects.inc();
            });
        } else {
            log::debug!("Keep `{}`#{}", object.name, object.generation);
            self.live_objects.inc();
            self.cum_bytes.add(object.size as usize);
        }

        Ok(())
    }

    fn dry_run_suffix(&self) -> &str {
        if self.config.dry_run {
            " [DRY RUN]"
        } else {
            ""
        }
    }
}
