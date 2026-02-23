use std::sync::Arc;

use clap::Parser;
use google_cloud_gax::{options::RequestOptions, paginator::Paginator};
use google_cloud_storage::{client::StorageControl, model::Object};
use google_cloud_wkt::{FieldMask, Timestamp};
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

    let (tx, rx) = tokio::sync::mpsc::channel(args.deletes_buffer);
    let execution_context = Arc::new(ExecutionContext::new(args).await?);

    let listings = tokio::spawn(execution_context.clone().run_listing(tx));
    let deletes = tokio::spawn(execution_context.clone().run_delete(rx));
    let monitoring = tokio::spawn(Monitor::new(execution_context.clone(), multi.clone())?);

    let listings_abort = listings.abort_handle();
    let deletes_abort = deletes.abort_handle();

    let mut run = std::pin::pin!(async { tokio::join!(listings, deletes) });

    tokio::select! {
        (listings, deletes) = &mut run => {
            match listings {
                Ok(Ok(())) => (),
                Ok(Err(err)) => log::error!("Listing error: {err}"),
                Err(err) => log::error!("Listing error: {err}"),
            }
            match deletes {
                Ok(Ok(())) => (),
                Ok(Err(err)) => log::error!("Delete error: {err}"),
                Err(err) => log::error!("Delete error: {err}"),
            }
        }
        _ = wait_terminate() => {
            tracing::error!("Application stopping");
            listings_abort.abort();
            deletes_abort.abort();
            let (listings, deletes) = run.await;
            match listings {
                Ok(Ok(())) => (),
                Ok(Err(err)) => log::error!("Listing error: {err}"),
                Err(err) => {
                    if !err.is_cancelled() {
                        log::error!("Listing error: {err}");
                    }
                }
            }
            match deletes {
                Ok(Ok(())) => (),
                Ok(Err(err)) => log::error!("Delete error: {err}"),
                Err(err) => {
                    if !err.is_cancelled() {
                        log::error!("Delete error: {err}");
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
                log::error!("Monitor error: {err}");
            }
        }
    }

    log::info!("Done");

    Ok(())
}

struct ListingRequest {
    prefix: Option<String>,
    depth: usize,
    listings_tx: tokio::sync::mpsc::Sender<ListingRequest>,
}

#[derive(Debug, Clone, Default)]
pub struct DeleteRequest {
    name: String,
    generation: i64,
    delete_time: Option<Timestamp>,
}

impl From<Object> for DeleteRequest {
    fn from(object: Object) -> Self {
        Self {
            name: object.name,
            generation: object.generation,
            delete_time: object.delete_time,
        }
    }
}

pub struct ExecutionContext {
    /// Gcs client
    client: StorageControl,
    /// Full name of the bucket
    bucket: String,
    /// Configuration
    pub(crate) config: Configuration,
    /// Number of listings that have finished
    pub listings: Counter,
    /// Total number of listings that have been planned
    pub total_listings: Counter,
    /// Number of listings that have failed
    pub error_listings: Counter,
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
    /// Semaphore to limit the number of parallel listing
    pub listing_semaphore: Arc<Semaphore>,
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
            listings: Default::default(),
            total_listings: Default::default(),
            error_listings: Default::default(),
            objects: Default::default(),
            live_objects: Default::default(),
            deleted_objects: Default::default(),
            error_objects: Default::default(),
            cum_bytes: Default::default(),
            cum_live_bytes: Default::default(),
            listing_semaphore: Arc::new(Semaphore::const_new(conf.listings_parallelism)),
            delete_semaphore: Arc::new(Semaphore::const_new(conf.deletes_parallelism)),
            config: conf,
        })
    }

    pub async fn run_listing(
        self: Arc<Self>,
        objects_tx: tokio::sync::mpsc::Sender<DeleteRequest>,
    ) -> anyhow::Result<()> {
        let (listings_tx, mut listings_rx) =
            tokio::sync::mpsc::channel(self.config.listings_buffer);
        self.total_listings.inc();
        listings_tx.try_send(ListingRequest {
            prefix: self.config.prefix.clone(),
            depth: usize::try_from(self.config.tree_depth).unwrap_or(usize::MAX),
            listings_tx: listings_tx.clone(),
        })?;

        std::mem::drop(listings_tx);

        let mut joiner = tokio::task::JoinSet::new();
        while let Some(request) = listings_rx.recv().await {
            let permit = self.listing_semaphore.clone().acquire_owned().await?;
            joiner.spawn(Arc::clone(&self).list(request, objects_tx.clone(), permit));

            while let Some(tasks) = joiner.try_join_next() {
                if let Err(err) = tasks {
                    log::error!("Could not join listing task: {err}");
                    self.error_listings.inc();
                }
            }
        }

        while let Some(tasks) = joiner.join_next().await {
            if let Err(err) = tasks {
                log::error!("Could not join listing task: {err}");
                self.error_listings.inc();
            }
        }

        Ok(())
    }

    async fn list(
        self: Arc<Self>,
        listing_request: ListingRequest,
        objects_tx: tokio::sync::mpsc::Sender<DeleteRequest>,
        _permit: tokio::sync::OwnedSemaphorePermit,
    ) {
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
        if let Some(prefix) = &listing_request.prefix {
            listing = listing.set_prefix(prefix);
        }
        if listing_request.depth > 0 {
            listing = listing
                .set_delimiter("/")
                .set_include_folders_as_prefixes(false);
        }
        if let Some(start) = &self.config.start {
            listing = listing.set_lexicographic_start(start);
        }
        if let Some(end) = &self.config.end {
            listing = listing.set_lexicographic_end(end);
        }

        let mut listing = listing.by_page();
        let mut last_object = scopeguard::guard(DeleteRequest::default(), |object| {
            log::warn!(
                "Listing for prefix `{}` cancelled. Last object fetched `{}`#{}",
                listing_request.prefix.as_deref().unwrap_or(""),
                object.name,
                object.generation
            );
        });

        while let Some(page) = listing.next().await {
            log::trace!(
                "Received listing page for prefix `{}`: {page:?}",
                listing_request.prefix.as_deref().unwrap_or(""),
            );
            match page {
                Ok(page) => {
                    if let Some(last) = page.objects.last() {
                        *last_object = last.clone().into();
                    } else if let Some(last) = page.prefixes.last() {
                        last_object.name = last.clone();
                        last_object.generation = 0;
                    }
                    for prefix in page.prefixes {
                        self.total_listings.inc();
                        listing_request
                            .listings_tx
                            .send(ListingRequest {
                                prefix: Some(prefix),
                                depth: listing_request.depth.saturating_sub(1),
                                listings_tx: listing_request.listings_tx.clone(),
                            })
                            .await
                            .unwrap();
                    }
                    for object in page.objects {
                        self.objects.inc();
                        self.cum_bytes.add(object.size as usize);

                        if object.delete_time.is_some() {
                            log::debug!("Non-current `{}`#{}", object.name, object.generation);

                            objects_tx.send(object.into()).await.unwrap();
                        } else {
                            log::debug!("Live object `{}`#{}", object.name, object.generation);
                            self.live_objects.inc();
                            self.cum_live_bytes.add(object.size as usize);
                        }
                    }
                }
                Err(err) => {
                    log::error!(
                        "Listing with prefix `{}` failed after object `{}`#{}: {err}",
                        listing_request.prefix.as_deref().unwrap_or(""),
                        last_object.name,
                        last_object.generation
                    );
                    self.error_listings.inc();
                }
            }
        }

        scopeguard::ScopeGuard::into_inner(last_object);
        self.listings.inc();
    }

    pub async fn run_delete(
        self: Arc<Self>,
        mut rx: tokio::sync::mpsc::Receiver<DeleteRequest>,
    ) -> anyhow::Result<()> {
        let mut joiner = tokio::task::JoinSet::new();

        while let Some(object) = rx.recv().await {
            let permit = self.delete_semaphore.clone().acquire_owned().await?;
            joiner.spawn(Arc::clone(&self).delete_object(object, permit));

            while let Some(tasks) = joiner.try_join_next() {
                if let Err(err) = tasks {
                    log::error!("Could not join delete task: {err}");
                    self.error_objects.inc();
                }
            }
        }

        while let Some(tasks) = joiner.join_next().await {
            if let Err(err) = tasks {
                log::error!("Could not join delete task: {err}");
                self.error_objects.inc();
            }
        }

        log::debug!("Delete finished",);

        Ok(())
    }

    async fn delete_object(
        self: Arc<Self>,
        object: DeleteRequest,
        _permit: tokio::sync::OwnedSemaphorePermit,
    ) {
        if object.delete_time.is_none() {
            log::error!(
                "ASSERT FAILED: Object `{}`#{} is not a non-current version, skipping deletion",
                object.name,
                object.generation
            );
            self.error_objects.inc();
            return;
        }
        log::debug!(
            "Deleting `{}`#{}{}",
            object.name,
            object.generation,
            self.dry_run_suffix()
        );

        let object = scopeguard::guard(object, |object| {
            log::warn!(
                "Deletion of object `{}`#{} has been cancelled",
                object.name,
                object.generation
            );
        });

        let mut request_options = RequestOptions::default();
        request_options.set_idempotency(true);

        if self.config.dry_run {
            match self
                .client
                .test_iam_permissions()
                .set_resource(&self.bucket)
                .set_permissions(["storage.objects.delete"])
                .send()
                .await
            {
                Ok(perms) => {
                    if perms.permissions.is_empty() {
                        let object = scopeguard::ScopeGuard::into_inner(object);
                        log::error!(
                            "Failed to delete `{}`#{}{}: Not authorized",
                            object.name,
                            object.generation,
                            self.dry_run_suffix(),
                        );
                        self.error_objects.inc();
                        return;
                    }
                }
                Err(err) => {
                    let object = scopeguard::ScopeGuard::into_inner(object);
                    log::error!(
                        "Failed to delete `{}`#{}{}: {err}",
                        object.name,
                        object.generation,
                        self.dry_run_suffix(),
                    );
                    self.error_objects.inc();
                    return;
                }
            }
        } else if let Err(err) = self
            .client
            .delete_object()
            .set_bucket(&self.bucket)
            .set_object(&object.name)
            .set_generation(object.generation)
            .with_options(request_options)
            .send()
            .await
        {
            let object = scopeguard::ScopeGuard::into_inner(object);
            log::error!(
                "Failed to delete `{}`#{}: {err}",
                object.name,
                object.generation
            );
            self.error_objects.inc();
            return;
        }

        let object = scopeguard::ScopeGuard::into_inner(object);

        log::info!(
            "Deleted `{}`#{}{}",
            object.name,
            object.generation,
            self.dry_run_suffix()
        );

        self.deleted_objects.inc();
    }

    fn dry_run_suffix(&self) -> &str {
        if self.config.dry_run {
            " [DRY RUN]"
        } else {
            ""
        }
    }
}
