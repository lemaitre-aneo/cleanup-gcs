use crate::{
    args::Configuration,
    atomic_counter::{AtomicCounter, SimpleAtomic},
};
use futures::TryStreamExt;
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};
use google_cloud_gax::error::rpc::Code;
use google_cloud_storage::{
    builder_ext::RewriteObjectExt,
    client::{Storage, StorageControl},
    streaming_source::StreamingSource,
};
use google_cloud_wkt::FieldMask;

pub struct ExecutionContext {
    storage: Storage,
    storage_control: StorageControl,
    pub(crate) config: Configuration,
    pub listing_progress: AtomicCounter,
    pub transfer_count_progress: AtomicCounter,
    pub transfer_bytes_progress: AtomicCounter,
    pub instant_bytes: AtomicCounter,
    pub transfer_semaphore: Arc<Semaphore>,
    pub listing_semaphore: Arc<Semaphore>,
}

impl
    ExecutionContext
{
    pub async fn new(conf: Configuration) -> anyhow::Result<Self> {
        let mut storage_builder = Storage::builder();
        let mut storage_control_builder = StorageControl::builder();
        if let Some(gcp_endpoint) = &conf.gcp_endpoint {
            storage_builder = storage_builder.with_endpoint(gcp_endpoint);
            storage_control_builder = storage_control_builder.with_endpoint(gcp_endpoint);
        }

        let storage = storage_builder.build().await?;
        let storage_control = storage_control_builder.build().await?;

        Ok(Self {
            storage,
            storage_control,
            listing_progress: Default::default(),
            transfer_count_progress: Default::default(),
            transfer_bytes_progress: Default::default(),
            instant_bytes: Default::default(),
            transfer_semaphore: Arc::new(Semaphore::const_new(10)),
            listing_semaphore: Arc::new(Semaphore::const_new(10)),
            config: conf,
        })
    }

    pub async fn run(self:Arc<Self>) -> anyhow::Result<()> {
        Ok(())
    }

}
