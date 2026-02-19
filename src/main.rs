use std::{env, sync::Arc};

use clap::Parser;
use indicatif::MultiProgress;
use indicatif_log_bridge::LogWrapper;

mod args;
mod atomic_counter;
mod execution_context;
mod monitor;

pub use args::Configuration;
pub use execution_context::ExecutionContext;
pub use monitor::Monitor;

#[cfg(target_env = "musl")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Configuration::parse();

    let multi = MultiProgress::new();
    let mut builder = pretty_env_logger::formatted_builder();

    if let Ok(s) = ::std::env::var("RUST_LOG") {
        builder.parse_filters(&s);
    }

    let logger = builder.build();
    let level = logger.filter();

    LogWrapper::new(multi.clone(), logger).try_init()?;
    log::set_max_level(level);

    let execution_context = Arc::new(ExecutionContext::new(args).await?);

    let listings = tokio::spawn(execution_context.clone().run());
    let monitoring = tokio::spawn(Monitor::new(execution_context.clone(), multi.clone())?);

    match listings.await {
        Ok(Ok(())) => (),
        Ok(Err(err)) => log::error!("Listing error: {err:?}"),
        Err(err) => log::error!("Listing error: {err:?}"),
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
