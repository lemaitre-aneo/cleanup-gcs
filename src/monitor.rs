pub use crate::ExecutionContext;
use indicatif::{MultiProgress, ProgressBar};
use std::sync::Arc;

pub struct Monitor {
    context: Arc<ExecutionContext>,
    multi: MultiProgress,
    listing_progress: ProgressBar,
    transfer_count_progress: ProgressBar,
    transfer_bytes_progress: ProgressBar,
    instant_bytes: ProgressBar,
    interval: tokio::time::Interval,
}

impl Monitor {
    pub fn new(context: Arc<ExecutionContext>, multi: MultiProgress) -> anyhow::Result<Self> {
        let listing_progress = multi
            .add(ProgressBar::new(0))
            .with_finish(indicatif::ProgressFinish::AndLeave)
            .with_style(
                indicatif::ProgressStyle::with_template(
                "{prefix} {bar} {pos}/{human_len} [{per_sec}] {elapsed}/{duration} ETA:{eta} Errors {errors}",
            )?.with_key("errors", CounterProgressTracker::new(context.clone(), |c| c.error_objects.get())));

        let transfer_count_progress = multi
            .add(ProgressBar::new(0))
            .with_finish(indicatif::ProgressFinish::AndLeave)
            .with_style(
                indicatif::ProgressStyle::with_template(
                    "{prefix} {bar} {pos}/{human_len} [{per_sec}] {elapsed}/{duration} ETA:{eta} Errors {errors}",
                )?
                .with_key(
                    "errors",
                    CounterProgressTracker::new(context.clone(), |c| c.error_objects.get()),
                ),
            );

        let transfer_bytes_progress = multi
        .add(ProgressBar::new(0))
        .with_finish(indicatif::ProgressFinish::AndLeave)
        .with_style(
            indicatif::ProgressStyle::with_template(
            "{prefix} {bar} {bytes}/{total_bytes} [{bytes_per_sec}] {elapsed}/{duration} ETA:{eta}",
        )?);
        let instant_bytes = multi.add(
            ProgressBar::new_spinner()
                .with_finish(indicatif::ProgressFinish::AndLeave)
                .with_style(
                    indicatif::ProgressStyle::with_template(
                        "{prefix} {spinner} {bytes_per_sec} | In-flight: {listing} listings | {transfers} transfers",
                    )?
                    .with_key(
                        "listing",
                        CounterProgressTracker::new(context.clone(), |c| {
                            c.config.parallelism - c.delete_semaphore.available_permits()
                        }),
                    )
                    .with_key(
                        "transfers",
                        CounterProgressTracker::new(context.clone(), |c| {
                            c.config.parallelism - c.delete_semaphore.available_permits()
                        }),
                    ),
                ),
        );
        listing_progress.set_prefix("Listing ");
        transfer_count_progress.set_prefix("Objects ");
        transfer_bytes_progress.set_prefix("Data    ");
        instant_bytes.set_prefix("Status");

        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        Ok(Self {
            context,
            multi,
            listing_progress,
            transfer_count_progress,
            transfer_bytes_progress,
            instant_bytes,
            interval,
        })
    }

    fn update(&self) {
        let listing_count = self.context.live_objects.get() as u64;
        let listing_max = self.context.objects.get() as u64;

        let transfer_count_count = self.context.live_objects.get() as u64;
        let transfer_count_max = self.context.objects.get() as u64;

        let transfer_bytes_count = self.context.cum_live_bytes.get() as u64;
        let transfer_bytes_max = self.context.cum_bytes.get() as u64;

        self.listing_progress.set_length(listing_max);
        self.transfer_count_progress.set_length(transfer_count_max);
        self.transfer_bytes_progress.set_length(transfer_bytes_max);

        self.listing_progress.set_position(listing_count);
        self.transfer_count_progress
            .set_position(transfer_count_count);
        self.transfer_bytes_progress
            .set_position(transfer_bytes_count);
        self.instant_bytes
            .set_position(self.context.cum_bytes.get() as u64);
    }
}

impl Future for Monitor {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        while matches!(
            self.as_mut().interval.poll_tick(cx),
            std::task::Poll::Ready(_)
        ) {
            self.update();
        }

        std::task::Poll::Pending
    }
}

impl Drop for Monitor {
    fn drop(&mut self) {
        self.update();

        if !self.multi.is_hidden() {
            eprint!("\n\n\n\n");
        }

        self.multi.remove(&self.listing_progress);
        self.multi.remove(&self.transfer_count_progress);
        self.multi.remove(&self.transfer_bytes_progress);
        self.multi.remove(&self.instant_bytes);
    }
}

#[derive(Clone)]
struct CounterProgressTracker<F>
where
    F: Send + Sync + 'static + Clone,
{
    context: F,
    value: usize,
    getter: fn(&F) -> usize,
}

impl<F> indicatif::style::ProgressTracker for CounterProgressTracker<F>
where
    F: Send + Sync + 'static + Clone,
{
    fn clone_box(&self) -> Box<dyn indicatif::style::ProgressTracker> {
        Box::new(self.clone())
    }

    fn tick(&mut self, _state: &indicatif::ProgressState, _now: std::time::Instant) {
        self.value = (self.getter)(&self.context)
    }

    fn reset(&mut self, _state: &indicatif::ProgressState, _now: std::time::Instant) {}

    fn write(&self, _state: &indicatif::ProgressState, w: &mut dyn std::fmt::Write) {
        let _ = write!(w, "{}", self.value);
    }
}

impl<F> CounterProgressTracker<F>
where
    F: Send + Sync + 'static + Clone,
{
    pub fn new(context: F, getter: fn(&F) -> usize) -> Self {
        Self {
            context,
            value: 0,
            getter,
        }
    }
}
