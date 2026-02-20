pub use crate::ExecutionContext;
use indicatif::{MultiProgress, ProgressBar};
use std::sync::Arc;

pub struct Monitor {
    context: Arc<ExecutionContext>,
    multi: MultiProgress,
    listing_progress: ProgressBar,
    deletion_progress: ProgressBar,
    bytes_progress: ProgressBar,
    instant_status: ProgressBar,
    interval: tokio::time::Interval,
}

impl Monitor {
    pub fn new(context: Arc<ExecutionContext>, multi: MultiProgress) -> anyhow::Result<Self> {
        let deletion_progress = multi
            .add(ProgressBar::new(0))
            .with_finish(indicatif::ProgressFinish::AndLeave)
            .with_prefix("Delete")
            .with_style(indicatif::ProgressStyle::with_template(
                "{prefix:7} {bar} {pos}/{human_len} [{per_sec} objects/sec]",
            )?);
        let listing_progress = multi
            .add(ProgressBar::new(0))
            .with_finish(indicatif::ProgressFinish::AndLeave)
            .with_prefix("Listing")
            .with_style(
                indicatif::ProgressStyle::with_template(
                    "{prefix:7} {spinner} {pos} objects, {live} live, {len} non-current [{per_sec}]",
                )?
                .with_key(
                    "live",
                    CounterProgressTracker::new(context.clone(), |c| c.live_objects.get()),
                ),
            );

        let bytes_progress = multi
            .add(ProgressBar::new(0))
            .with_finish(indicatif::ProgressFinish::AndLeave)
            .with_prefix("Size")
            .with_style(indicatif::ProgressStyle::with_template(
                "{prefix:7} {spinner} {bytes} total, {total_bytes} non-current",
            )?);

        let instant_status = multi.add(
            ProgressBar::new_spinner()
                .with_finish(indicatif::ProgressFinish::AndLeave)
                .with_prefix("Status")
                .with_style(
                    indicatif::ProgressStyle::with_template(
                        "{prefix:7} {spinner} {pos}/{len} in-flights, {errors} errors, {elapsed}",
                    )?
                    .with_key(
                        "errors",
                        CounterProgressTracker::new(context.clone(), |c| c.error_objects.get()),
                    ),
                ),
        );
        instant_status.set_length(context.config.deletes_parallelism as u64);

        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        Ok(Self {
            context,
            multi,
            listing_progress,
            deletion_progress,
            bytes_progress,
            instant_status,
            interval,
        })
    }

    fn update(&self) {
        let total = self.context.objects.get() as u64;
        let live = self.context.live_objects.get() as u64;
        let deleted = self.context.deleted_objects.get() as u64;
        let error = self.context.error_objects.get() as u64;
        let total_bytes = self.context.cum_bytes.get() as u64;
        let live_bytes = self.context.cum_live_bytes.get() as u64;
        let available = self.context.delete_semaphore.available_permits() as u64;
        let parallelism = self.context.config.deletes_parallelism as u64;
        let in_flight = parallelism.saturating_sub(available);

        self.listing_progress.set_position(total);
        self.listing_progress.set_length(total - live);

        self.bytes_progress.set_position(total_bytes);
        self.bytes_progress.set_length(live_bytes);

        self.deletion_progress.set_position(deleted);
        self.deletion_progress.set_length(total - live - error);

        self.instant_status.set_position(in_flight);
    }
}

impl std::future::Future for Monitor {
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
        self.multi.remove(&self.deletion_progress);
        self.multi.remove(&self.bytes_progress);
        self.multi.remove(&self.instant_status);
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
