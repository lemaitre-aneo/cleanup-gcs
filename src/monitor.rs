pub use crate::ExecutionContext;
use indicatif::{MultiProgress, ProgressBar};
use std::sync::Arc;

pub struct Monitor {
    context: Arc<ExecutionContext>,
    multi: MultiProgress,
    listing_progress: ProgressBar,
    deletion_progress: ProgressBar,
    object_progress: ProgressBar,
    bytes_progress: ProgressBar,
    interval: tokio::time::Interval,
}

impl Monitor {
    pub fn new(context: Arc<ExecutionContext>, multi: MultiProgress) -> anyhow::Result<Self> {
        let listing_progress = multi
            .add(ProgressBar::new(1))
            .with_finish(indicatif::ProgressFinish::AndLeave)
            .with_prefix("Listing")
            .with_style(
                indicatif::ProgressStyle::with_template(
                    "{prefix:7} {bar} {pos}/{len}, {errors} errors [{per_sec}, ETA: {eta}, inflight: {inflight}/{parallelism}]",
                )?
                .with_key(
                    "errors",
                    CounterProgressTracker::new(context.clone(), |c| c.error_listings.get()),
                )
                .with_key(
                    "inflight",
                    CounterProgressTracker::new(context.clone(), |c| c.config.listings_parallelism - c.listing_semaphore.available_permits()),
                )
                .with_key(
                    "parallelism",
                    CounterProgressTracker::new(context.clone(), |c| c.config.listings_parallelism),
                ),
            );
        let deletion_progress = multi
            .add(ProgressBar::new(0))
            .with_finish(indicatif::ProgressFinish::AndLeave)
            .with_prefix("Delete")
            .with_style(
                indicatif::ProgressStyle::with_template(
                    "{prefix:7} {bar} {human_pos}/{human_len}, {errors} errors [{per_sec}, ETA: {eta}, inflight: {inflight}/{parallelism}]",
                )?
                .with_key(
                    "errors",
                    CounterProgressTracker::new(context.clone(), |c| c.error_objects.get()),
                )
                .with_key(
                    "inflight",
                    CounterProgressTracker::new(context.clone(), |c| c.config.deletes_parallelism - c.delete_semaphore.available_permits()),
                )
                .with_key(
                    "parallelism",
                    CounterProgressTracker::new(context.clone(), |c| c.config.deletes_parallelism),
                ),
            );
        let object_progress = multi.add(
            ProgressBar::new_spinner()
                .with_finish(indicatif::ProgressFinish::AndLeave)
                .with_prefix("Objects")
                .with_style(
                    indicatif::ProgressStyle::with_template(
                        "{prefix:7} {spinner} {human_len} objects, {human_pos} live, {noncurrent} non-current [elapsed: {elapsed}]",
                    )?
                    .with_key(
                        "noncurrent",
                        CounterProgressTracker::new(context.clone(), |c| c.objects.get() - c.live_objects.get()),
                    ),
                ),
        );
        let bytes_progress = multi
            .add(ProgressBar::new(0))
            .with_finish(indicatif::ProgressFinish::AndLeave)
            .with_prefix("Size")
            .with_style(indicatif::ProgressStyle::with_template(
                "{prefix:7} {spinner} {total_bytes} total, {bytes} non-current [elapsed: {elapsed}]",
            )?);

        object_progress.set_length(context.config.deletes_parallelism as u64);

        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        Ok(Self {
            context,
            multi,
            listing_progress,
            deletion_progress,
            object_progress,
            bytes_progress,
            interval,
        })
    }

    fn update(&self) {
        self.listing_progress.update(|state| {
            state.set_pos((self.context.listings.get() + self.context.error_listings.get()) as u64);
            state.set_len(self.context.total_listings.get() as u64);
        });
        self.deletion_progress.update(|state| {
            state.set_pos(
                (self.context.deleted_objects.get() + self.context.error_objects.get()) as u64,
            );
            state.set_len((self.context.objects.get() - self.context.live_objects.get()) as u64);
        });
        self.object_progress.update(|state| {
            state.set_pos(self.context.live_objects.get() as u64);
            state.set_len(self.context.objects.get() as u64);
        });
        self.bytes_progress.update(|state| {
            let total = self.context.cum_bytes.get() as u64;
            state.set_pos(total - self.context.cum_live_bytes.get() as u64);
            state.set_len(total);
        });
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
        self.multi.remove(&self.object_progress);
        self.multi.remove(&self.bytes_progress);
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
