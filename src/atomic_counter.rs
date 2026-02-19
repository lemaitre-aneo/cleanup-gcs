use std::sync::atomic::AtomicUsize;

#[derive(Default)]
pub struct SimpleAtomic {
    inner: std::sync::atomic::AtomicUsize,
}

#[derive(Default)]
pub struct AtomicCounter {
    pub success: SimpleAtomic,
    pub max: SimpleAtomic,
    pub error: SimpleAtomic,
}

impl SimpleAtomic {
    pub fn new(value: usize) -> Self {
        Self {
            inner: AtomicUsize::new(value),
        }
    }
    pub fn inc(&self) {
        self.add(1);
    }
    pub fn dec(&self) {
        self.sub(1);
    }
    pub fn add(&self, value: usize) {
        self.inner
            .fetch_add(value, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn sub(&self, value: usize) {
        self.inner
            .fetch_sub(value, std::sync::atomic::Ordering::Relaxed);
    }
    pub fn get(&self) -> usize {
        self.inner.load(std::sync::atomic::Ordering::Relaxed)
    }
    pub fn set(&self, value: usize) {
        self.inner
            .store(value, std::sync::atomic::Ordering::Relaxed);
    }
}

impl AtomicCounter {
    pub fn with_max(max: usize) -> Self {
        Self {
            success: Default::default(),
            max: SimpleAtomic { inner: AtomicUsize::new(max) },
            error: Default::default(),
        }
    }
    
    pub fn progress(&self) -> usize {
        self.succeeded() + self.error.get()
    }

    pub fn succeeded(&self) -> usize {
        self.success.get()
    }

    pub fn error(&self) -> usize {
        self.error.get()
    }

    pub fn max(&self) -> usize {
        self.max.get()
    }
}
