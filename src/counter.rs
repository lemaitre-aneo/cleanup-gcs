// Cleanup GCS - A tool to clean up Google Cloud Storage buckets
// Copyright (C) 2026  ANEO
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::sync::atomic::AtomicUsize;

#[derive(Default)]
pub struct Counter {
    inner: std::sync::atomic::AtomicUsize,
}

impl Counter {
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
