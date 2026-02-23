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

use std::str::FromStr;

#[derive(Clone, Copy, Debug)]
pub struct Size {
    value: usize,
}

impl FromStr for Size {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let value = parse_size::parse_size(s)?;
        Ok(Self {
            value: value as usize,
        })
    }
}

impl From<Size> for usize {
    fn from(val: Size) -> Self {
        val.value
    }
}

#[derive(clap::Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Configuration {
    /// Name of the bucket
    #[arg(short, long)]
    pub bucket: String,

    /// ID of the project
    #[arg(short = 'P', long, hide = true, default_value = "_")]
    pub project: String,

    /// URL for the GCP endpoint
    #[arg(short, long)]
    pub gcp_endpoint: Option<String>,

    /// Max number of concurrent listings
    #[arg(short = 'l', long, default_value_t = 100)]
    pub listings_parallelism: usize,

    /// Max number of concurrent deletes
    #[arg(short = 'n', long, default_value_t = 1000)]
    pub deletes_parallelism: usize,

    /// Max number of concurrent listings
    #[arg(short = 'L', long, default_value_t = 10000)]
    pub listings_buffer: usize,

    /// Max number of concurrent deletes
    #[arg(short = 'N', long, default_value_t = 1000000)]
    pub deletes_buffer: usize,

    /// Do not perform any deletion
    #[arg(short, long, default_value_t = false)]
    pub dry_run: bool,

    /// Fetch all metadata and print them on trace loglevel
    #[arg(long, hide = true, default_value_t = false)]
    pub fetch_all_metadata: bool,

    /// Start listing at this object name
    #[arg(short = 'S', long)]
    pub start: Option<String>,

    /// Stop listing at this object name
    #[arg(short = 'E', long)]
    pub end: Option<String>,

    /// List only objects whose name have this prefix
    #[arg(short, long)]
    pub prefix: Option<String>,

    /// Parallelize listings up to given depth
    #[arg(short, long, default_value_t = 0)]
    pub tree_depth: i64,
}
