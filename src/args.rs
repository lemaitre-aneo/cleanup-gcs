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

    /// Parallelism for object deletion
    #[arg(long, default_value_t = 1000)]
    pub parallelism: usize,
    
    /// Do not perform any deletion
    #[arg(short, long, default_value_t = false)]
    pub dry_run: bool,

    /// Start listing at this object name
    #[arg(short = 'S', long)]
    pub start: Option<String>,

    /// Stop listing at this object name
    #[arg(short = 'E', long)]
    pub end: Option<String>,
    
    /// List only objects whose name have this prefix
    #[arg(short, long)]
    pub prefix: Option<String>,
}
