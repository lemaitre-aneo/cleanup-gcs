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
    #[arg(short, long)]
    pub project: String,

    /// URL for the GCP endpoint
    #[arg(short, long)]
    pub gcp_endpoint: Option<String>,
}
