use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use shared::cbz_metadata::read_simplified_metadata;

#[derive(Parser, Debug)]
struct Args {
    /// Path to the CBZ file
    file_path: String,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let file_path = PathBuf::from(&args.file_path);

    let metadata = read_simplified_metadata(&file_path)
        .with_context(|| format!("Could not read metadata from {}", file_path.display()))?;

    let output_json =
        serde_json::to_string(&metadata).context("Failed to serialize metadata to JSON")?;
    println!("{}", output_json);

    Ok(())
}
