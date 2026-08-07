use std::path::PathBuf;

use axum::extract::Query;
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use shared::cbz_metadata::{read_simplified_metadata, SimplifiedComicMetadata};

use crate::state::State;
use crate::AppError;

pub fn routes() -> Router<State> {
    Router::<State>::new().route("/cbz-metadata", get(get_cbz_metadata))
}

#[derive(Deserialize)]
struct GetCbzMetadataQuery {
    path: PathBuf,
}

/// Reads the `ComicInfo.xml` of the CBZ at `path` and returns it simplified
/// for KOReader's document properties. Runs in-process on the already
/// running server, so callers (the KOReader Lua frontend) don't need to
/// spawn a separate binary to read it — spawning arbitrary binaries at
/// runtime is blocked on Android.
async fn get_cbz_metadata(
    Query(GetCbzMetadataQuery { path }): Query<GetCbzMetadataQuery>,
) -> Result<Json<SimplifiedComicMetadata>, AppError> {
    let metadata = read_simplified_metadata(&path)?;

    Ok(Json(metadata))
}
