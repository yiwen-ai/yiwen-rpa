use axum::{extract::State, middleware, routing, Json, Router};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tower::ServiceBuilder;
use tower_http::{
    catch_panic::CatchPanicLayer,
    compression::{predicate::SizeAbove, CompressionLayer},
};

use axum_web::{context, encoding};

use crate::conf;

#[derive(Serialize, Deserialize)]
pub struct AppVersion {
    pub name: String,
    pub version: String,
}

pub async fn version(State(_): State<Arc<conf::AppState>>) -> Json<AppVersion> {
    Json(AppVersion {
        name: conf::APP_NAME.to_string(),
        version: conf::APP_VERSION.to_string(),
    })
}

pub async fn new(state: Arc<conf::AppState>) -> anyhow::Result<Router> {
    let mds = ServiceBuilder::new()
        .layer(CatchPanicLayer::new())
        .layer(middleware::from_fn(context::middleware))
        .layer(CompressionLayer::new().compress_when(SizeAbove::new(encoding::MIN_ENCODING_SIZE)));

    let app = Router::new()
        .route("/", routing::get(version))
        .route("/healthz", routing::get(version))
        .route_layer(mds)
        .with_state(state);

    Ok(app)
}
