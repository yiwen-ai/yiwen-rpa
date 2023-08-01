use config::{Config, ConfigError, File, FileFormat};
use serde::Deserialize;
use std::sync::Arc;

pub const APP_NAME: &str = env!("CARGO_PKG_NAME");
pub const APP_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Clone)]
pub struct AppState {
    pub handling: Arc<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Log {
    pub level: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Server {
    pub port: u16,
    pub cert_file: String,
    pub key_file: String,
    pub graceful_shutdown: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Base {
    pub taskbase: String,
    pub writing: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Conf {
    pub env: String,
    pub log: Log,
    pub server: Server,
    pub base: Base,
}

impl Conf {
    pub fn new() -> anyhow::Result<Self, ConfigError> {
        let file_name =
            std::env::var("CONFIG_FILE_PATH").unwrap_or_else(|_| "./config/default.toml".into());
        Self::from(&file_name)
    }

    pub fn from(file_name: &str) -> anyhow::Result<Self, ConfigError> {
        let builder = Config::builder().add_source(File::new(file_name, FileFormat::Toml));
        builder.build()?.try_deserialize::<Conf>()
    }

    pub async fn new_app_state(&self) -> anyhow::Result<Arc<AppState>> {
        Ok(Arc::new(AppState {
            handling: Arc::new("handling".to_string()),
        }))
    }
}
