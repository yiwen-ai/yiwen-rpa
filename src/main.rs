use futures::future::FutureExt;
use std::{io, net::SocketAddr, sync::Arc};
use structured_logger::{async_json::new_writer, Builder};
use tokio::{
    signal,
    time::{sleep, Duration},
};

mod background_job;
mod conf;
mod http_api;
mod jobs;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    let cfg = conf::Conf::new().unwrap_or_else(|err| panic!("config error: {}", err));

    Builder::with_level(cfg.log.level.as_str())
        .with_target_writer("*", new_writer(tokio::io::stdout()))
        .init();

    log::debug!("{:?}", cfg);

    let server_cfg = cfg.server.clone();
    let server_env = cfg.env.clone();
    let addr = SocketAddr::from(([0, 0, 0, 0], server_cfg.port));

    let app_state = cfg.new_app_state().await?;
    let app = http_api::new(app_state.clone()).await?;
    let shutdown = shutdown_signal(app_state.clone(), server_cfg.graceful_shutdown).shared();

    let api = async {
        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .with_graceful_shutdown(shutdown.clone())
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Interrupted, e))
    };

    let rpa = background_job::new(app_state.clone(), cfg);
    let monitor = async {
        rpa.run_with_signal(async {
            shutdown.clone().await;
            Ok(())
        })
        .await
    };

    log::info!(
        "{}@{} start {} at {}",
        conf::APP_NAME,
        conf::APP_VERSION,
        server_env,
        &addr
    );

    futures::future::try_join(api, monitor)
        .await
        .expect("Could not start services");
    Ok(())
}

async fn shutdown_signal(app: Arc<conf::AppState>, wait_secs: usize) {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    log::info!("signal received, starting graceful shutdown");

    let mut secs = wait_secs;
    loop {
        let handling = Arc::strong_count(&app.handling);
        if secs == 0 || handling <= 1 {
            log::info!("Goodbye!"); // Say goodbye and then be terminated...
            return;
        }

        log::info!(
            "signal received, waiting for {} handling to finish, or countdown: {} seconds",
            handling,
            secs
        );
        secs -= 1;
        sleep(Duration::from_secs(1)).await;
    }
}
