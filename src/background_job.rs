use apalis_core::{
    builder::{WorkerBuilder, WorkerFactory},
    context::JobContext,
    executor::TokioExecutor,
    job::Job,
    job_fn::job_fn,
    layers::extensions::Extension,
    monitor::Monitor,
    request::JobState,
    utils::timer::TokioTimer,
};
use apalis_cron::{CronStream, Schedule};
use chrono::{DateTime, Utc};
use std::{str::FromStr, sync::Arc, time::Instant};
use tower::ServiceBuilder;

use crate::conf;
use crate::jobs;

#[derive(Default, Debug, Clone)]
struct Reminder(DateTime<Utc>);
impl From<DateTime<Utc>> for Reminder {
    fn from(t: DateTime<Utc>) -> Self {
        Reminder(t)
    }
}

impl Job for Reminder {
    const NAME: &'static str = "reminder::MinutelyReminder";
}

async fn send_reminder(_job: Reminder, mut ctx: JobContext) {
    let start = Instant::now();
    let state: &Arc<conf::AppState> = ctx.data_opt().unwrap();
    let rpa: &Arc<jobs::RPA> = ctx.data_opt().unwrap();
    let mark = state.handling.clone();
    let rid = uuid::Uuid::from_u128(ctx.id().inner().0).to_string();
    match rpa.execute(&ctx, state.clone()).await {
        Ok(_) => {
            ctx.set_status(JobState::Done);
            log::info!(target: "job",
                action = "execute",
                rid = &rid,
                start = ctx.run_at().timestamp_millis(),
                elapsed = start.elapsed().as_millis() as u64;
                "finished",
            );
        }
        Err(err) => {
            ctx.set_status(JobState::Failed);
            log::error!(target: "job",
                action = "execute",
                rid = &rid,
                start = ctx.run_at().timestamp_millis(),
                elapsed = start.elapsed().as_millis() as u64,
                error = err.to_string();
                "failed",
            );
        }
    }

    let _ = mark.as_str(); // avoid unused warning
}

pub fn new(state: Arc<conf::AppState>, cfg: conf::Conf) -> Monitor<TokioExecutor> {
    let rpa = Arc::new(jobs::RPA::new(cfg));
    let schedule = Schedule::from_str("0 * * * * * *").unwrap();
    let service = ServiceBuilder::new()
        .layer(Extension(state))
        .layer(Extension(rpa))
        .service(job_fn(send_reminder));
    let worker = WorkerBuilder::new(conf::APP_NAME)
        .stream(CronStream::new(schedule).timer(TokioTimer).to_stream())
        .build(service);

    Monitor::new().register(worker)
}
