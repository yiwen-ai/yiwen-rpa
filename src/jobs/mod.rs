use apalis_core::context::JobContext;
use libflate::gzip::Encoder;
use reqwest::{header, Client, Method};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    io::Write,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::conf;
use axum_web::{
    context::unix_ms,
    erring::SuccessResponse,
    object::{cbor_from_slice, cbor_to_vec, PackObject},
};

const JARVIS: &str = "0000000000000jarvis0";
const COMPRESS_MIN_LENGTH: usize = 512;
static X_REQUEST_ID: header::HeaderName = header::HeaderName::from_static("x-request-id");
static APP_USER_AGENT: &str = concat!(
    "reqwest ",
    env!("CARGO_PKG_NAME"),
    "/",
    env!("CARGO_PKG_VERSION"),
);

pub struct RPA {
    client: Client,
    taskbase: reqwest::Url,
    writing: reqwest::Url,
    system_user: PackObject<xid::Id>,
}

impl RPA {
    pub fn new(cfg: conf::Conf) -> Self {
        let mut headers: header::HeaderMap<header::HeaderValue> =
            header::HeaderMap::with_capacity(2);
        headers.insert(header::ACCEPT, "application/cbor".parse().unwrap());
        headers.insert(header::ACCEPT_ENCODING, "gzip".parse().unwrap());
        headers.insert("x-auth-user", JARVIS.parse().unwrap());
        headers.insert("x-auth-user-rating", "127".parse().unwrap());

        let client = reqwest::Client::builder()
            .use_rustls_tls()
            .https_only(false)
            .http2_keep_alive_interval(Some(Duration::from_secs(25)))
            .http2_keep_alive_timeout(Duration::from_secs(15))
            .http2_keep_alive_while_idle(true)
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(60))
            .gzip(true)
            .user_agent(APP_USER_AGENT)
            .default_headers(headers)
            .build()
            .unwrap();

        let taskbase = reqwest::Url::parse(&cfg.base.taskbase).unwrap();
        let writing = reqwest::Url::parse(&cfg.base.writing).unwrap();

        Self {
            client,
            taskbase,
            writing,
            system_user: PackObject::Cbor(xid::Id::from_str(JARVIS).unwrap()),
        }
    }

    pub async fn execute(
        &self,
        ctx: &JobContext,
        _state: Arc<conf::AppState>,
    ) -> anyhow::Result<()> {
        self.publication_review(ctx.id().inner().0).await
    }

    async fn request<IN: Serialize, OUT: DeserializeOwned>(
        &self,
        method: Method,
        url: reqwest::Url,
        rid: &str,
        body: Option<&IN>,
    ) -> anyhow::Result<OUT> {
        let req = self
            .client
            .request(method, url)
            .header(header::ACCEPT_ENCODING, "gzip")
            .header(&X_REQUEST_ID, rid);

        let res = match body {
            None => req.send().await?,
            Some(body) => {
                let data = cbor_to_vec(body)?;
                if data.len() >= COMPRESS_MIN_LENGTH {
                    let mut encoder = Encoder::new(Vec::new())?;
                    encoder.write_all(&data)?;
                    let data = encoder.finish().into_result()?;

                    req.header("content-encoding", "gzip")
                        .header(header::CONTENT_TYPE, "application/cbor")
                        .body(data)
                        .send()
                        .await?
                } else {
                    req.header(header::CONTENT_TYPE, "application/cbor")
                        .body(data)
                        .send()
                        .await?
                }
            }
        };

        let status = res.status().as_u16();
        if status >= 204 {
            let text = res.text().await?;
            anyhow::bail!("{}: {}", status, text);
        }

        let body = res.bytes().await?;
        let output: SuccessResponse<OUT> = cbor_from_slice(&body)?;
        Ok(output.result)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Pagination {
    pub uid: PackObject<xid::Id>,
    pub page_token: Option<PackObject<Vec<u8>>>,
    pub page_size: Option<u16>,
    pub status: Option<i8>,
    pub fields: Option<Vec<String>>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct NotificationOutput {
    pub sender: PackObject<xid::Id>,
    pub tid: PackObject<xid::Id>,
    pub gid: PackObject<xid::Id>,
    pub status: i8,
    pub ack_status: i8,
    pub kind: String,
    pub payload: PackObject<Vec<u8>>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct AckTaskInput {
    pub uid: PackObject<xid::Id>,
    pub tid: PackObject<xid::Id>,
    pub sender: PackObject<xid::Id>,
    pub status: i8,
    pub message: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DeleteTaskInput {
    pub uid: PackObject<xid::Id>,
    pub id: Option<PackObject<xid::Id>>,
    pub status: Option<i8>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct PublicationInput {
    pub gid: PackObject<xid::Id>,
    pub cid: PackObject<xid::Id>,
    pub language: String,
    pub version: i16,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct PublicationOutput {
    pub gid: PackObject<xid::Id>,
    pub cid: PackObject<xid::Id>,
    pub language: String,
    pub version: i16,
    pub status: i8,
    pub updated_at: i64,
    pub content_length: usize,
}

impl RPA {
    async fn publication_review(&self, jid: u128) -> anyhow::Result<()> {
        let jid = uuid::Uuid::from_u128(jid).to_string();
        let ts = unix_ms() as i64 - 8 * 60 * 1000;
        let start = Instant::now();
        let todo = self.list_todo(&jid).await?;
        log::info!(target: "job",
            action = "list_todo",
            rid = &jid,
            todo = todo.len();
            "start",
        );

        for item in todo {
            let item_start = start.elapsed().as_millis() as u64;
            let task_uid = item.sender.clone();
            let task_id = item.tid.clone();
            let res = self.publication_review_item(&jid, ts, item).await;
            let elapsed = start.elapsed().as_millis() as u64 - item_start;
            match res {
                Ok(_) => {
                    log::info!(target: "job",
                        action = "publication_review",
                        rid = &jid,
                        start = item_start,
                        elapsed = elapsed;
                        "finished",
                    );
                }
                Err(err) => {
                    log::error!(
                        target: "job",
                        rid = &jid,
                        start = item_start,
                        elapsed = elapsed,
                        error = err.to_string();
                        "failed",
                    );
                    // clear invalid task
                    let _ = self
                        .remove_todo(
                            &jid,
                            &DeleteTaskInput {
                                uid: task_uid,
                                id: Some(task_id),
                                status: None,
                            },
                        )
                        .await;
                }
            }
        }

        Ok(())
    }

    async fn publication_review_item(
        &self,
        jid: &str,
        ts: i64,
        item: NotificationOutput,
    ) -> anyhow::Result<()> {
        let publ: PublicationInput = cbor_from_slice(&item.payload.unwrap())?;
        let mut publ = self.get_publication(jid, &publ).await?;
        if publ.status < 0 || publ.updated_at > ts {
            return Ok(());
        }
        if publ.status == 0 {
            publ.status = 1;
            let _ = self.set_publication_status(jid, &publ).await?;
        }
        self.ack_todo(
            jid,
            &AckTaskInput {
                uid: self.system_user.clone(),
                tid: item.tid,
                sender: item.sender,
                status: 1,
                message: "Done".to_string(),
            },
        )
        .await?;
        Ok(())
    }

    async fn list_todo(&self, jid: &str) -> anyhow::Result<Vec<NotificationOutput>> {
        let url = self.taskbase.join("/v1/notification/list")?;
        let res: Vec<NotificationOutput> = self
            .request(
                Method::POST,
                url,
                jid,
                Some(&Pagination {
                    uid: self.system_user.clone(),
                    page_token: None,
                    page_size: Some(1000),
                    status: Some(0i8),
                    fields: Some(vec!["payload".to_string()]),
                }),
            )
            .await?;
        Ok(res)
    }

    async fn ack_todo(&self, jid: &str, input: &AckTaskInput) -> anyhow::Result<()> {
        let url = self.taskbase.join("/v1/task/ack")?;
        let _: bool = self.request(Method::PATCH, url, jid, Some(input)).await?;

        let url = self.taskbase.join("/v1/notification/delete")?;
        let _: bool = self.request(Method::POST, url, jid, Some(input)).await?;
        Ok(())
    }

    async fn remove_todo(&self, jid: &str, input: &DeleteTaskInput) -> anyhow::Result<()> {
        let url = self.taskbase.join("/v1/task/delete")?;
        let _: bool = self.request(Method::POST, url, jid, Some(input)).await?;
        Ok(())
    }

    async fn get_publication(
        &self,
        jid: &str,
        input: &PublicationInput,
    ) -> anyhow::Result<PublicationOutput> {
        let mut url = self.writing.join("/v1/publication")?;
        url.query_pairs_mut()
            .append_pair("gid", &input.gid.to_string())
            .append_pair("cid", &input.cid.to_string())
            .append_pair("language", &input.language)
            .append_pair("version", &input.version.to_string())
            .append_pair("fields", "status,updated_at,content_length");
        let res = self
            .request::<(), PublicationOutput>(Method::GET, url, jid, None)
            .await?;
        Ok(res)
    }

    async fn set_publication_status(
        &self,
        jid: &str,
        input: &PublicationOutput,
    ) -> anyhow::Result<PublicationOutput> {
        let url = self.writing.join("/v1/publication/update_status")?;
        let res: PublicationOutput = self.request(Method::PATCH, url, jid, Some(input)).await?;
        Ok(res)
    }
}
