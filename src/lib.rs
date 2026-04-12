use std::collections::HashMap;
use std::io::Cursor;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use chrono::{DateTime, Utc};
use futures::future::{BoxFuture, FutureExt, Shared};
use google_cloud_auth::token::DefaultTokenSourceProvider;
use google_cloud_bigquery::client::{Client, ClientConfig};
use google_cloud_bigquery::grpc::apiv1::bigquery_client::StreamingReadClient;
use google_cloud_bigquery::http::job::get::GetJobRequest;
use google_cloud_bigquery::http::job::query::QueryRequest;
use google_cloud_bigquery::http::job::{JobState, JobType};
use google_cloud_gax::conn::{ConnectionManager, ConnectionOptions, Environment};
use google_cloud_googleapis::cloud::bigquery::storage::v1::big_query_read_client::BigQueryReadClient;
use google_cloud_googleapis::cloud::bigquery::storage::v1::read_rows_response::Rows;
use google_cloud_googleapis::cloud::bigquery::storage::v1::read_session::{
    table_read_options::OutputFormatSerializationOptions, Schema as SessionSchema, TableReadOptions,
};
use google_cloud_googleapis::cloud::bigquery::storage::v1::{
    ArrowSerializationOptions, CreateReadSessionRequest, DataFormat, ReadRowsRequest, ReadSession,
};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3_arrow::PyTable;
use redis::AsyncCommands;
use sha2::{Digest, Sha256};
use tokio::runtime::Runtime;

// ── Error type ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, thiserror::Error)]
enum BQRedisError {
    #[error("BigQuery error: {0}")]
    BigQuery(String),
    #[error("Redis error: {0}")]
    Redis(String),
    #[error("Arrow error: {0}")]
    Arrow(String),
    #[error("{0}")]
    Other(String),
}

impl From<BQRedisError> for PyErr {
    fn from(e: BQRedisError) -> Self {
        PyRuntimeError::new_err(e.to_string())
    }
}

// Results shared across concurrent waiters must be Clone.
type SharedResult<T> = Result<T, Arc<BQRedisError>>;
type SharedFuture = Shared<BoxFuture<'static, SharedResult<Arc<QueryResult>>>>;

// ── Cached query result ────────────────────────────────────────────────────────

#[derive(Clone)]
struct QueryResult {
    query_time: DateTime<Utc>,
    serialized_schema: Vec<u8>,
    serialized_data: Vec<u8>,
}

impl QueryResult {
    /// Decode the IPC bytes and return a `pyarrow.Table`.
    ///
    /// `serialized_data` is always a complete Arrow IPC stream (schema + batches + EOS).
    /// For empty tables it contains only the schema message followed by EOS.
    fn to_pyarrow(&self, py: Python) -> PyResult<Py<PyAny>> {
        let reader = StreamReader::try_new(Cursor::new(&self.serialized_data), None)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let schema = reader.schema();
        let batches: Result<Vec<_>, _> = reader.collect();
        let batches = batches.map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        PyTable::try_new(batches, schema)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
            .into_pyarrow(py)
            .map(|b| b.unbind())
    }
}

// ── Configuration ─────────────────────────────────────────────────────────────

struct BQRedisConfig {
    redis_key_prefix: String,
    cache_ttl_sec: u64,
    background_refresh_ttl_sec: u64,
    max_stream_count: i32,
}

// ── Core state (shared across Python threads via Arc) ─────────────────────────

struct BQRedisInner {
    project_id: String,
    bq_client: Client,
    conn_manager: Arc<ConnectionManager>,
    redis: redis::Client,
    config: BQRedisConfig,
    /// One in-flight future per query key. Concurrent callers for the same key
    /// each get a clone of the same Shared future and all receive the result
    /// once the single underlying task completes.
    inflight: Mutex<HashMap<String, SharedFuture>>,
}

impl BQRedisInner {
    /// Create a fresh StreamingReadClient from the shared connection pool.
    fn streaming_client(&self) -> StreamingReadClient {
        StreamingReadClient::new(BigQueryReadClient::new(self.conn_manager.conn()))
    }

    fn query_key(&self, query: &str) -> String {
        let hash = Sha256::digest(query.as_bytes());
        format!("{}{}", self.config.redis_key_prefix, hex::encode(hash))
    }

    /// Return an existing in-flight future for `key`, or create and register a
    /// new one that drives `run_query` to completion.
    fn get_or_submit(self: Arc<Self>, query: String, key: String) -> SharedFuture {
        let inflight = self.inflight.lock().unwrap();
        if let Some(fut) = inflight.get(&key) {
            tracing::debug!("Re-used inflight request for key: {key}");
            return fut.clone();
        }
        tracing::debug!("Dispatching new inflight request for key: {key}");

        let inner = Arc::clone(&self);
        let key_for_cleanup = key.clone();
        let fut: SharedFuture = async move {
            let result = inner
                .clone()
                .run_query_and_cache(query, key.clone())
                .await;
            // Remove from inflight map once the result is ready.
            if let Ok(mut inflight) = inner.inflight.lock() {
                inflight.remove(&key);
            }
            result
        }
        .boxed()
        .shared();

        self.inflight
            .lock()
            .unwrap()
            .insert(key_for_cleanup, fut.clone());
        fut
    }

    /// Execute the BigQuery job, read via Storage API, cache in Redis, return result.
    async fn run_query_and_cache(
        self: Arc<Self>,
        query: String,
        key: String,
    ) -> SharedResult<Arc<QueryResult>> {
        let result = self
            .read_bigquery_bytes(&query)
            .await
            .map_err(Arc::new)?;
        self.cache_put(&key, &result).await.map_err(Arc::new)?;
        tracing::debug!("Cached query result for key: {key}");
        Ok(Arc::new(result))
    }

    /// Submit a BigQuery query job, wait for completion, stream Arrow IPC bytes
    /// via the BigQuery Storage Read API, and return the raw bytes.
    async fn read_bigquery_bytes(&self, query: &str) -> Result<QueryResult, BQRedisError> {
        let query_time = Utc::now();

        // ── Submit job ────────────────────────────────────────────────────────
        let query_req = QueryRequest {
            query: query.to_string(),
            timeout_ms: Some(0), // return immediately with a job reference
            use_legacy_sql: false,
            ..Default::default()
        };
        let query_resp = self
            .bq_client
            .job()
            .query(&self.project_id, &query_req)
            .await
            .map_err(|e| BQRedisError::BigQuery(e.to_string()))?;

        let job_ref = query_resp.job_reference;

        // ── Poll until done ────────────────────────────────────────────────────
        let completed_job = loop {
            let job = self
                .bq_client
                .job()
                .get(
                    &job_ref.project_id,
                    &job_ref.job_id,
                    &GetJobRequest {
                        location: job_ref.location.clone(),
                    },
                )
                .await
                .map_err(|e| BQRedisError::BigQuery(e.to_string()))?;

            if matches!(job.status.state, JobState::Done) {
                break job;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        };

        // ── Check for job-level errors ────────────────────────────────────────
        if let Some(err) = completed_job.status.error_result.as_ref() {
            return Err(BQRedisError::BigQuery(format!(
                "{}: {}",
                err.reason.as_deref().unwrap_or("unknown"),
                err.message.as_deref().unwrap_or("")
            )));
        }

        // ── Get destination table ─────────────────────────────────────────────
        let destination = match &completed_job.configuration.job {
            JobType::Query(q) => q
                .destination_table
                .as_ref()
                .ok_or_else(|| BQRedisError::BigQuery("Job has no destination table".into()))?,
            _ => return Err(BQRedisError::BigQuery("Expected a query job".into())),
        };

        let table_path = format!(
            "projects/{}/datasets/{}/tables/{}",
            destination.project_id, destination.dataset_id, destination.table_id,
        );

        // ── Create BigQuery Storage read session ──────────────────────────────
        let mut streaming_client = self.streaming_client();

        let read_options = Some(TableReadOptions {
            output_format_serialization_options: Some(
                OutputFormatSerializationOptions::ArrowSerializationOptions(
                    ArrowSerializationOptions {
                        buffer_compression: 2, // ZSTD = 2
                    },
                ),
            ),
            ..Default::default()
        });

        let session = streaming_client
            .create_read_session(
                CreateReadSessionRequest {
                    parent: format!("projects/{}", self.project_id),
                    read_session: Some(ReadSession {
                        table: table_path,
                        data_format: DataFormat::Arrow.into(),
                        read_options,
                        ..Default::default()
                    }),
                    max_stream_count: self.config.max_stream_count,
                    ..Default::default()
                },
                None,
            )
            .await
            .map_err(|e| BQRedisError::BigQuery(e.to_string()))?
            .into_inner();

        // ── Extract Arrow schema ───────────────────────────────────────────────
        let raw_schema_bytes = match &session.schema {
            Some(SessionSchema::ArrowSchema(s)) => s.serialized_schema.to_vec(),
            _ => {
                return Err(BQRedisError::Arrow(
                    "BigQuery Storage session did not return an Arrow schema".into(),
                ))
            }
        };

        // Parse the Arrow schema from BigQuery's raw schema message.
        // StreamReader reads only the schema header, so it works whether or
        // not there is a trailing EOS marker in raw_schema_bytes.
        let arrow_schema = StreamReader::try_new(Cursor::new(&raw_schema_bytes), None)
            .map_err(|e| BQRedisError::Arrow(format!("Failed to parse BQ schema: {e}")))?
            .schema();

        // Build a clean schema-only IPC stream (schema message + EOS).
        let mut serialized_schema = Vec::new();
        {
            let mut w = StreamWriter::try_new(&mut serialized_schema, &arrow_schema)
                .map_err(|e| BQRedisError::Arrow(e.to_string()))?;
            w.finish().map_err(|e| BQRedisError::Arrow(e.to_string()))?;
        }

        // Prepare schema-without-EOS bytes for building per-batch mini-streams.
        const EOS_MARKER: &[u8] = &[0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00];
        let schema_no_eos: &[u8] = if raw_schema_bytes.ends_with(EOS_MARKER) {
            &raw_schema_bytes[..raw_schema_bytes.len() - 8]
        } else {
            &raw_schema_bytes[..]
        };

        // ── Stream record batches ─────────────────────────────────────────────
        let mut all_batch_bytes: Vec<Vec<u8>> = Vec::new();
        let mut page_count = 0usize;

        for stream in &session.streams {
            let mut row_stream = streaming_client
                .read_rows(
                    ReadRowsRequest {
                        read_stream: stream.name.clone(),
                        offset: 0,
                    },
                    None,
                )
                .await
                .map_err(|e| BQRedisError::BigQuery(e.to_string()))?
                .into_inner();

            while let Some(response) = row_stream
                .message()
                .await
                .map_err(|e| BQRedisError::BigQuery(e.to_string()))?
            {
                if let Some(Rows::ArrowRecordBatch(batch)) = response.rows {
                    all_batch_bytes.push(batch.serialized_record_batch.to_vec());
                    page_count += 1;
                }
            }
        }

        tracing::debug!(
            "Query returned {page_count} pages in {} streams",
            session.streams.len(),
        );

        // ── Build complete Arrow IPC stream ───────────────────────────────────
        // serialized_data is always a complete IPC stream (schema + batches + EOS)
        // so that cache_get / to_pyarrow can parse it directly without concatenation.
        let mut serialized_data = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut serialized_data, &arrow_schema)
                .map_err(|e| BQRedisError::Arrow(e.to_string()))?;

            for batch_bytes in &all_batch_bytes {
                // Each BigQuery serialized_record_batch is a raw IPC record batch
                // message without a schema header.  Wrap it in a mini-stream
                // (schema_no_eos + batch + EOS) so StreamReader can parse it.
                let mut mini = Vec::with_capacity(schema_no_eos.len() + batch_bytes.len() + 8);
                mini.extend_from_slice(schema_no_eos);
                mini.extend_from_slice(batch_bytes);
                mini.extend_from_slice(EOS_MARKER);

                let mut mini_reader = StreamReader::try_new(Cursor::new(mini), None)
                    .map_err(|e| BQRedisError::Arrow(format!("Failed to parse BQ batch: {e}")))?;
                if let Some(batch) = mini_reader.next() {
                    let batch = batch.map_err(|e| BQRedisError::Arrow(e.to_string()))?;
                    writer
                        .write(&batch)
                        .map_err(|e| BQRedisError::Arrow(e.to_string()))?;
                }
            }

            writer
                .finish()
                .map_err(|e| BQRedisError::Arrow(e.to_string()))?;
        }

        Ok(QueryResult {
            query_time,
            serialized_schema,
            serialized_data,
        })
    }

    // ── Redis helpers ─────────────────────────────────────────────────────────

    async fn redis_conn(&self) -> Result<redis::aio::MultiplexedConnection, BQRedisError> {
        self.redis
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| BQRedisError::Redis(e.to_string()))
    }

    async fn cache_put(&self, key: &str, result: &QueryResult) -> Result<(), BQRedisError> {
        let mut conn = self.redis_conn().await?;
        let ttl = self.config.cache_ttl_sec;
        redis::pipe()
            .set_ex(
                format!("{key}:schema"),
                result.serialized_schema.as_slice(),
                ttl,
            )
            .set_ex(
                format!("{key}:data"),
                result.serialized_data.as_slice(),
                ttl,
            )
            .set_ex(
                format!("{key}:query_time"),
                result.query_time.to_rfc3339().as_str(),
                ttl,
            )
            .query_async::<()>(&mut conn)
            .await
            .map_err(|e| BQRedisError::Redis(e.to_string()))
    }

    /// Returns `(result, query_time, needs_background_refresh)` or `None` if
    /// there is no usable cached value.
    async fn cache_get(
        &self,
        key: &str,
        max_age: Option<i64>,
    ) -> Result<Option<(QueryResult, DateTime<Utc>, bool)>, BQRedisError> {
        let mut conn = self.redis_conn().await?;
        let (data, schema, query_time_str): (
            Option<Vec<u8>>,
            Option<Vec<u8>>,
            Option<String>,
        ) = redis::pipe()
            .get(format!("{key}:data"))
            .get(format!("{key}:schema"))
            .get(format!("{key}:query_time"))
            .query_async(&mut conn)
            .await
            .map_err(|e| BQRedisError::Redis(e.to_string()))?;

        let (Some(data), Some(query_time_str)) = (data, query_time_str) else {
            return Ok(None);
        };

        let cached_query_time = query_time_str
            .parse::<DateTime<Utc>>()
            .map_err(|e| BQRedisError::Other(e.to_string()))?;

        let age_sec = (Utc::now() - cached_query_time).num_seconds();

        if max_age == Some(0) || max_age.is_some_and(|ma| age_sec > ma) {
            return Ok(None);
        }

        let needs_refresh = age_sec > self.config.background_refresh_ttl_sec as i64;

        Ok(Some((
            QueryResult {
                query_time: cached_query_time,
                serialized_schema: schema.unwrap_or_default(),
                serialized_data: data,
            },
            cached_query_time,
            needs_refresh,
        )))
    }

    async fn background_refresh(&self, query: String, key: String) {
        let bg_key = format!("{key}:background_refresh");
        let mut conn = match self.redis_conn().await {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("Background refresh: Redis connect failed: {e}");
                return;
            }
        };
        let already: bool = conn
            .exists(&bg_key)
            .await
            .unwrap_or(false);
        if already {
            tracing::info!("Background refresh already in progress for key: {key}");
            return;
        }
        let _: Result<(), _> = conn
            .set_ex(&bg_key, 1u8, self.config.background_refresh_ttl_sec)
            .await;

        tracing::debug!("Background refreshing cache for key: {key}");
        match self.read_bigquery_bytes(&query).await {
            Ok(result) => {
                if let Err(e) = self.cache_put(&key, &result).await {
                    tracing::error!("Background refresh: cache_put failed: {e}");
                }
            }
            Err(e) => {
                tracing::error!("Background refresh for key {key} failed: {e}");
                let _: Result<(), _> = conn.del(&bg_key).await;
                return;
            }
        }
        let _: Result<(), _> = conn.del(&bg_key).await;
    }

    // ── Top-level query logic ─────────────────────────────────────────────────

    async fn query_inner(
        self: Arc<Self>,
        query: String,
        max_age: Option<i64>,
    ) -> Result<(QueryResult, DateTime<Utc>), BQRedisError> {
        let key = self.query_key(&query);
        if let Some((cached, time, needs_refresh)) = self.cache_get(&key, max_age).await? {
            tracing::debug!("Using cached result for key: {key}");
            if needs_refresh {
                let inner = Arc::clone(&self);
                let q = query.clone();
                let k = key.clone();
                tokio::spawn(async move { inner.background_refresh(q, k).await });
            }
            return Ok((cached, time));
        }
        tracing::debug!("Requesting new execution for key: {key}");
        let result = self
            .clone()
            .get_or_submit(query, key)
            .await
            .map_err(|e| (*e).clone())?;
        Ok(((*result).clone(), result.query_time))
    }

    async fn clear_cache_inner(&self) -> Result<(), BQRedisError> {
        tracing::debug!("Beginning cache clear");
        let mut conn = self.redis_conn().await?;
        let keys: Vec<String> = conn
            .keys(format!("{}*", self.config.redis_key_prefix))
            .await
            .map_err(|e| BQRedisError::Redis(e.to_string()))?;
        let count = keys.len();
        if !keys.is_empty() {
            conn.del::<Vec<String>, ()>(keys)
                .await
                .map_err(|e| BQRedisError::Redis(e.to_string()))?;
        }
        // Also clear in-flight requests.
        if let Ok(mut inflight) = self.inflight.lock() {
            inflight.clear();
        }
        tracing::info!("Cleared {count} keys from Redis cache");
        Ok(())
    }

}

// ── Python class ─────────────────────────────────────────────────────────────

/// A Redis-backed cache for BigQuery queries.
///
/// All I/O (BigQuery and Redis) runs on an owned Tokio runtime without holding
/// the Python GIL. Results are returned as `pyarrow.Table` objects.
///
/// Authentication uses Application Default Credentials (ADC). Run
/// `gcloud auth application-default login` or set
/// `GOOGLE_APPLICATION_CREDENTIALS` before instantiating.
#[pyclass(module = "bqredis")]
pub struct BQRedis {
    rt: Runtime,
    inner: Arc<BQRedisInner>,
}

#[pymethods]
impl BQRedis {
    #[new]
    #[pyo3(signature = (
        redis_url,
        *,
        redis_key_prefix = "bigquery_cache:",
        redis_cache_ttl_sec = 3600,
        redis_background_refresh_ttl_sec = 300,
        max_stream_count = None,
    ))]
    fn new(
        redis_url: &str,
        redis_key_prefix: &str,
        redis_cache_ttl_sec: u64,
        redis_background_refresh_ttl_sec: u64,
        max_stream_count: Option<i32>,
    ) -> PyResult<Self> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        let (bq_client, project_id, conn_manager) = rt
            .block_on(async {
                // Use ADC for the high-level HTTP + gRPC BigQuery client.
                let (config, project_id) = ClientConfig::new_with_auth()
                    .await
                    .map_err(|e| BQRedisError::BigQuery(e.to_string()))?;
                let project_id = project_id.unwrap_or_default();
                let bq_client = Client::new(config)
                    .await
                    .map_err(|e| BQRedisError::BigQuery(e.to_string()))?;

                // Build a separate authenticated gRPC connection pool for the
                // BigQuery Storage Read API.
                let tsp = DefaultTokenSourceProvider::new(
                    google_cloud_auth::project::Config::default()
                        .with_audience("https://bigquerystorage.googleapis.com/")
                        .with_scopes(&[
                            "https://www.googleapis.com/auth/bigquery",
                            "https://www.googleapis.com/auth/cloud-platform",
                        ]),
                )
                .await
                .map_err(|e| BQRedisError::BigQuery(e.to_string()))?;

                let conn_manager = ConnectionManager::new(
                    1,
                    "bigquerystorage.googleapis.com",
                    "https://bigquerystorage.googleapis.com/",
                    &Environment::GoogleCloud(Box::new(tsp)),
                    &ConnectionOptions::default(),
                )
                .await
                .map_err(|e| BQRedisError::BigQuery(format!("gRPC connection error: {e:?}")))?;

                Ok::<_, BQRedisError>((bq_client, project_id, conn_manager))
            })
            .map_err(|e: BQRedisError| PyErr::from(e))?;

        let redis_client =
            redis::Client::open(redis_url).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        let inner = Arc::new(BQRedisInner {
            project_id,
            bq_client,
            conn_manager: Arc::new(conn_manager),
            redis: redis_client,
            config: BQRedisConfig {
                redis_key_prefix: redis_key_prefix.to_string(),
                cache_ttl_sec: redis_cache_ttl_sec,
                background_refresh_ttl_sec: redis_background_refresh_ttl_sec,
                max_stream_count: max_stream_count.unwrap_or(0),
            },
            inflight: Mutex::new(HashMap::new()),
        });

        Ok(BQRedis { rt, inner })
    }

    /// Execute a query, returning a pyarrow.Table (cached result allowed).
    #[pyo3(signature = (query, max_age = None))]
    fn query_sync(
        &self,
        py: Python,
        query: String,
        max_age: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let (result, _time) = self.block_on_query(py, query, max_age)?;
        result.to_pyarrow(py)
    }

    /// Like `query_sync` but also returns the timestamp of when the data was queried.
    #[pyo3(signature = (query, max_age = None))]
    fn query_sync_with_time(
        &self,
        py: Python,
        query: String,
        max_age: Option<i64>,
    ) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
        let (result, time) = self.block_on_query(py, query, max_age)?;
        let table = result.to_pyarrow(py)?;
        let py_time = Self::datetime_to_python(py, time)?;
        Ok((table, py_time))
    }

    /// Return a `concurrent.futures.Future` that resolves to a `pyarrow.Table`.
    #[pyo3(signature = (query, max_age = None))]
    fn query<'py>(
        &self,
        py: Python<'py>,
        query: String,
        max_age: Option<i64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (fut, fut_ref) = Self::make_python_future(py)?;
        let inner = Arc::clone(&self.inner);
        self.rt.spawn(async move {
            let result = inner.query_inner(query, max_age).await;
            Python::attach(|py| {
                match result {
                    Ok((qr, _)) => match qr.to_pyarrow(py) {
                        Ok(tbl) => {
                            let _ = fut_ref.call_method1(py, "set_result", (tbl,));
                        }
                        Err(e) => {
                            let _ = fut_ref.call_method1(py, "set_exception", (e,));
                        }
                    },
                    Err(e) => {
                        let exc = PyRuntimeError::new_err(e.to_string());
                        let _ = fut_ref.call_method1(py, "set_exception", (exc,));
                    }
                }
            });
        });
        Ok(fut)
    }

    /// Return a `concurrent.futures.Future` that resolves to `(pyarrow.Table, datetime)`.
    #[pyo3(signature = (query, max_age = None))]
    fn query_with_time<'py>(
        &self,
        py: Python<'py>,
        query: String,
        max_age: Option<i64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (fut, fut_ref) = Self::make_python_future(py)?;
        let inner = Arc::clone(&self.inner);
        self.rt.spawn(async move {
            let result = inner.query_inner(query, max_age).await;
            Python::attach(|py| {
                match result {
                    Ok((qr, time)) => {
                        let tbl = qr.to_pyarrow(py);
                        let ts = Self::datetime_to_python(py, time);
                        match (tbl, ts) {
                            (Ok(t), Ok(ts)) => {
                                let _ = fut_ref.call_method1(py, "set_result", ((t, ts),));
                            }
                            (Err(e), _) | (_, Err(e)) => {
                                let _ = fut_ref.call_method1(py, "set_exception", (e,));
                            }
                        }
                    }
                    Err(e) => {
                        let exc = PyRuntimeError::new_err(e.to_string());
                        let _ = fut_ref.call_method1(py, "set_exception", (exc,));
                    }
                }
            });
        });
        Ok(fut)
    }

    /// Submit a background task that refreshes the cache for `query`.
    /// Returns a `concurrent.futures.Future[None]`.
    fn submit_background_refresh<'py>(
        &self,
        py: Python<'py>,
        query: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (fut, fut_ref) = Self::make_python_future(py)?;
        let inner = Arc::clone(&self.inner);
        self.rt.spawn(async move {
            let key = inner.query_key(&query);
            inner.background_refresh(query, key).await;
            Python::attach(|py| {
                let _ = fut_ref.call_method1(py, "set_result", (py.None(),));
            });
        });
        Ok(fut)
    }

    /// Clear the Redis cache synchronously.
    fn clear_cache_sync(&self, py: Python) -> PyResult<()> {
        py.detach(|| self.rt.block_on(self.inner.clear_cache_inner()))
            .map_err(PyErr::from)
    }

    /// Clear the Redis cache asynchronously; returns a `concurrent.futures.Future[None]`.
    fn clear_cache<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let (fut, fut_ref) = Self::make_python_future(py)?;
        let inner = Arc::clone(&self.inner);
        self.rt.spawn(async move {
            let result = inner.clear_cache_inner().await;
            Python::attach(|py| match result {
                Ok(()) => {
                    let _ = fut_ref.call_method1(py, "set_result", (py.None(),));
                }
                Err(e) => {
                    let exc = PyRuntimeError::new_err(e.to_string());
                    let _ = fut_ref.call_method1(py, "set_exception", (exc,));
                }
            });
        });
        Ok(fut)
    }
}

// ── Private helpers ───────────────────────────────────────────────────────────

impl BQRedis {
    /// Run `query_inner` synchronously, releasing the GIL while waiting.
    fn block_on_query(
        &self,
        py: Python,
        query: String,
        max_age: Option<i64>,
    ) -> PyResult<(QueryResult, DateTime<Utc>)> {
        let inner = Arc::clone(&self.inner);
        py.detach(move || {
            self.rt
                .block_on(inner.query_inner(query, max_age))
                .map_err(PyErr::from)
        })
    }

    /// Allocate a `concurrent.futures.Future` and return both the bound Python
    /// object and an owned `Py<PyAny>` for use from Rust background tasks.
    fn make_python_future<'py>(py: Python<'py>) -> PyResult<(Bound<'py, PyAny>, Py<PyAny>)> {
        let fut = py
            .import("concurrent.futures")?
            .getattr("Future")?
            .call0()?;
        let owned: Py<PyAny> = fut.clone().unbind();
        Ok((fut, owned))
    }

    /// Convert a `chrono::DateTime<Utc>` to a Python `datetime.datetime`.
    fn datetime_to_python(py: Python, dt: DateTime<Utc>) -> PyResult<Py<PyAny>> {
        let datetime_mod = py.import("datetime")?;
        let timezone = datetime_mod.getattr("timezone")?;
        let utc = timezone.getattr("utc")?;
        let datetime_cls = datetime_mod.getattr("datetime")?;
        Ok(datetime_cls
            .call_method1(
                "fromisoformat",
                (dt.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),),
            )?
            .call_method1("astimezone", (utc,))?
            .into())
    }
}

// ── Module registration ───────────────────────────────────────────────────────

#[pymodule]
fn _bqredis(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<BQRedis>()?;
    Ok(())
}
