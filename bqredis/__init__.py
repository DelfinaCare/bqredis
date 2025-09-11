"""Implementation of a Redis BigQuery cache."""

import concurrent.futures
import hashlib
import io
import logging
import threading
import typing
import weakref

import pyarrow.ipc
import redis
from google.cloud import bigquery
from google.cloud import bigquery_storage_v1 as bq_storage

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class _QueryResult:
    key: str
    serialized_schema: bytes
    serialized_data: io.BytesIO
    records: typing.Any

    def __init__(self, key: str, serialized_schema: bytes, serialized_data: io.BytesIO):
        self.key = key
        self.serialized_schema = serialized_schema
        self.serialized_data = serialized_data
        self.records = None


class BQRedis:
    """A class to cache BigQuery results in Redis.

    This should be instantiated as a singleton, as it maintains a
    ThreadPoolExecutor. This allows concurrent query execution and caching
    across multiple instances which share a redis connection. Additionally,
    each instance maintains at most one inflight connection per query to
    BigQuery.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        executor: concurrent.futures.ThreadPoolExecutor | None = None,
        bigquery_client: bigquery.Client | None = None,
        bigquery_storage_client: bq_storage.BigQueryReadClient | None = None,
        redis_key_prefix: str = "bigquery_cache:",
        redis_cache_ttl_sec: int = 3600,  # 1 hour
        redis_background_refresh_ttl_sec: int = 300,  # 5 minutes
    ):
        self.bigquery_client = bigquery_client or bigquery.Client()
        self.bigquery_storage_client = (
            bigquery_storage_client or bq_storage.BigQueryReadClient()
        )
        self.redis_client = redis_client
        self.executor = executor or concurrent.futures.ThreadPoolExecutor()
        self.redis_key_prefix = redis_key_prefix
        self.redis_cache_ttl = redis_cache_ttl_sec
        self.redis_background_refresh_ttl = redis_background_refresh_ttl_sec
        self.inflight_requests: weakref.WeakValueDictionary[
            str, concurrent.futures.Future[_QueryResult]
        ] = weakref.WeakValueDictionary()
        self.inflight_requests_lock = threading.Lock()

    def convert_arrow_to_output_format(
        self, records: pyarrow.RecordBatch
    ) -> typing.Any:
        """Convert the pyarrow RecordBatch to the desired output format."""
        # This method should be overridden in subclasses to convert
        # the RecordBatch to the desired output format.
        # The base implementation just returns the RecordBatch itself.
        return records

    def _submit_query(
        self, query: str, key: str
    ) -> concurrent.futures.Future[_QueryResult]:
        """Only have one instance of a specific query inflight at a time."""
        with self.inflight_requests_lock:
            # Atomic grab to extract object because values can disappear at any time
            try:
                inflight_request = self.inflight_requests[key]
            except KeyError:
                logger.debug("Dispatching new inflight request for key: %s", key)
                new_request = self.executor.submit(self._execute_query, query, key)
                self.inflight_requests[key] = new_request
                new_request.add_done_callback(self._cache_put)
                return new_request
        logger.debug("Re-used inflight request for key: %s", key)
        return inflight_request

    def _cache_put(
        self, query_result_future: concurrent.futures.Future[_QueryResult]
    ) -> None:
        """Store the serialized schema and data in Redis."""
        query_result = query_result_future.result()
        pipeline = self.redis_client.pipeline()
        pipeline.set(
            query_result.key + ":schema",
            query_result.serialized_schema,
            ex=self.redis_cache_ttl,
        )
        pipeline.set(
            query_result.key + ":data",
            query_result.serialized_data.getvalue(),
            ex=self.redis_cache_ttl,
        )
        pipeline.execute()
        logger.debug("Cached query result for key: %s", query_result.key)

    def _execute_query(self, query: str, key: str) -> _QueryResult:
        result = self._read_bigquery_bytes(query, key)
        schema = pyarrow.ipc.read_schema(
            pyarrow.BufferReader(result.serialized_schema).read_buffer()
        )
        buffer = result.serialized_data.getbuffer()
        if buffer.nbytes == 0:
            records = pyarrow.RecordBatch.from_pylist([], schema)
        else:
            records = pyarrow.ipc.read_record_batch(buffer, schema)
        result.records = self.convert_arrow_to_output_format(records)
        return result

    # This is its own function for easy mocking.
    def _read_bigquery_bytes(self, query: str, key: str) -> _QueryResult:
        query_job: bigquery.QueryJob = self.bigquery_client.query(query)
        while not query_job.done(reload=False):
            query_job.reload()
        # WHAT ARE YOU DOING? YOU ARE MODIFYING THE DICT WITHOUT THE LOCK????
        # Whoa there - you are right that this is strange - but let me clarify.
        # This is a weakref dictionary, so entries can already be garbage
        # collected. We really need the lock to make sure only one thread
        # at a time tries to create a new inflight request to prevent
        # duplication which could easily saturate all of our worker threads.
        self.inflight_requests.pop(key, None)
        if exc := query_job.exception():
            deleted_count = 0
            try:
                deleted_count = self.redis_client.delete(key + ":background_refresh")  # type: ignore
            finally:
                if deleted_count:
                    logger.info(
                        "Cleared background refresh marker on failure for key: %s", key
                    )
                logger.error("BigQuery job failed for key: %s, error: %s", key, exc)
                raise exc
        read_request = bq_storage.types.ReadSession(
            table=query_job.destination.to_bqstorage(),
            data_format=bq_storage.types.DataFormat.ARROW,
        )
        read_request.read_options.arrow_serialization_options.buffer_compression = (
            bq_storage.types.ArrowSerializationOptions.CompressionCodec.ZSTD  # type: ignore
        )
        session = self.bigquery_storage_client.create_read_session(
            parent=f"projects/{query_job.destination.project}",
            read_session=read_request,
            max_stream_count=1,
        )
        result = _QueryResult(
            key=key,
            serialized_schema=session.arrow_schema.serialized_schema,
            serialized_data=io.BytesIO(),
        )
        page_count = 0
        for stream in session.streams:
            reader = self.bigquery_storage_client.read_rows(stream.name)
            for page in reader:
                page_count += 1
                result.serialized_data.write(
                    page.arrow_record_batch.serialized_record_batch
                )
        logger.debug(
            "Query for key %s returned %d pages in %d streams with a total of %d bytes",
            key,
            page_count,
            len(session.streams),
            result.serialized_data.getbuffer().nbytes,
        )
        return result

    def _background_refresh_cache(self, query: str, key: str) -> None:
        if self.redis_client.get(key + ":background_refresh"):
            logger.info("Background refresh already in progress for query key %s", key)
            return
        logger.debug("Background refreshing cache for query key %s", key)
        submission = self._submit_query(query, key)
        self.redis_client.set(
            key + ":background_refresh", "1", ex=self.redis_background_refresh_ttl
        )
        submission.result()  # Wait for the query to complete before the ref to it expires.

    def submit_background_refresh(self, query: str) -> concurrent.futures.Future[None]:
        """Submit a background task to refresh the cache for a given query."""
        query_hash = hashlib.sha256(query.encode()).hexdigest()
        key = self.redis_key_prefix + query_hash
        return self.executor.submit(self._background_refresh_cache, query, key)

    def _check_redis_cache(self, query: str, key: str, max_age: int | None) -> pyarrow.RecordBatch | None:
        pipeline = self.redis_client.pipeline()
        pipeline.get(key + ":data")
        pipeline.get(key + ":schema")
        pipeline.ttl(key + ":data")
        cached_data: bytes | None
        cached_schema: bytes | None
        ttl: int | None
        cached_data, cached_schema, ttl = pipeline.execute()
        if cached_schema is None or ttl is None:
            return None
        result_age = self.redis_cache_ttl - ttl
        if max_age is None or (max_age != 0 and result_age < max_age):
            if result_age > self.redis_background_refresh_ttl:
                self.executor.submit(self._background_refresh_cache, query, key)
            logger.debug("Using cached result for query key: %s", key)
            schema = pyarrow.ipc.read_schema(
                pyarrow.BufferReader(cached_schema).read_buffer()
            )
            if len(cached_data) == 0:
                return pyarrow.RecordBatch.from_pylist([], schema)
            return pyarrow.ipc.read_record_batch(cached_data, schema)

    def query_sync(self, query: str, max_age: int | None = None) -> pyarrow.RecordBatch:
        """Execute a bigquery allowing cached results."""
        query_hash = hashlib.sha256(query.encode()).hexdigest()
        key = self.redis_key_prefix + query_hash
        cached_records = self._check_redis_cache(query, key, max_age)
        if cached_records is not None:
            return self.convert_arrow_to_output_format(cached_records)
        logger.debug("Requesting new execution for query key %s", key)
        return self._submit_query(query, key).result().records

    def query(
        self, query: str, max_age: int | None = None
    ) -> concurrent.futures.Future[pyarrow.RecordBatch]:
        """Execute a bigquery allowing cached results as a future."""
        return self.executor.submit(self.query_sync, query, max_age)

    def clear_cache_sync(self) -> None:
        """Clear the cache synchronously."""
        with self.inflight_requests_lock:
            self.inflight_requests.clear()
        logger.debug("Beginning cache clear")
        key_count = 0
        for key in self.redis_client.scan_iter(self.redis_key_prefix + "*"):
            self.redis_client.delete(key)
            key_count += 1
        logger.info("Cleared %d keys from Redis cache", key_count)

    def clear_cache(self) -> concurrent.futures.Future[None]:
        """Clear the cache in the background."""
        return self.executor.submit(self.clear_cache_sync)
