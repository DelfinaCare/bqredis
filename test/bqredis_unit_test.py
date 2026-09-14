"""Unit tests for bqredis.

These tests verify that BQRedis can be imported and that pre-populated Redis
cache data is returned correctly without contacting BigQuery.

Constructing a BQRedis instance requires Application Default Credentials
(ADC) and a running Redis server.  Tests in this file that need a real
BQRedis instance are skipped when credentials or Redis are unavailable.
"""

import datetime
import hashlib
import unittest

import pyarrow as pa
import pyarrow.ipc

import bqredis

REDIS_URL = "redis://localhost:6379"


def _make_ipc_bytes(table: pa.Table) -> tuple[bytes, bytes]:
    """Serialize *table* to Arrow IPC stream bytes (schema, data)."""
    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, table.schema)
    for batch in table.to_batches():
        writer.write_batch(batch)
    writer.close()
    stream_bytes = sink.getvalue().to_pybytes()
    # Schema-only IPC (no batches) for the :schema key
    schema_sink = pa.BufferOutputStream()
    schema_writer = pa.ipc.new_stream(schema_sink, table.schema)
    schema_writer.close()
    return schema_sink.getvalue().to_pybytes(), stream_bytes


def _try_make_cache(redis_url=REDIS_URL):
    try:
        return bqredis.BQRedis(redis_url)
    except Exception:
        return None


class TestBQRedisImport(unittest.TestCase):
    """Tests that do not require GCP or Redis."""

    def test_module_exports_bqredis(self):
        self.assertTrue(hasattr(bqredis, "BQRedis"))

    def test_class_is_callable(self):
        self.assertTrue(callable(bqredis.BQRedis))

    def test_invalid_redis_url_raises(self):
        with self.assertRaises(Exception):
            bqredis.BQRedis("not-a-redis-url")


class TestBQRedisCaching(unittest.TestCase):
    """Tests that exercise the Redis-caching path without calling BigQuery.

    Skipped when a BQRedis instance cannot be constructed (no ADC / no Redis).
    """

    def setUp(self):
        super().setUp()
        self.cache = _try_make_cache()
        if self.cache is None:
            self.skipTest("Could not construct BQRedis (no ADC or no Redis)")
        self.query_str = "SELECT alpha_2_code FROM mock_table"
        self.query_hash = hashlib.sha256(self.query_str.encode()).hexdigest()
        self.key = "bigquery_cache:" + self.query_hash
        import redis as _redis

        self.redis = _redis.Redis.from_url(REDIS_URL)

    def tearDown(self):
        self.cache.clear_cache_sync()
        return super().tearDown()

    def _populate_cache(self, table: pa.Table):
        schema_bytes, stream_bytes = _make_ipc_bytes(table)
        now = datetime.datetime.now(datetime.timezone.utc)
        self.redis.set(self.key + ":schema", schema_bytes)
        self.redis.set(self.key + ":data", stream_bytes)
        self.redis.set(self.key + ":query_time", now.isoformat())

    def test_returns_cached_table(self):
        table = pa.table({"alpha_2_code": ["ZW", "ZM", "ZA"]})
        self._populate_cache(table)
        result = self.cache.query_sync(self.query_str)
        self.assertEqual(result.num_rows, 3)
        self.assertEqual(result.column("alpha_2_code").to_pylist(), ["ZW", "ZM", "ZA"])

    def test_returns_cached_table_with_time(self):
        table = pa.table({"alpha_2_code": ["ZW", "ZM", "ZA"]})
        self._populate_cache(table)
        result, ts = self.cache.query_sync_with_time(self.query_str)
        self.assertEqual(result.num_rows, 3)
        self.assertIsInstance(ts, datetime.datetime)

    def test_clear_cache_removes_entries(self):
        table = pa.table({"x": [1, 2, 3]})
        self._populate_cache(table)
        self.assertIsNotNone(self.redis.get(self.key + ":schema"))
        self.cache.clear_cache_sync()
        self.assertIsNone(self.redis.get(self.key + ":schema"))


if __name__ == "__main__":
    unittest.main()
