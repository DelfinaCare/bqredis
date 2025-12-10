import hashlib
import unittest

import pyarrow as pa
import redis

import bqredis


class TestBQRedisIntegration(unittest.TestCase):
    """This test is not hermetic and requires being authenticated with GCP."""

    def setUp(self):
        super().setUp()
        self.redis = redis.Redis.from_url("redis://localhost:6379")
        self.cache = bqredis.BQRedis(self.redis)
        self.query_str = "SELECT alpha_2_code FROM `bigquery-public-data.country_codes.country_codes` ORDER BY alpha_3_code DESC LIMIT 3"
        self.expected_result = pa.RecordBatch.from_arrays(
            [pa.array(["ZW", "ZM", "ZA"])], names=["alpha_2_code"]
        )
        self.expected_bin_schema = self.expected_result.schema.serialize().to_pybytes()
        # The data could be compressed in different ways, so we will not actually
        # compare binary data directly. We will just ensure that something gets
        # stored in redis, and that the data deserialized correctly.
        # self.expected_bin_data = pa.compress(self.expected_result.serialize(), "zstd").to_pybytes()
        self.query_hash = hashlib.sha256(self.query_str.encode()).hexdigest()

    def tearDown(self):
        self.assertEqual(len(self.cache.inflight_requests), 0)
        self.cache.clear_cache_sync()
        return super().tearDown()

    def assert_not_in_cache(self, key):
        self.assertIsNone(self.redis.get(self.cache.redis_key_prefix + key + ":data"))
        self.assertIsNone(self.redis.get(self.cache.redis_key_prefix + key + ":schema"))

    def assert_in_cache(self, key):
        data: bytes = self.redis.get(self.cache.redis_key_prefix + key + ":data")  # type: ignore
        schema = self.redis.get(self.cache.redis_key_prefix + key + ":schema")
        self.assertIsNotNone(data)
        self.assertEqual(schema, self.expected_bin_schema)
        schema = pa.ipc.read_schema(pa.BufferReader(schema).read_buffer())
        self.assertEqual(pa.ipc.read_record_batch(data, schema), self.expected_result)

    def test_execution_end_to_end(self):
        self.assert_not_in_cache(self.query_hash)
        first_result = self.cache.query_sync(self.query_str)
        self.assertEqual(len(first_result), 3)
        self.assertEqual(first_result, self.expected_result)
        self.assert_in_cache(self.query_hash)
        second_result = self.cache.query_sync(self.query_str)
        self.assertEqual(len(second_result), 3)
        self.assertEqual(second_result, self.expected_result)
        self.assert_in_cache(self.query_hash)

    def test_execution_end_to_end_empty_result(self):
        query_str = "SELECT alpha_2_code FROM `bigquery-public-data.country_codes.country_codes` WHERE alpha_2_code = 'ZZ'"
        query_hash = hashlib.sha256(query_str.encode()).hexdigest()
        self.assert_not_in_cache(query_hash)
        first_result = self.cache.query_sync(query_str)
        self.assertEqual(len(first_result), 0)
        schema = self.redis.get(self.cache.redis_key_prefix + query_hash + ":schema")
        self.assertEqual(schema, self.expected_bin_schema)
        second_result = self.cache.query_sync(query_str)
        self.assertEqual(len(second_result), 0)
        schema = self.redis.get(self.cache.redis_key_prefix + query_hash + ":schema")
        self.assertEqual(schema, self.expected_bin_schema)

    def test_background_execution(self):
        self.assert_not_in_cache(self.query_hash)
        self.cache.submit_background_refresh(self.query_str).result()
        self.assert_in_cache(self.query_hash)
        second_result = self.cache.query_sync(self.query_str)
        self.assertEqual(len(second_result), 3)
        self.assertEqual(second_result, self.expected_result)
        self.assert_in_cache(self.query_hash)

    def test_with_failing_query(self):
        failing_query = "SELECT nonexistent_col FROM `bigquery-public-data.country_codes.country_codes`"
        query_hash = hashlib.sha256(failing_query.encode()).hexdigest()
        failing_result = self.cache.query(failing_query)
        with self.assertRaises(Exception):
            failing_result.result()
        self.assert_not_in_cache(query_hash)

    def test_multiple_queries(self):
        query_str_2 = "SELECT alpha_2_code FROM `bigquery-public-data.country_codes.country_codes` ORDER BY alpha_3_code ASC LIMIT 3"
        result1 = self.cache.query(self.query_str)
        result2 = self.cache.query(query_str_2)
        self.assertEqual(len(result1.result()), 3)
        self.assertEqual(result1.result(), self.expected_result)
        self.assertEqual(len(result2.result()), 3)
        self.assertNotEqual(result2.result(), self.expected_result)


if __name__ == "__main__":
    unittest.main()
