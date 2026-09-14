"""Integration tests for bqredis.

Not hermetic - requires:
  - Application Default Credentials (gcloud auth application-default login)
  - A running Redis server at redis://localhost:6379
"""

import hashlib
import unittest

import pyarrow as pa
import redis

import bqredis

REDIS_URL = "redis://localhost:6379"

QUERY = (
    "SELECT alpha_2_code"
    " FROM \`bigquery-public-data.country_codes.country_codes\`"
    " ORDER BY alpha_3_code DESC LIMIT 3"
)


class TestBQRedisIntegration(unittest.TestCase):
    """End-to-end tests that hit BigQuery and Redis."""

    def setUp(self):
        super().setUp()
        self.redis = redis.Redis.from_url(REDIS_URL)
        self.cache = bqredis.BQRedis(REDIS_URL)
        self.query_str = QUERY
        self.query_hash = hashlib.sha256(self.query_str.encode()).hexdigest()
        self.expected_table = pa.table({"alpha_2_code": ["ZW", "ZM", "ZA"]})

    def tearDown(self):
        self.cache.clear_cache_sync()
        return super().tearDown()

    def _key(self, query_hash=None):
        h = query_hash or self.query_hash
        return "bigquery_cache:" + h

    def assert_not_in_cache(self, query_hash=None):
        k = self._key(query_hash)
        self.assertIsNone(self.redis.get(k + ":data"))
        self.assertIsNone(self.redis.get(k + ":schema"))

    def assert_in_cache(self, query_hash=None):
        k = self._key(query_hash)
        self.assertIsNotNone(self.redis.get(k + ":data"))
        self.assertIsNotNone(self.redis.get(k + ":schema"))

    def test_execution_end_to_end(self):
        self.assert_not_in_cache()
        first_result = self.cache.query_sync(self.query_str)
        self.assertEqual(len(first_result), 3)
        self.assertEqual(first_result, self.expected_table)
        self.assert_in_cache()
        second_result = self.cache.query_sync(self.query_str)
        self.assertEqual(len(second_result), 3)
        self.assertEqual(second_result, self.expected_table)
        self.assert_in_cache()

    def test_execution_end_to_end_empty_result(self):
        query_str = (
            "SELECT alpha_2_code"
            " FROM \`bigquery-public-data.country_codes.country_codes\`"
            " WHERE alpha_2_code = 'ZZ'"
        )
        query_hash = hashlib.sha256(query_str.encode()).hexdigest()
        self.assert_not_in_cache(query_hash)
        first_result = self.cache.query_sync(query_str)
        self.assertEqual(len(first_result), 0)
        second_result = self.cache.query_sync(query_str)
        self.assertEqual(len(second_result), 0)

    def test_background_refresh(self):
        self.assert_not_in_cache()
        self.cache.submit_background_refresh(self.query_str).result()
        self.assert_in_cache()
        result = self.cache.query_sync(self.query_str)
        self.assertEqual(len(result), 3)
        self.assertEqual(result, self.expected_table)

    def test_with_failing_query(self):
        failing_query = (
            "SELECT nonexistent_col"
            " FROM \`bigquery-public-data.country_codes.country_codes\`"
        )
        failing_hash = hashlib.sha256(failing_query.encode()).hexdigest()
        with self.assertRaises(Exception):
            self.cache.query(failing_query).result()
        self.assert_not_in_cache(failing_hash)

    def test_stream_count_consistency(self):
        query_str = (
            "SELECT alpha_2_code"
            " FROM \`bigquery-public-data.country_codes.country_codes\`"
        )

        def sorted_col(t):
            return sorted(t.column("alpha_2_code").to_pylist())

        r0 = bqredis.BQRedis(REDIS_URL).query_sync(query_str)
        r1 = bqredis.BQRedis(REDIS_URL, max_stream_count=1).query_sync(query_str)
        r2 = bqredis.BQRedis(REDIS_URL, max_stream_count=2).query_sync(query_str)
        self.assertEqual(sorted_col(r0), sorted_col(r1))
        self.assertEqual(sorted_col(r0), sorted_col(r2))

    def test_multiple_queries(self):
        query2 = (
            "SELECT alpha_2_code"
            " FROM \`bigquery-public-data.country_codes.country_codes\`"
            " ORDER BY alpha_3_code ASC LIMIT 3"
        )
        f1 = self.cache.query(self.query_str)
        f2 = self.cache.query(query2)
        self.assertEqual(len(f1.result()), 3)
        self.assertEqual(f1.result(), self.expected_table)
        self.assertEqual(len(f2.result()), 3)
        self.assertNotEqual(f2.result(), self.expected_table)


if __name__ == "__main__":
    unittest.main()
