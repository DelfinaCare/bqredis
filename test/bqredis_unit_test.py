import concurrent.futures
import hashlib
import io
import unittest.mock

import fakeredis
import pyarrow as pa

import bqredis

def _query_result(key: str, result: pa.RecordBatch) -> bqredis._QueryResult:
    return bqredis._QueryResult(
            key=key,
            serialized_schema=result.schema.serialize().to_pybytes(),
            serialized_data=io.BytesIO(result.serialize().to_pybytes()),
        )


class TestBQRedis(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.redis = fakeredis.FakeStrictRedis()
        self.cache = bqredis.BQRedis(
            self.redis, bigquery_client=object(), bigquery_storage_client=object()
        )
        self.query_str = "SELECT foo FROM bar;"
        self.query_hash = hashlib.sha256(self.query_str.encode()).hexdigest()

    def mock_execute_query_to_bytes(
        self, key: str, expected_result: pa.RecordBatch
    ) -> unittest.mock.MagicMock:
        mock_query_result = _query_result(key, expected_result)
        return unittest.mock.patch.object(  # type: ignore
            self.cache, "_read_bigquery_bytes", return_value=mock_query_result
        )

    def test_check_cache(self):
        key = self.cache.redis_key_prefix + self.query_hash
        records = pa.RecordBatch.from_arrays(
                [pa.array(["ZW", "ZM", "ZA"])], names=["alpha_2_code"]
            )
        # With no value set, should not find anything
        with unittest.mock.patch.object(self.cache.executor, "submit") as mock_submit:
            self.assertEqual(self.cache._check_redis_cache(self.query_str, key, None), None)
            self.assertEqual(mock_submit.call_count, 0)
            self.assertEqual(self.cache._check_redis_cache(self.query_str, key, 10), None)
            self.assertEqual(mock_submit.call_count, 0)
        ft = concurrent.futures.Future()
        ft.set_result(_query_result(key, records))
        self.cache._cache_put(ft)
        with unittest.mock.patch.object(self.cache.executor, "submit") as mock_submit:
            # A fresh result should get returned, with no cache refresh
            self.assertEqual(self.cache._check_redis_cache(self.query_str, key, None), records)
            self.assertEqual(mock_submit.call_count, 0)
            self.assertEqual(self.cache._check_redis_cache(self.query_str, key, 1), records)
            self.assertEqual(mock_submit.call_count, 0)
            # Data is close to expiring - only 10 seconds left. Its age is now mocked as 3600 - 10
            self.cache.redis_client.expire(key + ":data", 10)
            self.assertEqual(self.cache._check_redis_cache(self.query_str, key, None), records)
            self.assertEqual(mock_submit.call_count, 1)
            # Requesting a fresh result does not find anything, and does not schedule a background
            # refresh because the main execution will be used to fill the cache.
            self.assertEqual(self.cache._check_redis_cache(self.query_str, key, 1), None)
            self.assertEqual(mock_submit.call_count, 1)


    def test_execution_called_only_once_with_cache(self):
        key = self.cache.redis_key_prefix + self.query_hash
        expected_result = pa.RecordBatch.from_arrays(
            [pa.array(["ZW", "ZM", "ZA"])], names=["alpha_2_code"]
        )
        with self.mock_execute_query_to_bytes(key, expected_result) as execution_mock:
            self.assertEqual(execution_mock.call_count, 0)
            result = self.cache.query_sync(self.query_str)
            self.assertEqual(result, expected_result)
            execution_mock.assert_called_once_with(self.query_str, key)
            self.assertEqual(execution_mock.call_count, 1)
            second_result = self.cache.query_sync(self.query_str)
            self.assertEqual(second_result, expected_result)
            self.assertEqual(execution_mock.call_count, 1)

    def test_execution_called_only_once_with_cache_empty_result(self):
        key = self.cache.redis_key_prefix + self.query_hash
        returned_bytes = bqredis._QueryResult(
            key=key,
            serialized_schema=pa.RecordBatch.from_arrays(
                [pa.array(["ZW", "ZM", "ZA"])], names=["alpha_2_code"]
            )
            .schema.serialize()
            .to_pybytes(),
            serialized_data=io.BytesIO(b""),
        )
        with unittest.mock.patch.object(
            self.cache, "_read_bigquery_bytes", return_value=returned_bytes
        ) as execution_mock:
            self.assertEqual(execution_mock.call_count, 0)
            result = self.cache.query_sync(self.query_str)
            self.assertEqual(len(result), 0)
            execution_mock.assert_called_once_with(self.query_str, key)
            self.assertEqual(execution_mock.call_count, 1)
            second_result = self.cache.query_sync(self.query_str)
            self.assertEqual(len(second_result), 0)
            self.assertEqual(execution_mock.call_count, 1)


if __name__ == "__main__":
    unittest.main()
