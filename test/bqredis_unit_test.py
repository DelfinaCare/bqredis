import concurrent.futures
import datetime
import hashlib
import io
import threading
import unittest.mock

import fakeredis
import pyarrow as pa

import bqredis


def _query_result(key: str, result: pa.RecordBatch) -> bqredis._QueryResult:
    return bqredis._QueryResult(
        key=key,
        query_time=datetime.datetime.now(datetime.timezone.utc),
        serialized_schema=result.schema.serialize().to_pybytes(),
        serialized_data=io.BytesIO(result.serialize().to_pybytes()),
    )


class MockException(Exception):
    pass


class TestBQRedis(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.executor = concurrent.futures.ThreadPoolExecutor()
        self.redis = fakeredis.FakeStrictRedis()
        self.cache = bqredis.BQRedis(
            self.redis,
            bigquery_client=object(),
            bigquery_storage_client=object(),
            executor=self.executor,
        )
        self.query_str = "SELECT foo FROM bar;"
        self.query_hash = hashlib.sha256(self.query_str.encode()).hexdigest()

    def tearDown(self):
        self.executor.shutdown(wait=False, cancel_futures=True)
        return super().tearDown()

    def mock_execute_query_to_bytes(self) -> unittest.mock.MagicMock:
        return unittest.mock.patch.object(  # type: ignore
            self.cache, "_read_bigquery_bytes"
        )

    def test_check_cache(self):
        key = self.cache.redis_key_prefix + self.query_hash
        records = pa.RecordBatch.from_arrays(
            [pa.array(["ZW", "ZM", "ZA"])], names=["alpha_2_code"]
        )
        mock_result = _query_result(key, records)
        # With no value set, should not find anything
        with unittest.mock.patch.object(self.cache.executor, "submit") as mock_submit:
            self.assertEqual(
                self.cache._check_redis_cache(self.query_str, key, None), (None, None)
            )
            self.assertEqual(mock_submit.call_count, 0)
            self.assertEqual(
                self.cache._check_redis_cache(self.query_str, key, 10), (None, None)
            )
            self.assertEqual(mock_submit.call_count, 0)
            self.assertEqual(
                self.cache._check_redis_cache(self.query_str, key, 0), (None, None)
            )
            self.assertEqual(mock_submit.call_count, 0)
        ft = concurrent.futures.Future()
        ft.set_result(mock_result)
        self.cache._cache_put(ft)
        with unittest.mock.patch.object(self.cache.executor, "submit") as mock_submit:
            # A fresh result should get returned, with no cache refresh
            self.assertEqual(
                self.cache._check_redis_cache(self.query_str, key, None),
                (records, mock_result.query_time),
            )
            self.assertEqual(mock_submit.call_count, 0)
            self.assertEqual(
                self.cache._check_redis_cache(self.query_str, key, 1),
                (records, mock_result.query_time),
            )
            self.assertEqual(mock_submit.call_count, 0)

            # Data is close to expiring - only 10 seconds left. Its age is now mocked as 3600 - 10
            about_to_expire = datetime.datetime.now(
                datetime.timezone.utc
            ) - datetime.timedelta(seconds=3590)
            self.cache.redis_client.set(
                key + ":query_time", about_to_expire.isoformat()
            )
            self.assertEqual(
                self.cache._check_redis_cache(self.query_str, key, None),
                (records, about_to_expire),
            )
            self.assertEqual(mock_submit.call_count, 1)
            # Requesting a fresh result does not find anything, and does not schedule a background
            # refresh because the main execution will be used to fill the cache.
            self.assertEqual(
                self.cache._check_redis_cache(self.query_str, key, 1), (None, None)
            )
            self.assertEqual(mock_submit.call_count, 1)
            self.assertEqual(
                self.cache._check_redis_cache(self.query_str, key, 0), (None, None)
            )
            self.assertEqual(mock_submit.call_count, 1)

    def test_execution_called_only_once_with_cache(self):
        key = self.cache.redis_key_prefix + self.query_hash
        expected_result = pa.RecordBatch.from_arrays(
            [pa.array(["ZW", "ZM", "ZA"])], names=["alpha_2_code"]
        )
        with self.mock_execute_query_to_bytes() as execution_mock:
            execution_mock.return_value = _query_result(key, expected_result)
            self.assertEqual(execution_mock.call_count, 0)
            result = self.cache.query_sync_with_time(self.query_str)
            self.assertEqual(len(result), 2)
            self.assertEqual(result[0], expected_result)
            self.assertEqual(result[1], execution_mock.return_value.query_time)
            execution_mock.assert_called_once_with(self.query_str, key)
            self.assertEqual(execution_mock.call_count, 1)
            second_result = self.cache.query_sync(self.query_str)
            self.assertEqual(second_result, expected_result)
            self.assertEqual(execution_mock.call_count, 1)

    def test_execution_called_only_once_with_cache_empty_result(self):
        key = self.cache.redis_key_prefix + self.query_hash
        returned_bytes = _query_result(
            key, pa.RecordBatch.from_arrays([pa.array([])], names=["alpha_2_code"])
        )
        # Sometimes, we actually just end up with an empty set of bytes.
        returned_bytes.serialized_data = io.BytesIO(b"")
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

    def test_execution_failure(self):
        key = self.cache.redis_key_prefix + self.query_hash
        with self.mock_execute_query_to_bytes() as execution_mock:
            execution_mock.side_effect = MockException("BigQuery error")
            self.assertEqual(execution_mock.call_count, 0)
            with self.assertRaises(MockException):
                self.cache.query(self.query_str).result(timeout=1)
            execution_mock.assert_called_once_with(self.query_str, key)
            self.assertEqual(execution_mock.call_count, 1)
            with self.assertRaises(MockException):
                self.cache.query(self.query_str).result(timeout=1)
            self.assertEqual(execution_mock.call_count, 2)

    def test_executor_exhaustion(self):
        self.executor.shutdown(wait=False)
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self.cache.executor = self.executor
        key1 = self.cache.redis_key_prefix + self.query_hash
        query_str2 = "SELECT baz from qux;"
        key2 = (
            self.cache.redis_key_prefix
            + hashlib.sha256(query_str2.encode()).hexdigest()
        )

        expected_result1 = pa.RecordBatch.from_arrays(
            [pa.array(["ZW", "ZM", "ZA"])], names=["alpha_2_code"]
        )
        expected_result2 = pa.RecordBatch.from_arrays(
            [pa.array(["ZA", "ZM", "ZW"])], names=["alpha_2_code"]
        )
        block = threading.Lock()

        def _mock_query_executor(query: str, key: str) -> bqredis._QueryResult:
            nonlocal block
            with block:
                if key == key1:
                    return _query_result(key1, expected_result1)
                if key == key2:
                    return _query_result(key2, expected_result2)
                raise KeyError(key)

        with self.mock_execute_query_to_bytes() as execution_mock:
            block.acquire(timeout=1)
            execution_mock.side_effect = _mock_query_executor
            self.assertEqual(execution_mock.call_count, 0)
            query1 = self.cache.query(self.query_str)
            query2 = self.cache.query(query_str2)
            # Allow the functions to proceed without enough threads in the threadpool
            block.release()
            self.assertEqual(query1.result(timeout=1), expected_result1)
            self.assertEqual(query2.result(timeout=1), expected_result2)
            self.assertEqual(execution_mock.call_count, 2)


if __name__ == "__main__":
    unittest.main()
