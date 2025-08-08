import hashlib
import io
import unittest.mock

import fakeredis
import pyarrow as pa

import bqredis


class TestBQRedis(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.redis = fakeredis.FakeStrictRedis()
        self.cache = bqredis.BQRedis(
            self.redis, bigquery_client=object(), bigquery_storage_client=object()
        )
        self.query_str = "SELECT foo FROM bar;"
        self.query_hash = hashlib.sha256(self.query_str.encode()).hexdigest()
        self.expected_result = pa.RecordBatch.from_arrays(
            [pa.array(["ZW", "ZM", "ZA"])], names=["alpha_2_code"]
        )
        self.expected_bin_schema = self.expected_result.schema.serialize().to_pybytes()
        self.expected_bin_data = io.BytesIO(
            self.expected_result.serialize().to_pybytes()
        )
        self.mock_query_result = bqredis._QueryResult(
            key=self.cache.redis_key_prefix + self.query_hash,
            serialized_schema=self.expected_bin_schema,
            serialized_data=self.expected_bin_data,
        )
        self.mock_query_result.records = self.expected_result
        self.bigquery_execution_patcher = unittest.mock.patch.object(
            self.cache, "_execute_query_to_bytes", return_value=self.mock_query_result
        )
        self.execution_mock = self.bigquery_execution_patcher.start()

    def tearDown(self):
        self.bigquery_execution_patcher.stop()
        return super().tearDown()

    def test_execution_called_only_once_with_cache(self):
        self.assertEqual(self.execution_mock.call_count, 0)
        result = self.cache.query_sync(self.query_str)
        self.assertEqual(result, self.expected_result)
        self.execution_mock.assert_called_once_with(
            self.query_str, self.cache.redis_key_prefix + self.query_hash
        )
        self.assertEqual(self.execution_mock.call_count, 1)
        second_result = self.cache.query_sync(self.query_str)
        self.assertEqual(second_result, self.expected_result)
        self.assertEqual(self.execution_mock.call_count, 1)


if __name__ == "__main__":
    unittest.main()
