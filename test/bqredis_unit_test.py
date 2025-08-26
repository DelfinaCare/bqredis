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

    def mock_execute_query_to_bytes(
        self, key: str, expected_result: pa.RecordBatch
    ) -> unittest.mock.MagicMock:
        mock_query_result = bqredis._QueryResult(
            key=key,
            serialized_schema=expected_result.schema.serialize().to_pybytes(),
            serialized_data=io.BytesIO(expected_result.serialize().to_pybytes()),
        )
        return unittest.mock.patch.object(  # type: ignore
            self.cache, "_read_bigquery_bytes", return_value=mock_query_result
        )

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
