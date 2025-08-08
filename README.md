# bqredis

This library provides functionality to cache calls to bigquery with redis.

The use case which drove creation of this library was a web-based dashboard
which pulled data from bigquery. Often queries were inexplicably slow. Even
worse, a slow loading page would prompt users to refresh, would would saturate
the available threadpools, further slowing the experience.

This is the result: a library which uses redis to cache bigquery results and
prevents more than one inflight request from going out from a given cache
instance at a time. Enough talking, let's cut to an example:

```python
import bqredis

_QUERY = "SELECT * FROM `bigquery-public-data.country_codes.country_codes` ORDER BY alpha_3_code DESC LIMIT 10"

redis_client = redis.Redis.from_url("redis://localhost:6379")
cache = bqredis.BQRedis(redis_client, redis_cache_ttl_sec=300, redis_background_refresh_ttl_sec=5)
# Subsequent calls to this for the next 300 seconds will be cached.
cache.query_sync(_QUERY)
# Start a background refresh for this query.
promise = cache.submit_background_refresh(_QUERY)
promise.result()
# More calls for refreshing the background cache for the next 5 seconds will
# not start.
```

## Return type
By default, this library will parse results as a `pyarrow.RecordBatch`. To
instead use a different format, override the `convert_arrow_to_output` method.
For example, to have results as a `polars.DataFrame` do the following:

```
import polars
import pyarrow

class BQRedisPolars(bqredis.BQRedis):
    def convert_arrow_to_output_format(self, records: pyarrow.RecordBatch) -> polars.DataFrame:
        return polars.from_arrow(records)

pl_cache = BQRedisPolars(redis_client, redis_cache_ttl_sec=300, redis_background_refresh_ttl_sec=5)
pl_cache.query_sync(_QUERY)
```

We use arrow as the underlying format because it is the direct format being
sent by BigQuery. This can be converted with 0-copy to polars as show above.
