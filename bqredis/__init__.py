"""Redis-backed BigQuery query cache, implemented in Rust.

All BigQuery I/O and threading runs in Rust without holding the Python GIL.
Results are returned as ``pyarrow.Table`` objects.

Authentication uses Application Default Credentials (ADC). Run:
    gcloud auth application-default login
or set GOOGLE_APPLICATION_CREDENTIALS before instantiating BQRedis.
"""

from bqredis._bqredis import BQRedis

__all__ = ["BQRedis"]
