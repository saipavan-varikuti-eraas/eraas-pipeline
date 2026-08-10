"""MongoDB connection helper.

Centralizes: URI from .env, TLS certificate (macOS Python needs certifi),
client lifecycle. Every module that needs Mongo imports get_client() from here
rather than constructing its own MongoClient.

In production this is where you'd add:
  - Connection pooling config (maxPoolSize, minPoolSize)
  - Read/write concern settings
  - Retry configuration
  - Monitoring/logging hooks
"""
import os
import certifi
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

_client = None


def get_client():
    """Singleton MongoClient. Reused across the pipeline run."""
    global _client
    if _client is None:
        uri = os.getenv("MONGO_URI")
        if not uri:
            raise RuntimeError(
                "MONGO_URI not set in .env. "
                "Format: mongodb+srv://user:pass@cluster.mongodb.net/")
        _client = MongoClient(uri, tlsCAFile=certifi.where())
    return _client


def get_database(db_name):
    """Get a database handle. Creates the DB on first write (Mongo default)."""
    return get_client()[db_name]


def close():
    """Clean shutdown. Call at end of pipeline run."""
    global _client
    if _client:
        _client.close()
        _client = None