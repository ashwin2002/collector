"""
Couchbase storage layer.

THE CONNECTION PROBLEM IN THE OLD CODE
────────────────────────────────────────
The old jinja.py called newClient() inside storeTest / store_cloud / storeOperator,
which ran inside a multiprocessing.Pool.  Each pool task = a new Cluster() object =
a new TCP connection negotiation + auth handshake.  With dozens of jobs running in
parallel that meant dozens of simultaneous Couchbase connections being opened and
torn down every 120-second cycle — expensive and error-prone.

THE FIX
────────
Use multiprocessing.Pool(initializer=init_worker, initargs=(...)).
The initializer runs *once per worker process* when the process starts.
All tasks subsequently dispatched to that worker reuse the same Cluster object
and the same per-bucket Collection handles.  Connection count = pool size (constant),
not number of jobs (unbounded).
"""
from __future__ import annotations

import hashlib
import logging
from typing import Any, Dict, List, Optional

from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator
import couchbase.subdocument as SD

logger = logging.getLogger(__name__)

# Module-level singletons — one set per worker process.
# Never share across processes; multiprocessing gives each process its own copy.
_cluster: Optional[Cluster] = None
_collections: Dict[str, Any] = {}


# ---------------------------------------------------------------------------
# Pool initializer — called once when each worker process starts
# ---------------------------------------------------------------------------

def init_worker(host: str, username: str, password: str) -> None:
    global _cluster, _collections
    _cluster = Cluster(
        f"couchbase://{host}",
        ClusterOptions(PasswordAuthenticator(username, password)),
    )
    _collections = {}
    logger.debug("Worker %d connected to Couchbase @ %s", _worker_pid(), host)


def _worker_pid() -> int:
    import os
    return os.getpid()


# ---------------------------------------------------------------------------
# Collection access
# ---------------------------------------------------------------------------

def _col(bucket: str) -> Any:
    if _cluster is None:
        raise RuntimeError("Storage not initialised — call init_worker() first")
    if bucket not in _collections:
        _collections[bucket] = _cluster.bucket(bucket).default_collection()
    return _collections[bucket]


def _scoped_col(bucket: str, scope: str, collection: str) -> Any:
    """Handle to a specific scope.collection (e.g. capella._default.jobs)."""
    if _cluster is None:
        raise RuntimeError("Storage not initialised — call init_worker() first")
    key = f"{bucket}.{scope}.{collection}"
    if key not in _collections:
        _collections[key] = _cluster.bucket(bucket).scope(scope).collection(collection)
    return _collections[key]


def upsert_scoped(bucket: str, scope: str, collection: str, key: str, doc: Dict[str, Any]) -> bool:
    try:
        _scoped_col(bucket, scope, collection).upsert(key, doc)
        return True
    except Exception as exc:
        logger.warning("upsert_scoped failed %s.%s.%s key=%s: %s", bucket, scope, collection, key, exc)
        return False


def get_scoped(bucket: str, scope: str, collection: str, key: str) -> Optional[Dict[str, Any]]:
    """Read one doc from a specific scope.collection (e.g. capella._default.pipeline)."""
    try:
        return _scoped_col(bucket, scope, collection).get(key).content_as[dict]
    except Exception:
        return None


def query(statement: str, *positional: Any) -> List[Dict[str, Any]]:
    """
    Run a N1QL query on the worker's cluster handle.

    Needed by the Capella pipeline linkage: a test_suite_executor build names its
    dispatcher only through its `descriptor` param, so the owning pipeline is found by
    matching that descriptor against the `dispatched` array of the dispatcher docs.
    That is an inherently query-shaped lookup (no derivable document key).
    """
    if _cluster is None:
        raise RuntimeError("Storage not initialised — call init_worker() first")
    try:
        from couchbase.options import QueryOptions
        opts = QueryOptions(positional_parameters=list(positional)) if positional else None
        res = _cluster.query(statement, opts) if opts else _cluster.query(statement)
        return list(res.rows())
    except Exception as exc:
        logger.warning("query failed (%s): %s", statement[:90], exc)
        return []


# ---------------------------------------------------------------------------
# Key helpers
# ---------------------------------------------------------------------------

def make_key(name: str, build_id: int, suffix: str = "") -> str:
    return hashlib.md5(f"{name}-{build_id}{suffix}".encode()).hexdigest()


# ---------------------------------------------------------------------------
# CRUD operations
# ---------------------------------------------------------------------------

def upsert(bucket: str, key: str, doc: Dict[str, Any], retries: int = 5) -> bool:
    col = _col(bucket)
    for attempt in range(1, retries + 1):
        try:
            col.upsert(key, doc)
            return True
        except Exception as exc:
            logger.warning("upsert attempt %d/%d failed key=%s: %s",
                           attempt, retries, key, exc)
    logger.error("upsert permanently failed key=%s bucket=%s", key, bucket)
    return False


def remove(bucket: str, key: str) -> bool:
    try:
        _col(bucket).remove(key)
        return True
    except Exception:
        return False


def get(bucket: str, key: str) -> Optional[Dict[str, Any]]:
    try:
        return _col(bucket).get(key).value
    except Exception:
        return None


def lookup_subdoc(bucket: str, doc_key: str, path: str) -> Optional[Any]:
    try:
        return _col(bucket).lookup_in(doc_key, SD.get(path))[0]
    except Exception:
        return None


# ---------------------------------------------------------------------------
# High-level helpers used by processors
# ---------------------------------------------------------------------------

def get_expected_total_count(bucket: str, view_bucket: str,
                              os_name: str, component: str,
                              job_name: str) -> Optional[int]:
    path = f"{view_bucket}.{os_name}.{component}.{job_name}.totalCount"
    val = lookup_subdoc("greenboard", f"existing_builds_{view_bucket}", path)
    try:
        return int(val) if val is not None else None
    except (TypeError, ValueError):
        return None


def get_triage_and_bugs(
    view_bucket: str, job_name: str, build: str
) -> tuple:
    triage, bugs = "", []
    try:
        major = build.split("-")[0]
        build_num = int(build.split("-")[1])
        key = f"{job_name}_{major}_{view_bucket}"
        doc = get("triage_history", key)
        if doc and build_num >= doc.get("build", 0):
            triage = doc.get("triage", "")
            bugs = doc.get("bugs", [])
    except Exception:
        pass
    return triage, bugs


def purge_disabled_job(job_name: str, builds: List[Dict], bucket: str) -> None:
    if not builds:
        return
    high_bid = builds[0]["number"]
    col = _col(bucket)
    for bid in range(1, high_bid + 1):
        key = hashlib.md5(f"{job_name}-{bid}".encode()).hexdigest()
        try:
            col.remove(key)
        except Exception:
            pass


def write_error(doc_repr: str, path: str = "errors.txt") -> None:
    try:
        with open(path, "a") as f:
            f.write(doc_repr + "\n")
    except Exception:
        pass
