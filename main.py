"""
Collector entry point.

Run directly:
    cd /path/to/jinja
    python -m collector.main

Or inside a screen session:
    screen -S collector
    source env/bin/activate
    python -m collector.main
    Ctrl-A, D  (detach)

Environment overrides (all optional — see config.py for defaults):
    CB_HOST, CB_USER, CB_PASS
    UBER_USER, UBER_PASS
    POLL_INTERVAL, WORKER_POOL_SIZE, HTTP_TIMEOUT, CONSOLE_TIMEOUT
"""
from __future__ import annotations

import logging
import multiprocessing
import re
import sys
import time
from logging.handlers import TimedRotatingFileHandler
from threading import Thread
from typing import Any, Dict, List, Optional

import config, storage
import capella_pipeline as cap
from config import ViewConfig
from jenkins import JenkinsClient
from models import JobDoc
from parsing import (
    resolve_capella_platform, resolve_operator_platform,
    is_executor, _os_from_job_name,
)
from processors import (
    ProcessTask,
    ServerProcessor, CapellaProcessor, OperatorProcessor, BuildProcessor, CaoProcessor,
    set_jenkins_client, set_gb_label_map,
)

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def _setup_logging() -> None:
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s %(name)s %(levelname)s: %(message)s")

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(fmt)
    root.addHandler(console_handler)

    file_handler = TimedRotatingFileHandler(
        "collector.log", when="D", backupCount=15
    )
    file_handler.setFormatter(fmt)
    root.addHandler(file_handler)

    error_handler = TimedRotatingFileHandler(
        "collector_errors.log", when="D", backupCount=15
    )
    error_handler.setLevel(logging.WARNING)
    error_handler.setFormatter(fmt)
    root.addHandler(error_handler)


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Worker-process initializer — runs ONCE per pool worker at startup
# ---------------------------------------------------------------------------

def _worker_init(
    cb_host: str, cb_user: str, cb_pass: str,
    credentials_path: str,
    http_timeout: int, console_timeout: int,
    gb_label_map: Dict = None,
) -> None:
    """
    Called by each pool worker on startup.
    Establishes one Couchbase connection and one JenkinsClient per worker.
    All tasks dispatched to that worker reuse these — no per-task reconnection.
    The gb_label_map (loaded once in the main process) is shared in to each worker.
    """
    storage.init_worker(cb_host, cb_user, cb_pass)

    client = JenkinsClient.from_ini(
        path=credentials_path,
        timeout=http_timeout,
        console_timeout=console_timeout,
    )
    set_jenkins_client(client)
    set_gb_label_map(gb_label_map or {})
    # Capella control-plane versions come from the couchbase-cloud repo when a pipeline
    # pins no explicit version; the PAT lives in the same credentials.ini.
    cap.set_github_token(cap.load_github_token(credentials_path))


def _load_gb_label_map() -> Dict:
    """Load {(component, subcomponent): gb_label} from the QE-Test-Suites catalog.

    Best-effort: a missing/unreachable catalog just yields an empty map (docs then
    carry raw component, same as before). Keys lowercased for case-insensitive lookup.
    """
    if not config.CATALOG_HOST:
        return {}
    try:
        from datetime import timedelta
        from couchbase.cluster import Cluster
        from couchbase.options import ClusterOptions, QueryOptions
        from couchbase.auth import PasswordAuthenticator
        cluster = Cluster(
            "couchbase://%s" % config.CATALOG_HOST,
            ClusterOptions(PasswordAuthenticator(config.CATALOG_USER, config.CATALOG_PASS)),
        )
        cluster.wait_until_ready(timedelta(seconds=20))
        stmt = ("SELECT t.component, t.subcomponent, t.gb_label FROM `%s` t "
                "WHERE t.gb_label IS NOT MISSING AND t.gb_label IS NOT NULL"
                % config.CATALOG_BUCKET)
        out: Dict = {}
        for row in cluster.query(stmt, QueryOptions(timeout=timedelta(seconds=60))):
            c, s, g = row.get("component"), row.get("subcomponent"), row.get("gb_label")
            if c and s and g:
                out[(str(c).strip().lower(), str(s).strip().lower())] = g
        logger.info("Loaded %d gb_label override(s) from %s@%s",
                    len(out), config.CATALOG_BUCKET, config.CATALOG_HOST)
        return out
    except Exception as exc:
        logger.warning("Could not load gb_label map from %s (%s) — proceeding without overrides",
                       config.CATALOG_HOST, exc)
        return {}


# ---------------------------------------------------------------------------
# Pool-map shims (module-level so multiprocessing can pickle them)
# ---------------------------------------------------------------------------

def _run_server(task: ProcessTask) -> str:
    try:
        ServerProcessor().process(task)
    except Exception as exc:
        logger.exception("Unhandled error in server processor: %s", exc)
    return task.job_doc.name


def _run_capella(task: ProcessTask) -> str:
    try:
        CapellaProcessor().process(task)
    except Exception as exc:
        logger.exception("Unhandled error in capella processor: %s", exc)
    return task.job_doc.name


def _run_operator(task: ProcessTask) -> str:
    try:
        OperatorProcessor().process(task)
    except Exception as exc:
        logger.exception("Unhandled error in operator processor: %s", exc)
    return task.job_doc.name


def _run_cao(task: ProcessTask) -> str:
    try:
        CaoProcessor().process(task)
    except Exception as exc:
        logger.exception("Unhandled error in cao processor: %s", exc)
    return task.job_doc.name


def _run_pool(pool: Any, fn: Any, tasks: List[ProcessTask], label: str) -> None:
    """
    Dispatch tasks across the worker pool and log each job as it finishes.
    Uses imap_unordered (order doesn't matter) so progress is reported live
    instead of blocking silently until the whole view completes.
    """
    total = len(tasks)
    if total == 0:
        return
    done = 0
    for name in pool.imap_unordered(fn, tasks):
        done += 1
        logger.info("  [%s %d/%d] done: %s", label, done, total, name)


# ---------------------------------------------------------------------------
# Job discovery helpers
# ---------------------------------------------------------------------------

_JENKINS_MAIN: Optional[JenkinsClient] = None  # used in the main process for discovery


def _jk() -> JenkinsClient:
    return _JENKINS_MAIN  # type: ignore[return-value]


def _is_excluded(view: ViewConfig, job_name: str) -> bool:
    return any(re.search(p, job_name) for p in view.exclude_patterns)


def _filters_pass(view: ViewConfig, job_name: str) -> bool:
    if view.filters and not any(f.upper() in job_name.upper() for f in view.filters):
        return False
    if view.none_filters and any(
        f.upper() in job_name.upper() and "P2P" not in job_name.upper()
        for f in view.none_filters
    ):
        return False
    return True


def _discover_server_jobs(
    view: ViewConfig, already_scraped: Any, capella_urls: List[str]
) -> List[ProcessTask]:
    tasks: List[ProcessTask] = []
    seen_names: set = set()

    # Capella job names to exclude from the server view — fetched ONCE up front,
    # not per job. Guards against get_json() returning None (failed/404 fetch).
    capella_names: set = set()
    for cu in capella_urls:
        cdata = _jk().get_json(cu, {"depth": 0, "tree": "jobs[name]"})
        if cdata and cdata.get("jobs"):
            capella_names.update(j["name"] for j in cdata["jobs"] if j.get("name"))

    for url in view.urls:
        data = _jk().get_json(url, {"depth": 0, "tree": "jobs[name,url,color]"})
        if not data or not data.get("jobs"):
            continue
        for job in data["jobs"]:
            name = job["name"]
            if name in seen_names:
                continue
            if _is_excluded(view, name) or not _filters_pass(view, name):
                logger.debug("Skipping %s (excluded/filtered)", name)
                continue
            # exclude jobs that belong to the capella view
            if name in capella_names:
                continue
            # Discovery gate — mirrors the old collector's pollTest: only walk a job
            # that is EITHER an executor OR whose name resolves to a known OS. Personal/
            # dev Jenkins projects (py3_kushagra_*, etc.) carry no platform token in the
            # name, so they are skipped HERE — we never fetch their build history at all
            # (the old collector skipped them the same way). The per-build component gate
            # in ServerProcessor remains the backstop for anything that slips past.
            if not is_executor(name) and _os_from_job_name(name, view) is None:
                logger.debug("Skipping %s (no OS in name, not an executor)", name)
                continue
            seen_names.add(name)
            # os/component are left None here — resolved from build params in the processor
            doc = JobDoc(name=name, url=job["url"], color=job.get("color"))
            tasks.append(ProcessTask(doc, view, already_scraped))
    # Process test_suite_executor first — it holds nearly all real test data,
    # so the valuable docs land early instead of after every noise job.
    tasks.sort(key=lambda t: 0 if is_executor(t.job_doc.name) else 1)
    return tasks


def _discover_capella_jobs(view: ViewConfig, already_scraped: Any) -> List[ProcessTask]:
    tasks: List[ProcessTask] = []
    seen_names: set = set()
    for url in view.urls:
        data = _jk().get_json(url, {"depth": 0, "tree": "jobs[name,url,color]"})
        if not data or not data.get("jobs"):
            continue
        for job in data["jobs"]:
            name = job["name"]
            if name in seen_names or _is_excluded(view, name):
                continue
            seen_names.add(name)
            platform = resolve_capella_platform(name, view)
            doc = JobDoc(name=name, url=job["url"], color=job.get("color"), os=platform)
            tasks.append(ProcessTask(doc, view, already_scraped))
    return tasks


def _discover_operator_jobs(view: ViewConfig, already_scraped: Any) -> List[ProcessTask]:
    tasks: List[ProcessTask] = []
    seen_names: set = set()
    for url in view.urls:
        data = _jk().get_json(url, {"depth": 0, "tree": "jobs[name,url,color]"})
        if not data or not data.get("jobs"):
            continue
        for job in data["jobs"]:
            name = job["name"]
            if name in seen_names or _is_excluded(view, name):
                continue
            platform = resolve_operator_platform(name, view)
            if not platform:
                continue
            seen_names.add(name)
            doc = JobDoc(name=name, url=job["url"], color=job.get("color"), os=platform)
            tasks.append(ProcessTask(doc, view, already_scraped))
    return tasks


def _discover_cao_jobs(view: ViewConfig, already_scraped: Any) -> List[ProcessTask]:
    """CAO's urls point straight at the single executor job — one task per url."""
    tasks: List[ProcessTask] = []
    for url in view.urls:
        data = _jk().get_json(url, {"depth": 0, "tree": "name,color"})
        if data is None:
            continue
        name = data.get("name") or url.rstrip("/").split("/")[-1]
        doc = JobDoc(name=name, url=url, color=data.get("color"), os="")
        tasks.append(ProcessTask(doc, view, already_scraped))
    return tasks


# ---------------------------------------------------------------------------
# Build view polling (no pool — uses threads internally)
# ---------------------------------------------------------------------------

def _poll_build_view(view: ViewConfig) -> None:
    bp = BuildProcessor()
    threads: List[Thread] = []

    for url in view.urls:
        job_data = _jk().get_json(url, {"depth": 0})
        if not job_data:
            continue
        name = job_data["name"]
        for build_entry in job_data.get("builds", []):
            run_data = _jk().get_json(
                build_entry["url"], {"depth": 0, "tree": "runs[url,number]"}
            )
            if not run_data:
                continue
            runs = run_data.get("runs") or [build_entry]
            for run in runs:
                t = Thread(target=bp.process_run, args=(run, name, view))
                t.start()
                threads.append(t)
                if len(threads) >= 10:
                    for t2 in threads:
                        t2.join()
                    threads = []

    for t in threads:
        t.join()


# ---------------------------------------------------------------------------
# Build-info changelog collector (background thread)
# ---------------------------------------------------------------------------

def _collect_build_info_loop(credentials_path: str) -> None:
    client = JenkinsClient.from_ini(credentials_path)
    # Use a dedicated Couchbase connection for this thread
    from couchbase.cluster import Cluster
    from couchbase.options import ClusterOptions
    from couchbase.auth import PasswordAuthenticator
    cluster = Cluster(
        f"couchbase://{config.COUCHBASE_HOST}",
        ClusterOptions(PasswordAuthenticator(config.COUCHBASE_USER, config.COUCHBASE_PASS)),
    )
    col = cluster.bucket("server").default_collection()

    while True:
        time.sleep(600)
        try:
            for url in config.BUILDER_URLS:
                _collect_build_info(client, col, url)
        except Exception as exc:
            logger.exception("Build info collection error: %s", exc)


def _collect_build_info(client: JenkinsClient, col: Any, url: str) -> None:
    import json as _json
    res = client.get_json(url, {"depth": 1, "tree": "builds[number,url]"})
    if not res:
        return
    for b in res.get("builds", []):
        job = client.get_json(b["url"])
        if not job:
            continue
        actions = job["actions"]
        from parsing import extract_params, get_action
        params    = extract_params(actions)
        version   = get_action(params, "name", "VERSION")
        build_no  = get_action(params, "name", "BLD_NUM")
        if not build_no:
            continue
        key = f"{version}-{build_no.zfill(4)}"
        try:
            col.get(key)
            continue  # already collected
        except Exception:
            pass
        if not version or version[:3] == "0.0":
            continue
        try:
            if float(version[:3]) > 4.6:
                cl_url = (f"{config.CHANGE_LOG_URL}?ver={version}"
                          f"&from={int(build_no)-1}&to={build_no}")
                changelog = client.get_json(cl_url, append_api=False)
                if changelog:
                    job = _convert_changelog(changelog, job["timestamp"])
                key = f"{version}-{build_no[1:].zfill(4)}"
        except (ValueError, Exception):
            pass
        retries = 5
        for _ in range(retries):
            try:
                col.upsert(key, job)
                break
            except Exception as exc:
                logger.warning("changelog upsert failed: %s", exc)


def _convert_changelog(doc: Dict, timestamp: int) -> Dict:
    items = []
    for change in doc.get("log", []):
        msg = change["message"]
        idx = msg.find("Change-Id")
        if idx > 0:
            msg = msg[:idx].replace("\n", " ") + msg[idx - 1:]
        items.append({"msg": msg})
    return {"timestamp": timestamp, "changeSet": {"items": items}}


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run(credentials_path: str = "credentials.ini") -> None:
    _setup_logging()
    logger.info("Greenboard collector starting — poll interval %ds, pool size %d",
                config.POLL_INTERVAL_SECONDS, config.WORKER_POOL_SIZE)

    global _JENKINS_MAIN
    _JENKINS_MAIN = JenkinsClient.from_ini(
        credentials_path,
        timeout=config.HTTP_TIMEOUT_SECONDS,
        console_timeout=config.CONSOLE_TIMEOUT_SECONDS,
    )

    # Load the gb_label catalog map in the main process; workers receive a copy.
    # The catalog (QE-Test-Suites) is edited out-of-band — a suite's gb_label is often
    # added AFTER runs have already been collected. If we loaded this only once at
    # startup, a long-running collector would keep stamping the raw component for any
    # suite whose gb_label appeared post-boot, so those jobs stay under the wrong
    # greenboard section forever (this is exactly how rbac_rbac-builtin ended up under
    # RBAC instead of its NSERV gb_label). So refresh it periodically below.
    gb_label_map = _load_gb_label_map()
    last_gb_load = time.monotonic()
    gb_refresh_secs = getattr(config, "GB_LABEL_REFRESH_SECONDS", 1800)

    # Background build-info thread
    t_build = Thread(target=_collect_build_info_loop, args=(credentials_path,), daemon=True)
    t_build.start()

    manager = multiprocessing.Manager()
    # already_scraped: bucket → shared list of "url+bid" strings already stored
    scraped: Dict[str, Any] = {}

    while True:
        # Refresh the gb_label overrides on an interval so catalog edits are picked up
        # without a collector restart. Best-effort: keep the last-good map on failure or
        # an empty result (a fresh Pool is created below, so new workers get the update).
        if time.monotonic() - last_gb_load >= gb_refresh_secs:
            refreshed = _load_gb_label_map()
            if refreshed:
                gb_label_map = refreshed
            last_gb_load = time.monotonic()

        pool_kwargs = dict(
            processes=config.WORKER_POOL_SIZE,
            initializer=_worker_init,
            initargs=(
                config.COUCHBASE_HOST, config.COUCHBASE_USER, config.COUCHBASE_PASS,
                credentials_path,
                config.HTTP_TIMEOUT_SECONDS, config.CONSOLE_TIMEOUT_SECONDS,
                gb_label_map,
            ),
        )

        capella_urls = config.CAPELLA_VIEW.urls  # used to exclude from SERVER_VIEW
        try:
            with multiprocessing.Pool(**pool_kwargs) as pool:
                for view in config.VIEWS:
                    if view.bucket not in scraped:
                        scraped[view.bucket] = manager.list()
                    bucket_scraped = scraped[view.bucket]

                    logger.info("Polling view '%s' (%s)", view.name, view.bucket)

                    if view.bucket == "build":
                        _poll_build_view(view)

                    elif view.bucket == "operator":
                        tasks = _discover_operator_jobs(view, bucket_scraped)
                        logger.info("  %d operator jobs to process", len(tasks))
                        _run_pool(pool, _run_operator, tasks, "operator")

                    elif view.bucket == "cao":
                        tasks = _discover_cao_jobs(view, bucket_scraped)
                        logger.info("  %d cao jobs to process", len(tasks))
                        _run_pool(pool, _run_cao, tasks, "cao")

                    elif view.bucket == "capella":
                        tasks = _discover_capella_jobs(view, bucket_scraped)
                        # test_suite_executor* builds find their pipeline only through
                        # the dispatcher's descriptor map, so the dispatcher jobs must
                        # be fully collected BEFORE the rest. pool.map runs a batch
                        # concurrently, so they need their own batch — not just to be
                        # first in one list.
                        disp = [t for t in tasks if "dispatcher" in t.job_doc.name.lower()]
                        rest = [t for t in tasks if t not in disp]
                        logger.info("  %d capella jobs to process (%d dispatcher first)",
                                    len(tasks), len(disp))
                        if disp:
                            _run_pool(pool, _run_capella, disp, "capella-dispatcher")
                        _run_pool(pool, _run_capella, rest, "capella")

                    else:
                        tasks = _discover_server_jobs(view, bucket_scraped, capella_urls)
                        logger.info("  %d server/sg/lite jobs to process", len(tasks))
                        _run_pool(pool, _run_server, tasks, view.name)

        except Exception as exc:
            logger.exception("Error in main poll loop: %s", exc)

        logger.info("Cycle complete — sleeping %ds", config.POLL_INTERVAL_SECONDS)
        time.sleep(config.POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    credentials = sys.argv[1] if len(sys.argv) > 1 else "credentials.ini"
    run(credentials)
