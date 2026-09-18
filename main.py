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

import argparse
import dataclasses
import glob
import json
import logging
import multiprocessing
import os
import re
import sys
import time
from logging.handlers import TimedRotatingFileHandler
from threading import Thread
from typing import Any, Dict, List, Optional, Tuple
from xml.etree import ElementTree

import config, storage
import capella_pipeline as cap
from config import ViewConfig
from jenkins import JenkinsClient
from models import JobDoc
from parsing import (
    resolve_capella_platform, resolve_operator_platform,
    is_executor, _os_from_job_name,
    # push mode
    compose_test_name, parse_build_version, get_variants, add_variants_to_name,
)
from processors import (
    ProcessTask,
    ServerProcessor, CapellaProcessor, OperatorProcessor, BuildProcessor, CaoProcessor,
    set_jenkins_client, set_gb_label_map,
    # push mode — reused so the pushed doc matches a polled one field for field
    _lookup_gb_label, _update_skip_count,
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

# ---------------------------------------------------------------------------
# One-shot push mode  —  `python3 main.py push ...`
# ---------------------------------------------------------------------------
#
# WHY THIS EXISTS
# ─────────────────
# test_suite_executor-RTAF cannot be polled, and not because of a bug. Its pipeline is
# a "train": ONE Jenkins build runs N suites sequentially against one shared cluster.
# The poller derives exactly one name per build, and RTAF publishes no Jenkins
# testReport, so a polled RTAF build lands as a single document named after a raw JSON
# map ('debian-rosetta_{"rqg_1.0": "conf/rosetta/rqg.conf"}') carrying 0/0 counts.
# One build must instead become N documents. So RTAF is excluded from the server views
# (config._EXCLUDE_RTAF) and pushes its own result once per suite, from inside the train
# loop in JenkinsFile_Rosetta_Pipeline.
#
# PARITY IS THE WHOLE GAME
# ─────────────────────────
# This re-enters the SAME document assembly the poller uses — the tail of
# ServerProcessor._process_build. The doc key is md5(name-build_id), so any divergence
# in naming does not error, it silently starts a SECOND history for the same job. Every
# non-obvious parity point below is commented where it happens; the full derivation is
# in docs/rtaf_self_report.md.
#
# Deliberately NOT reused from the poller:
#   _setup_logging()      — see _push_setup_logging()
#   set_jenkins_client()  — the push path never calls Jenkins. The only part of the
#                           reused tail that would is _get_claim() (console scraping),
#                           which is skipped: result and counts are already known here.

_PUSH_RESULT_MAP = {
    "PASSED":  "SUCCESS", "PASS": "SUCCESS", "SUCCESS":  "SUCCESS",
    "FAILED":  "FAILURE", "FAIL": "FAILURE", "FAILURE":  "FAILURE",
    "ABORTED": "ABORTED", "UNSTABLE": "UNSTABLE",
}


def _push_setup_logging() -> None:
    """Plain stdout logging — deliberately NOT _setup_logging().

    _setup_logging() installs two TimedRotatingFileHandlers writing collector.log and
    collector_errors.log into the CWD. That is right for a long-lived daemon and wrong
    for a one-shot run on a Jenkins agent, where it would drop rotating files into the
    build workspace and hide the output from the console log.
    """
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
    root.addHandler(handler)


def _sum_junit_counts(logs_dir: str) -> Tuple[int, int, int, int]:
    """Sum every report-*.xml in ONE testrunner log directory.

    RTAF's XUnitTestResult.write() (taf/lib/framework_lib/xunit.py) loops over its
    suites and emits one file PER TEST CLASS — not one per greenboard suite. A single
    conf run routinely produces six, e.g.

        logs/testrunner-26-Jul-31_15-59-06/
            report-...-RosettaAggregationTest.xml
            report-...-RosettaAppHealthTest.xml
            ... four more ...

    so reading a single file would report a sixth of the tests. The whole directory is
    summed instead, which is why the CLI takes --logs-dir and not a file path.

    `failures` and `errors` are incremented TOGETHER for the same failing test (see
    XUnitTestCase.update_results), so they are always equal and summing both would
    double-count. Only `failures` is used.

    Returns (tests, failures, skips, files_read).
    """
    tests = failures = skips = files_read = 0
    for path in sorted(glob.glob(os.path.join(logs_dir, "report-*.xml"))):
        try:
            root = ElementTree.parse(path).getroot()
        except Exception as exc:
            logger.warning("unreadable JUnit XML, skipped: %s (%s)", path, exc)
            continue

        def _attr(name: str) -> int:
            try:
                return int(root.get(name) or 0)
            except (TypeError, ValueError):
                return 0

        tests    += _attr("tests")
        failures += _attr("failures")
        skips    += _attr("skip")      # the attribute is 'skip', not 'skipped'
        files_read += 1
    return tests, failures, skips, files_read


def _push_build_doc(args: Any) -> Optional[JobDoc]:
    """Assemble one greenboard document, mirroring ServerProcessor._process_build."""
    # --- build version ------------------------------------------------------
    # Normalised, never raw: parse_build_version() zero-pads the build number
    # (8.5.0-115 -> 8.5.0-0115) and rejects junk. The poller bails when this is falsy
    # ("if not doc.build: return"); so does this, or the doc lands unfindable.
    build = parse_build_version(args.version)
    if not build:
        logger.error("unusable --version %r — expected X.Y.Z-NNNN", args.version)
        return None

    doc = JobDoc(name=args.job, url=args.url, color=None)
    # doc.url stays the JOB url. _process_build never reassigns job_doc.url, so the
    # stored url is the job's, and the build link is composed downstream from
    # url + build_id. Passing $BUILD_URL here would compose to ".../85/85".
    # doc.color stays None: the poller reads it from the Jenkins job listing, which a
    # push has no equivalent of. to_dict() emits color: null.
    doc.build_id  = args.build
    doc.build     = build
    raw_result    = args.result.strip().upper()
    doc.result    = _PUSH_RESULT_MAP.get(raw_result, raw_result)
    doc.duration  = args.duration_ms
    doc.timestamp = args.timestamp_ms or int(time.time() * 1000)

    # --- counts -------------------------------------------------------------
    tests, failures, skips, files_read = _sum_junit_counts(args.logs_dir)
    if files_read == 0:
        # The suite died before writing any report. Still pushed: the poller keeps
        # executor builds whose totalCount is 0, and a visible 0/0 row is far better
        # than a silently absent suite (the push is now RTAF's ONLY source).
        logger.warning("no report-*.xml under %s — pushing with zero counts", args.logs_dir)
    # Mirrors _process_build exactly on both halves:
    #   total is NET of skips, and doc.skip_count is deliberately left at 0 here.
    # The poller likewise never stores Jenkins' skipCount directly — only
    # _update_skip_count() populates doc.skip_count, from the expected-count
    # reconciliation below.
    doc.total_count = max(tests - skips, 0)
    doc.fail_count  = failures
    logger.info("counts from %d file(s): tests=%d failures=%d skip=%d -> total=%d fail=%d",
                files_read, tests, failures, skips, doc.total_count, doc.fail_count)

    # --- identity -----------------------------------------------------------
    # Case is NOT uniform here, and that is deliberate. resolve_os_and_component()
    # upper-cases os/component onto the doc, while the NAME is composed from the raw,
    # lower-case params. One poller-written doc carries both spellings:
    #     os=DEBIAN  component=ROSETTA  name=debian-rosetta_rqg_1.0...
    # Upper-casing the name would rename every RTAF job and fork its history; lowering
    # os/component would move it out of its board section.
    doc.name = compose_test_name(args.os, args.component, args.subcomponent, args.arch)
    if not doc.name:
        logger.error("could not compose a test name — --component is required")
        return None
    doc.os        = args.os.upper()
    doc.component = args.component.upper()
    # ServerProcessor never sets sub_component (only CapellaProcessor does), so no other
    # server doc carries it. Set knowingly: it is exactly what the field exists for —
    # a mis-keyed run stays repairable offline once Jenkins has aged the build out.
    doc.sub_component = args.subcomponent

    # arch appears TWICE in the poller: once inside the name (handled by
    # compose_test_name above) and again on doc.os. Both, or neither matches.
    if args.arch and args.arch != config.DEFAULT_ARCHITECTURE:
        doc.os = f"{doc.os}-{args.arch}"
    if args.server_type and args.server_type != config.DEFAULT_SERVER_TYPE:
        doc.os = args.server_type.upper()
    # Windows variants collapse to WIN on the server board (executors report
    # "windows22"/"WINDOWS"); without this they split into their own section.
    if args.bucket == "server" and doc.os and doc.os.upper().startswith("WIN"):
        doc.os = "WIN"

    priority = (args.priority or config.P1).upper()
    doc.priority = priority if priority in (config.P0, config.P1, config.P2) else config.P1

    # gb_label — curated board section, keyed on the RAW (component, subcomponent)
    # params exactly as the poller keys it. Absent => the board groups under `component`.
    gb = _lookup_gb_label(args.component, args.subcomponent)
    if gb:
        doc.gb_label = gb

    # --- variants -----------------------------------------------------------
    # get_variants() reads the free-form `parameters` string out of a Jenkins actions
    # array; rebuild the minimal shape it expects rather than duplicating its defaults
    # (bucket_storage=COUCHSTORE, and GSI_type=PLASMA only for GSI components —
    # 'rosetta' is not one, so UNDEFINED). Passing doc.component (upper-case) matches
    # the poller, which calls this after the upper-casing above.
    synth_params = [{"name": "parameters", "value": args.parameters}] if args.parameters else []
    doc.variants     = get_variants(synth_params, doc.component)
    doc.display_name = doc.name
    doc.name         = add_variants_to_name(doc.name, doc.variants)

    # doc.claim stays "": _get_claim() scrapes the Jenkins console, and this path has no
    # Jenkins client by design. The failure reason is in the suite's own logs.
    return doc


def _push_supersede(bucket: str, doc: JobDoc) -> None:
    """Delete docs from OLDER builds resolving to the same (name, build-version).

    The server board's rule is one run per (name, build): a rerun supersedes. The poller
    enforces it by deleting the loser mid-walk (ServerProcessor._process_build), which a
    one-shot push cannot see — it knows only its own build.

    OFF BY DEFAULT (--supersede). It needs the `server` bucket to serve name+build
    without a full scan, which is unverified. See docs/rtaf_self_report.md section 7b.
    storage.query() already swallows and logs query errors, so a missing index degrades
    to "no supersession" rather than a failed push.
    """
    rows = storage.query(
        f"SELECT build_id FROM `{bucket}` WHERE name = $1 AND build = $2 AND build_id < $3",
        doc.name, doc.build, doc.build_id,
    )
    for row in rows:
        try:
            old_bid = int(row["build_id"])
        except (KeyError, TypeError, ValueError):
            continue
        if storage.remove(bucket, storage.make_key(doc.name, old_bid)):
            logger.info("superseded older run of %s: build %d", doc.name, old_bid)


def _push_parse_args(argv: List[str]) -> Any:
    p = argparse.ArgumentParser(
        prog="main.py push",
        description="Push one already-run test suite into greenboard. For jobs that "
                    "cannot be polled because one Jenkins build produces many suites "
                    "(RTAF's train pipeline).",
    )
    p.add_argument("--logs-dir", required=True,
                   help="Directory holding this suite's report-*.xml files, e.g. "
                        "RTAF/logs/testrunner-<ts>/. ALL of them are summed.")
    p.add_argument("--job", required=True, help="Jenkins job name ($JOB_NAME)")
    p.add_argument("--build", required=True, type=int, help="Build number ($BUILD_NUMBER)")
    p.add_argument("--url", required=True,
                   help="Jenkins JOB url ($JOB_URL) — NOT $BUILD_URL. The build link is "
                        "composed downstream from url + build_id.")
    p.add_argument("--os", required=True, help="e.g. debian (case as the param has it)")
    p.add_argument("--component", required=True, help="e.g. rosetta")
    p.add_argument("--subcomponent", required=True, help="the suite name, e.g. rqg_1.0")
    p.add_argument("--version", required=True, help="Server build under test, X.Y.Z-NNNN")
    p.add_argument("--result", required=True,
                   help="PASSED/FAILED (or SUCCESS/FAILURE/ABORTED/UNSTABLE)")
    p.add_argument("--arch", default="", help="CPU arch; only non-default is appended")
    p.add_argument("--server-type", default="", help="Overrides OS when non-default")
    p.add_argument("--parameters", default="",
                   help="Test-runner parameter string, source of the bucket_storage / "
                        "gsi_type variants")
    p.add_argument("--priority", default="", help="P0/P1/P2 (default P1)")
    p.add_argument("--bucket", default="server", help="Target bucket (default: server)")
    p.add_argument("--duration-ms", type=int, default=0, help="Suite duration in ms")
    p.add_argument("--timestamp-ms", type=int, default=0,
                   help="Suite start in epoch ms (default: now)")
    p.add_argument("--supersede", action="store_true",
                   help="Delete older builds' docs for the same (name, build-version). "
                        "Off by default — see _push_supersede().")
    p.add_argument("--dry-run", action="store_true",
                   help="Compose and print the document; touch Couchbase not at all.")
    return p.parse_args(argv)


def push_main(argv: List[str]) -> int:
    """Entry point for `main.py push`. Returns a process exit code."""
    args = _push_parse_args(argv)
    _push_setup_logging()

    if not os.path.isdir(args.logs_dir):
        logger.error("--logs-dir is not a directory: %s", args.logs_dir)
        return 2

    if args.dry_run:
        set_gb_label_map({})
    else:
        storage.init_worker(config.COUCHBASE_HOST, config.COUCHBASE_USER,
                            config.COUCHBASE_PASS)
        # The poller loads this once in the parent process and ships it into each worker
        # through _worker_init(); a one-shot run gets neither, so without this every
        # pushed doc would carry no gb_label and group under the raw component forever.
        # Best-effort by design: an unreachable catalog yields {} and costs the grouping,
        # not the push.
        set_gb_label_map(_load_gb_label_map())

    doc = _push_build_doc(args)
    if doc is None:
        return 2

    if args.dry_run:
        logger.info("dry run — no Couchbase writes. key would be %s",
                    storage.make_key(doc.name, doc.build_id))
        print(json.dumps(doc.to_dict(), indent=2, sort_keys=True))
        return 0

    # _update_skip_count reads only view.bucket; reuse the real qe-jenkins1 server view
    # so any future change to it applies here too.
    view = (config.SERVER_VIEW_2 if args.bucket == config.SERVER_VIEW_2.bucket
            else dataclasses.replace(config.SERVER_VIEW_2, bucket=args.bucket))
    _update_skip_count(doc, view)

    doc.triage, doc.bugs = storage.get_triage_and_bugs(
        args.bucket, doc.display_name or doc.name, doc.build or "")

    if args.supersede:
        _push_supersede(args.bucket, doc)

    key = storage.make_key(doc.name, doc.build_id)
    # storage.upsert() already retries 5x internally; upsert is idempotent on this key,
    # so a retried Jenkins stage re-pushing the same suite is harmless.
    if storage.upsert(args.bucket, key, doc.to_dict()):
        logger.info("pushed %s (build %d) -> %s/%s", doc.name, doc.build_id, args.bucket, key)
        return 0

    # Loud, not silent: with the poller exclusion in place this push is the ONLY source
    # of this suite's result. A swallowed failure is a permanently missing row.
    logger.error("PUSH FAILED after retries — %s (build %d) -> %s/%s",
                 doc.name, doc.build_id, args.bucket, key)
    return 1


def _check_parse_args(argv: List[str]) -> Any:
    p = argparse.ArgumentParser(
        prog="main.py check",
        description="Preflight the push path's configuration: are CB_HOST/CB_USER/"
                    "CB_PASS set, does the cluster answer, does auth succeed, does the "
                    "target bucket open? Exits non-zero with a specific reason if not.",
    )
    p.add_argument("--bucket", default="server", help="Bucket the push writes to")
    p.add_argument("--catalog", action="store_true",
                   help="Also probe the gb_label catalog. Reported as a WARNING, never "
                        "an error: the catalog failing only costs the board grouping.")
    return p.parse_args(argv)


def check_main(argv: List[str]) -> int:
    """Entry point for `main.py check`. Returns a process exit code.

    Exists because every way this can be misconfigured is otherwise SILENT and late.
    CB_PASS unset does not raise at import - config.py defaults it to "" - so the first
    sign of trouble is a failed upsert, per suite, at the end of a train that has
    already held a cluster for hours. And the push is wrapped in catchError so that the
    board being down cannot fail a good test run, which means those failures do not even
    turn the build red. Run this BEFORE the run instead.
    """
    args = _check_parse_args(argv)
    _push_setup_logging()

    problems: List[str] = []

    # 1. Settings present. Empty CB_PASS is the classic one: config.py:376-378 defaults
    #    it to "", so the daemon and the push both start happily and fail at the write.
    logger.info("target cluster : couchbase://%s", config.COUCHBASE_HOST)
    logger.info("username       : %s", config.COUCHBASE_USER or "(empty)")
    if not config.COUCHBASE_HOST:
        problems.append("CB_HOST is empty")
    if not config.COUCHBASE_USER:
        problems.append("CB_USER is empty")
    if not config.COUCHBASE_PASS:
        problems.append("CB_PASS is empty - export it, or bind it from a Jenkins "
                        "credential, before running")
    if problems:
        for pr in problems:
            logger.error("MISSING: %s", pr)
        return 2

    # 2. Cluster reachable, credentials accepted, bucket opens. A KV read of a key that
    #    cannot exist is the cheapest probe that exercises all three: DocumentNotFound
    #    means everything worked, anything else is a real problem.
    try:
        storage.init_worker(config.COUCHBASE_HOST, config.COUCHBASE_USER,
                            config.COUCHBASE_PASS)
        col = storage._col(args.bucket)
        try:
            col.get("__greenboard_preflight_probe__")
        except Exception as exc:
            if type(exc).__name__ != "DocumentNotFoundException":
                raise
    except Exception as exc:
        logger.error("CANNOT REACH GREENBOARD: %s: %s", type(exc).__name__, exc)
        logger.error("  checked: couchbase://%s bucket=%s as %s",
                     config.COUCHBASE_HOST, args.bucket, config.COUCHBASE_USER)
        return 2
    logger.info("OK: cluster reachable, auth accepted, bucket '%s' opens", args.bucket)

    # 3. Catalog is advisory only - _load_gb_label_map() already fails soft to {}, which
    #    costs the gb_label grouping and nothing else. Never fail the build for it.
    if args.catalog:
        gb = _load_gb_label_map()
        if gb:
            logger.info("OK: gb_label catalog reachable (%d entries)", len(gb))
        else:
            logger.warning("WARNING: gb_label catalog at %s returned nothing - pushed "
                           "docs will group under the raw component",
                           config.CATALOG_HOST)
    return 0


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
    # Subcommand, not a flag: argv[1] is the daemon's credentials path and has been
    # since this file existed. "push" can never be a credentials filename, so the
    # existing contract (`main.py` / `main.py credentials.ini`) is untouched.
    if len(sys.argv) > 1 and sys.argv[1] == "push":
        sys.exit(push_main(sys.argv[2:]))
    if len(sys.argv) > 1 and sys.argv[1] == "check":
        sys.exit(check_main(sys.argv[2:]))
    credentials = sys.argv[1] if len(sys.argv) > 1 else "credentials.ini"
    run(credentials)
