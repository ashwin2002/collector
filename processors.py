"""
Job processors — one class per product type (Server/Mobile, Capella, Operator, Build).

ARCHITECTURE NOTES
──────────────────
Each processor's process() method is the unit of work dispatched to a pool worker.
The module-level _jenkins and _storage singletons are initialised once per worker
process by main.py's pool initializer — so no per-call connection overhead.

TWO-PASS ELIMINATION
─────────────────────
The old code used a recursive two-pass pattern (first_pass=True then a recursive
call with first_pass=False).  The purpose was:
  Pass 1 (oldest→newest): establish baseline; build the buildHist dedup map.
  Pass 2 (newest→oldest): delete duplicate docs where a newer run supersedes an older.

This is replaced here with a single pass (newest→oldest, the natural Jenkins order)
that deduplicates via buildHist and deletes the old doc when a duplicate is detected.
On first boot there is no behaviour change; on subsequent polls the already_scraped
set guards against re-processing.
"""
from __future__ import annotations

import logging
import re
import time
from typing import Any, Dict, List, Optional, Set, Tuple

import storage
import cao
import capella_pipeline as cap
from config import (
    ViewConfig, P1,
    DEFAULT_ARCHITECTURE, DEFAULT_SERVER_TYPE,
    BUILD_COMPONENTS, MAX_BUILDS_PER_JOB, STOP_AFTER_MISSING_BUILDS,
)
from jenkins import JenkinsClient
from models import JobDoc
from parsing import (
    extract_params, get_action,
    should_skip_collect, should_skip_server_collect,
    is_executor, is_disabled, build_is_finished,
    get_build_and_priority, get_build_from_image,
    resolve_os_and_component, resolve_capella_platform, resolve_operator_platform,
    build_test_name,
    get_claim_from_console, get_claim_from_test_report, linkify_tickets,
    get_servers_from_params, get_servers_from_console,
    get_variants, add_variants_to_name, get_env_from_params,
    caveat_swap_xdcr, caveat_should_skip, caveat_should_skip_mobile,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level singletons — set once per worker process by main.py
# ---------------------------------------------------------------------------

_jenkins: Optional[JenkinsClient] = None

# Curated {(component, subcomponent): gb_label} from the QE-Test-Suites catalog.
# Set once per worker (main loads it; workers receive it via the pool initializer).
# Keys are lowercased for case-insensitive lookup against raw Jenkins params.
_gb_label_map: Dict[Tuple[str, str], str] = {}


def set_jenkins_client(client: JenkinsClient) -> None:
    global _jenkins
    _jenkins = client


def set_gb_label_map(mapping: Dict[Tuple[str, str], str]) -> None:
    global _gb_label_map
    _gb_label_map = mapping or {}


def _lookup_gb_label(component: Optional[str], subcomponent: Optional[str]) -> Optional[str]:
    if not component or not subcomponent:
        return None
    return _gb_label_map.get((component.strip().lower(), subcomponent.strip().lower()))


def _jk() -> JenkinsClient:
    if _jenkins is None:
        raise RuntimeError("Jenkins client not set — call set_jenkins_client first")
    return _jenkins


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _get_claim(
    actions: Any, result: str, total_count: int, job_url: str
) -> str:
    analyse_log    = result != "SUCCESS"
    analyse_report = total_count > 0 and result != "SUCCESS"

    # Prefer existing Jenkins claim plugin annotation
    if get_action(actions, "claimed"):
        raw = get_action(actions, "reason") or ""
        return linkify_tickets(raw)

    reason: Optional[str] = None
    if analyse_report:
        report = _jk().get_test_report(job_url)
        reason = get_claim_from_test_report(report)
    if analyse_log and reason is None:
        lines = _jk().stream_console_lines(job_url)
        reason = get_claim_from_console(lines)
    return reason or ""


def _get_servers(params: Any, job_url: str) -> Tuple[List[str], bool]:
    servers, failure = get_servers_from_params(params)
    if servers:
        # still check log for install failures even when servers come from params
        _, log_failure = get_servers_from_console(_jk().stream_console_lines(job_url))
        return servers, failure or log_failure
    return get_servers_from_console(_jk().stream_console_lines(job_url))


def _update_skip_count(doc: JobDoc, view: ViewConfig) -> None:
    if doc.result not in ("FAILURE", "ABORTED") or doc.total_count == 0:
        return
    expected = storage.get_expected_total_count(
        view.bucket, view.bucket, doc.os or "", doc.component or "", doc.display_name or doc.name
    )
    if expected is not None and expected > doc.total_count:
        doc.skip_count = expected - doc.total_count
        doc.total_count = expected


def _apply_additional_fields(doc: JobDoc, view: ViewConfig) -> None:
    for field_key, value_pairs in view.additional_fields.items():
        for pair in value_pairs:
            if pair[0].upper() in doc.name.upper():
                setattr(doc, field_key, pair[1].upper())
                break


def _build_ids_for_job(res: Dict, executor: bool) -> List[int]:
    if executor:
        # Jenkins returns firstBuild/lastBuild = null (not missing) for a job that
        # has never built, so a plain .get(key, {}) still yields None -> crash.
        first = (res.get("firstBuild") or {}).get("number")
        last  = (res.get("lastBuild")  or {}).get("number")
        if not first or not last:
            return []                       # never built — nothing to scrape
        ids = list(range(first, last + 1))
        ids.reverse()                       # newest first
    else:
        ids = [b["number"] for b in res.get("builds", [])]  # Jenkins returns newest first
    # Cap to the newest N builds when configured — avoids walking the entire
    # history of high-volume jobs (test_suite_executor) on a cold run.
    if MAX_BUILDS_PER_JOB > 0:
        ids = ids[:MAX_BUILDS_PER_JOB]
    return ids


def _fetch_test_results(build_url: str) -> Tuple[list, list]:
    """(passed, failed) test-cases from a Jenkins build's testReport."""
    try:
        data = _jk().get_json(f"{build_url}/testReport", {"depth": 0})
    except Exception:
        return [], []
    if not data:
        return [], []
    passed, failed = [], []
    for suite in (data.get("suites") or []):
        sname = suite.get("name")
        for c in (suite.get("cases") or []):
            case = {
                "name": c.get("name"), "className": c.get("className"), "suite": sname,
                "status": c.get("status"), "duration": c.get("duration"),
                "errorDetails": c.get("errorDetails"),
                "errorStackTrace": c.get("errorStackTrace"),
            }
            (passed if c.get("status") == "PASSED" else failed).append(case)
    return passed, failed


# ---------------------------------------------------------------------------
# ProcessTask — the pickleable unit passed to pool.map
# ---------------------------------------------------------------------------

class ProcessTask:
    """Everything a worker needs to process one Jenkins job."""
    __slots__ = ("job_doc", "view", "already_scraped")

    def __init__(
        self,
        job_doc: JobDoc,
        view: ViewConfig,
        already_scraped: Any,          # multiprocessing.Manager list
    ) -> None:
        self.job_doc = job_doc
        self.view = view
        self.already_scraped = already_scraped


# ---------------------------------------------------------------------------
# ServerProcessor  (server / sync_gateway / cblite buckets)
# ---------------------------------------------------------------------------

class ServerProcessor:
    """Processes jobs that land in the server, sync_gateway, or cblite buckets."""

    def process(self, task: ProcessTask) -> None:
        job_doc, view, already_scraped = task.job_doc, task.view, task.already_scraped
        bucket = view.bucket

        url = job_doc.url
        if "sdkbuilds.couchbase" in url:
            url = url.replace("sdkbuilds.couchbase", "sdkbuilds.sc.couchbase")

        res = _jk().get_json(url, {"depth": 0})
        if res is None:
            return

        if is_disabled({"color": job_doc.color}):
            storage.purge_disabled_job(job_doc.name, res.get("builds", []), bucket)
            return

        is_sg    = bucket == "sync_gateway"
        is_lite  = bucket == "cblite"
        executor = is_executor(job_doc.name)
        bids     = _build_ids_for_job(res, executor)
        build_hist: Dict[str, int] = {}
        consecutive_missing = 0

        for bid in bids:
            scraped_key = job_doc.url + str(bid)
            if scraped_key in already_scraped:
                consecutive_missing = 0          # this build exists, just already done
                continue
            status, build_res = self._wait_for_build(url, bid)
            if status == "missing":
                # Confirmed 404 — this build was wiped (past Jenkins' ~5-day
                # retention). ONLY a real 404 counts toward stopping the walk:
                # once enough consecutive builds are truly gone, every older one is
                # gone too. Walking newest→oldest, this is the retention boundary.
                consecutive_missing += 1
                if executor and consecutive_missing >= STOP_AFTER_MISSING_BUILDS:
                    logger.info("%s: %d consecutive missing builds at #%d — past retention, "
                                "stopping walk", job_doc.name, consecutive_missing, bid)
                    break
                continue
            if status != "ok":
                # Transient error (timeout / HTTP error) or a build still running.
                # The build is NOT confirmed gone, so it must not advance — nor be
                # mistaken for — the retention boundary. Skip it (retry next cycle)
                # and keep walking. This is what kept the old collector robust on a
                # slow/busy Jenkins: it never aborted the walk on a transient failure.
                continue
            consecutive_missing = 0
            try:
                self._process_build(
                    bid, build_res, job_doc, url, view, already_scraped,
                    build_hist, is_sg, is_lite, executor,
                )
            except Exception as exc:
                logger.exception("Error processing %s build %d: %s", job_doc.name, bid, exc)

    def _process_build(
        self,
        bid: int,
        res: Dict,
        job_doc: JobDoc,
        url: str,
        view: ViewConfig,
        already_scraped: Any,
        build_hist: Dict[str, int],
        is_sg: bool,
        is_lite: bool,
        executor: bool,
    ) -> None:
        scraped_key = job_doc.url + str(bid)

        doc = job_doc.copy()
        doc.build_id  = bid
        doc.result    = res["result"]
        doc.duration  = res["duration"]
        doc.timestamp = res["timestamp"]

        actions = res["actions"]
        params  = extract_params(actions)

        if should_skip_collect(params):
            return
        if not is_sg and not is_lite and should_skip_server_collect(params):
            return

        total_count = get_action(actions, "totalCount") or 0
        fail_count  = get_action(actions, "failCount")  or 0
        skip_count  = get_action(actions, "skipCount")  or 0

        if total_count == 0:
            if not executor:
                return
        else:
            doc.total_count = total_count - skip_count
            doc.fail_count  = fail_count

        # Resolve OS and component — read directly from params, no translation table.
        doc.os, doc.component = resolve_os_and_component(
            params, doc.name, view, fallback_os=doc.os
        )

        # test_suite_executor is a generic Jenkins job; the real per-test identity
        # lives in its component/subcomponent params. Rebuild the name accordingly
        # ("<os>-<component>_<subcomponent>") so each test is a distinct job — without
        # this every executor build collapses to one name+build key and is deduped away.
        test_name = build_test_name(params, fallback_os=doc.os)
        if test_name:
            doc.name = test_name

        # Arch suffix appended to OS when non-default (e.g. "UBUNTU-ARM64")
        arch = get_action(params, "name", "arch")
        if arch and arch != DEFAULT_ARCHITECTURE and doc.os:
            doc.os = f"{doc.os}-{arch}"

        # server_type overrides OS for non-default deployments (e.g. containers)
        server_type = get_action(params, "name", "server_type")
        if server_type and server_type != DEFAULT_SERVER_TYPE:
            doc.os = server_type.upper()

        # Normalize Windows variants to WIN for the server board. The executor
        # reports the OS as "windows22"/"WINDOWS" etc., but prod/history groups all
        # Windows server jobs under "WIN" — without this they split into a separate
        # WINDOWS22 section that doesn't match prod.
        if view.bucket == "server" and doc.os and doc.os.upper().startswith("WIN"):
            doc.os = "WIN"

        # Gate: a real server result must resolve to a component. The old collector
        # skipped, at discovery, any NON-executor job whose name resolved to no
        # os/component (jinja.py pollTest). The param-based rewrite lost that gate, so
        # personal/dev Jenkins PROJECTS that run ad-hoc without the standard
        # component/OS params (e.g. py3_kushagra_*) leak through as docs with
        # component=null, os=null and show up in greenboard. They never appeared in the
        # old board and must not appear here.
        #   - component is required for ALL server docs: a param-less executor build
        #     would otherwise regress into a junk "test_suite_executor" doc.
        #   - OS is additionally required for non-executors (mirrors the old gate);
        #     executors carry OS in params and are otherwise always allowed.
        # Scoped to the server bucket (sg/cblite/capella resolve identity differently).
        if view.bucket == "server" and (
            not doc.component or (not executor and not doc.os)
        ):
            logger.debug("Skipping %s/%d — unresolved component/os (not a real test job)",
                         job_doc.name, bid)
            return

        # gb_label — curated greenboard display section from the QE-Test-Suites catalog,
        # keyed by the raw (component, subcomponent) params. Overrides the displayed
        # grouping (eventing nests by gb_label || component); raw component stays the truth.
        if view.bucket == "server":
            gb = _lookup_gb_label(get_action(params, "name", "component"),
                                  get_action(params, "name", "subcomponent"))
            if gb:
                doc.gb_label = gb

        doc.servers, install_failure = _get_servers(params, url + str(bid))
        if install_failure:
            doc.result = "INST_FAIL"

        # P2P cblite: handled inline (separate key scheme)
        if doc.component == "P2P":
            self._process_p2p(doc, bid, url, view, params, actions,
                              total_count, build_hist, already_scraped, scraped_key)
            return

        doc.build, doc.priority = get_build_and_priority(params, view.build_param_names)

        # Columnar override
        col_ver = get_action(params, "name", "columnar_version_number")
        if col_ver and col_ver != "0":
            doc.build = col_ver

        if is_sg:
            doc.server_version = get_action(params, "name", "COUCHBASE_SERVER_VERSION") or "Unknown"
        elif is_lite:
            doc.server_version       = get_action(params, "name", "COUCHBASE_SERVER_VERSION") or "Unknown"
            doc.sync_gateway_version = get_action(params, "name", "SYNC_GATEWAY_VERSION") or "Unknown"

        if not doc.build:
            return

        doc.component = caveat_swap_xdcr(doc.build, doc.component or "")
        if caveat_should_skip(doc.build, doc.os or "", doc.component, doc.name):
            return
        if caveat_should_skip_mobile(doc.component, doc.os or ""):
            return

        _apply_additional_fields(doc, view)

        if view.bucket == "server":
            doc.variants    = get_variants(params, doc.component)
            doc.display_name = doc.name
            doc.name         = add_variants_to_name(doc.name, doc.variants)

        doc.claim = _get_claim(actions, doc.result, doc.total_count, url + str(bid))
        _update_skip_count(doc, view)
        doc.triage, doc.bugs = storage.get_triage_and_bugs(
            view.bucket, doc.display_name or doc.name, doc.build or "")

        hist_key = doc.name + "-" + (doc.build or "")
        if hist_key in build_hist:
            old_key = storage.make_key(doc.name, bid)
            storage.remove(view.bucket, old_key)
            return

        key = storage.make_key(doc.name, bid)
        if storage.upsert(view.bucket, key, doc.to_dict()):
            build_hist[hist_key] = bid
            already_scraped.append(scraped_key)
        else:
            storage.write_error(str(doc.to_dict()))

    def _process_p2p(
        self,
        doc: JobDoc,
        bid: int,
        url: str,
        view: ViewConfig,
        params: Any,
        actions: Any,
        total_count: int,
        build_hist: Dict[str, int],
        already_scraped: Any,
        scraped_key: str,
    ) -> None:
        os_arr = doc.name.upper().split("P2P")[1].replace("/", "").split("-")[1:]
        os_match = next((o for o in os_arr if o in view.platforms), None)
        if not os_match:
            return

        viable = [n for n in view.build_param_names if os_match in n.upper()]
        build_val = None
        for param_name in viable:
            raw = get_action(params, "name", param_name)
            if raw:
                from parsing import parse_build_version
                build_val = parse_build_version(raw)
                if build_val:
                    break
        if not build_val or not os_match:
            return

        doc.os    = os_match
        doc.build = build_val
        doc.claim = _get_claim(actions, doc.result, total_count, url + str(bid))
        _update_skip_count(doc, view)
        doc.triage, doc.bugs = storage.get_triage_and_bugs(
            view.bucket, doc.name, doc.build)

        hist_key = doc.name + "-" + doc.build + os_match
        if hist_key in build_hist:
            storage.remove(view.bucket, storage.make_key(doc.name, bid, f"-{os_match}"))
            return

        key = storage.make_key(doc.name, bid, f"-{os_match}")
        if storage.upsert(view.bucket, key, doc.to_dict()):
            build_hist[hist_key] = bid
            already_scraped.append(scraped_key)
        else:
            storage.write_error(str(doc.to_dict()))

    @staticmethod
    def _wait_for_build(url: str, bid: int) -> Tuple[str, Optional[Dict]]:
        """
        Returns (status, res):
            ("ok", dict)       — build finished, ready to process
            ("missing", None)  — confirmed 404: build wiped (past retention)
            ("error", None)    — transient (timeout / HTTP error): build state unknown
            ("unfinished", None) — build exists but is still running / no result yet

        Only "missing" is evidence of the retention boundary. Everything else means
        the build is (or may be) live, so the caller must NOT treat it as the wall
        that ends the walk.
        """
        for _ in range(2):
            status, res = _jk().get_json_status(url + str(bid), {"depth": 0})
            if status == "missing":
                return "missing", None
            if status == "error":
                return "error", None
            if not build_is_finished(res):
                return "unfinished", None
            if res["duration"] == 0:
                logger.debug("Zero duration on %s%d, Jenkins race — retrying", url, bid)
                time.sleep(10)
                continue
            return "ok", res
        return "unfinished", None


# ---------------------------------------------------------------------------
# CapellaProcessor
# ---------------------------------------------------------------------------

class CapellaProcessor:
    """
    Capella jobs, including the pipeline context the legacy board was built around.

    Unlike the server board, a Capella result is only meaningful together with the
    PIPELINE that launched it: the pipeline carries the environment, the control-plane
    version, and — for jobs that pass no version of their own — the cluster version.
    See capella_pipeline for the identity/version rules and the dispatcher indirection.
    """

    _SKIP_NAMES = re.compile(
        r"SERVERLESS|DAPI|NEBULA|ELIXIR", re.IGNORECASE
    )

    def process(self, task: ProcessTask) -> None:
        job_doc, view, already_scraped = task.job_doc, task.view, task.already_scraped

        if self._SKIP_NAMES.search(job_doc.name):
            return

        # Use the canonical Jenkins URL as-is. (It comes back as
        # qe-jenkins1.sc.couchbase.com/job/<name>/, which is reachable.) A previous
        # rewrite to qe-jenkins.sc.couchbase.com/view/Cloud/ pointed at a host that no
        # longer exists -> [Errno 113] No route to host, so nothing got collected.
        url = job_doc.url

        res = _jk().get_json(url, {"depth": 0})
        if res is None:
            return
        if is_disabled({"color": job_doc.color}):
            storage.purge_disabled_job(job_doc.name, res.get("builds", []), view.bucket)
            return

        # A dispatcher build has no test results of its own — it exists only to publish
        # the descriptor -> pipeline mapping that executor builds resolve through. It
        # must therefore be collected BEFORE the executor jobs in the same cycle
        # (main.py orders the capella tasks accordingly).
        if "dispatcher" in job_doc.name.lower():
            self._process_dispatcher_job(job_doc, url, view, res, already_scraped)
            return

        executor = is_executor(job_doc.name)
        bids     = _build_ids_for_job(res, executor)
        build_hist: Dict[str, int] = {}
        consecutive_missing = 0

        # Same retention-boundary walk as the server path, which this processor was
        # missing. For an executor, _build_ids_for_job() expands firstBuild..lastBuild
        # — thousands of ids for test_suite_executor_cloud-TAF while Jenkins keeps only
        # a few hundred — so without the early stop every cycle re-requested builds
        # that are permanently 404, on every poll.
        for bid in bids:
            scraped_key = job_doc.url + str(bid)
            if scraped_key in already_scraped:
                consecutive_missing = 0      # this build exists, just already done
                continue
            status, build_res = ServerProcessor._wait_for_build(url, bid)
            if status == "missing":
                # Only a confirmed 404 counts toward the boundary: walking
                # newest->oldest, once this many builds are truly gone, so is the rest.
                consecutive_missing += 1
                if executor and consecutive_missing >= STOP_AFTER_MISSING_BUILDS:
                    logger.info("%s: %d consecutive missing builds at #%d — past retention, "
                                "stopping walk", job_doc.name, consecutive_missing, bid)
                    break
                continue
            if status != "ok":
                # Transient error or still running — not confirmed gone, so it must
                # not advance the boundary. Retry next cycle.
                continue
            consecutive_missing = 0
            try:
                self._process_build(bid, build_res, job_doc, url, view,
                                    already_scraped, build_hist, executor)
            except Exception as exc:
                logger.exception("Capella error %s build %d: %s", job_doc.name, bid, exc)

    # ------------------------------------------------------------------
    # Dispatcher: descriptor -> pipeline map
    # ------------------------------------------------------------------

    def _process_dispatcher_job(self, job_doc: JobDoc, url: str, view: ViewConfig,
                                res: Dict, already_scraped: Any) -> None:
        for bid in _build_ids_for_job(res, False):
            # Namespaced so a dispatcher build never collides with a test build of the
            # same number in the shared already_scraped list.
            scraped_key = f"dispatcher::{job_doc.url}{bid}"
            if scraped_key in already_scraped:
                continue
            status, build_res = ServerProcessor._wait_for_build(url, bid)
            if status != "ok":
                continue
            pipeline = cap.upstream_from_causes(build_res.get("actions"))
            descriptors = cap.descriptors_from_console(
                _jk().stream_console_lines(f"{url}{bid}")
            )
            if not descriptors:
                continue
            cap.store_dispatcher(view.bucket, job_doc.name, bid, pipeline, descriptors)
            already_scraped.append(scraped_key)
            logger.debug("dispatcher %s/%d -> %d descriptors", job_doc.name, bid,
                         len(descriptors))

    # ------------------------------------------------------------------
    # Pipeline document (cached in the pipeline collection, like legacy)
    # ------------------------------------------------------------------

    def _pipeline_doc(self, view: ViewConfig, root: str, name: Optional[str],
                      build_id: Optional[int], pipeline_url: Optional[str]
                      ) -> Optional[Dict[str, Any]]:
        if not name or not build_id:
            return None
        key = cap.doc_id(name, build_id)
        existing = storage.get_scoped(view.bucket, cap.SCOPE, cap.PIPELINE_COLLECTION, key)
        if existing and existing.get("result"):
            return existing                    # already captured, and it finished

        rel = (pipeline_url or f"job/{name}/").lstrip("/")
        res = _jk().get_json(f"{root}{rel}{build_id}", {"depth": 0})
        if not res:
            return existing
        params   = extract_params(res.get("actions"))
        run_date = res.get("timestamp") or 0
        cp_version, commit_url = cap.cp_version_from_params(
            params, run_date, cap.github_token(), fetch_commit=cap.fetch_commit_sha)
        doc = {
            "name":        name,
            "buildId":     build_id,
            "url":         res.get("url"),
            "environment": cap.get_param(params, cap.ENVIRONMENT_PARAMS) or "sbx",
            "cpVersion":   cp_version,
            "cbVersion":   cap.cb_version_from_params(params),
            "commitUrl":   commit_url,
            "result":      res.get("result"),
            "runDate":     run_date,
            "duration":    res.get("duration"),
            "description": res.get("description"),
            "jobs":        (existing or {}).get("jobs") or {},
        }
        storage.upsert_scoped(view.bucket, cap.SCOPE, cap.PIPELINE_COLLECTION, key, doc)
        return doc

    def _resolve_pipeline(self, view: ViewConfig, root: str, job_name: str,
                          actions: Any, params: Any) -> Optional[Dict[str, Any]]:
        """
        The pipeline that launched this build. test_suite_executor* builds are launched
        by the dispatcher and only carry a `descriptor`, so they resolve through the
        dispatcher collection; everything else names its pipeline in causes.
        """
        if is_executor(job_name):
            descriptor = get_action(params, "name", "descriptor")
            if not descriptor:
                return None
            name, purl, pbid = cap.pipeline_for_descriptor(view.bucket, descriptor)
        else:
            name, purl, pbid = cap.upstream_from_causes(actions)
        if not name or name == job_name:
            return None                        # direct run, or a rebuild of itself
        return self._pipeline_doc(view, root, name, pbid, purl)

    def _process_build(
        self,
        bid: int,
        res: Dict,
        job_doc: JobDoc,
        url: str,
        view: ViewConfig,
        already_scraped: Any,
        build_hist: Dict[str, int],
        executor: bool,
    ) -> None:
        scraped_key = job_doc.url + str(bid)

        doc = job_doc.copy()
        doc.build_id  = bid
        doc.result    = res["result"]
        doc.duration  = res["duration"]
        doc.timestamp = res["timestamp"]

        actions = res["actions"]
        params  = extract_params(actions)

        if should_skip_collect(params) or should_skip_server_collect(params):
            return

        doc.servers, install_failure = _get_servers(params, url + str(bid))
        if install_failure:
            doc.result = "INST_FAIL"

        total_count = get_action(actions, "totalCount") or 0
        fail_count  = get_action(actions, "failCount")  or 0
        skip_count  = get_action(actions, "skipCount")  or 0

        if total_count == 0 and not executor:
            return
        doc.total_count = total_count - skip_count
        doc.fail_count  = fail_count

        # Component is read RAW — deliberately NOT uppercased like the server path —
        # because every Capella component already in capella_gb is the raw param
        # ("smoke_capella", "capella_fusion", "columnar"). Uppercasing forks each one
        # into a second board section that never joins its own history, which is how
        # SMOKE_CAPELLA ended up sitting alongside the legacy smoke_capella.
        # The job-name fallback is the small capella table, not the server FEATURES list.
        doc.os, _ = resolve_os_and_component(
            params, doc.name, view, fallback_os=doc.os
        )
        doc.component     = cap.resolve_component(params, doc.name)
        doc.sub_component = get_action(params, "name", "subcomponent")

        arch = get_action(params, "name", "arch")
        if arch and arch != DEFAULT_ARCHITECTURE and doc.os:
            doc.os = f"{doc.os}-{arch}"

        server_type = get_action(params, "name", "server_type")
        if server_type and server_type != DEFAULT_SERVER_TYPE:
            if server_type.split("_")[0].upper() == "SERVERLESS":
                return
            doc.os = server_type.split("_")[0].upper()

        # Provider is the Capella board's OS axis, and it drives the job name below, so
        # it is resolved the way the legacy pipeline collector did: explicit param, else
        # inferred from the scenario/spec or the job name, else aws.
        provider = cap.resolve_provider(params, doc.name)
        doc.provider = provider.upper()
        # Any os value that isn't one of the view's platforms has to become one. Checking
        # only ("AWS", "PROVISIONED") let other server_type values through as if they
        # were platforms: the sdk-*-integration-capella jobs pass server_type=CAPELLA and
        # landed under a bogus "CAPELLA" platform column.
        if not doc.os or doc.os not in view.platforms:
            doc.os = doc.provider if doc.provider in view.platforms else \
                resolve_capella_platform(doc.name, view)

        # UI / CP-CLI are generic runners: one project, one build per spec/scenario.
        # Without the suffix every spec collapses onto a single job key.
        spec = cap.get_param(params, ["SPEC", "SCENARIO"])
        if spec and "SERVERLESS" in str(spec).upper():
            return
        # Match the runner names precisely. A bare "UI" substring test (what legacy
        # used) also fires on test_s-UI-te_executor, which is harmless only because
        # the executor happens to carry no SPEC/SCENARIO param.
        upper_name = doc.name.upper()
        if "UI-AUTOMATION" in upper_name or "CP-CLI" in upper_name:
            suffix = cap.spec_suffix(spec)
            if suffix:
                doc.name = f"{doc.name}_{suffix}"

        # Same gate as the server path: a real Capella result must resolve to a
        # component, or it is a personal/dev Cloud-view project that never belonged on
        # the board (exactly like py3_kushagra_* on the server board).
        if not doc.component:
            logger.debug("Skipping capella %s/%d — unresolved component (not a real test job)",
                         job_doc.name, bid)
            return

        if doc.os == "SERVERLESS":
            return

        root = url.split("/job/")[0] + "/" if "/job/" in url else url
        pipeline = self._resolve_pipeline(view, root, job_doc.name, actions, params)

        # Per-suite identity for the cloud executor, matching the legacy board:
        # "<provider>-<component>-<subcomponent>", then prefixed with the provider
        # again (the doubling is legacy's, and keeping it is what lets new runs join
        # the per-suite history already folded into capella_gb).
        if executor and doc.component and doc.sub_component:
            doc.name = f"{provider}-{doc.component}-{doc.sub_component}"
        doc.name = f"{provider}_{doc.name}"

        doc.env = (get_env_from_params(params, view.env_param_names) or "").upper() or None
        if not doc.env and pipeline:
            doc.env = str(pipeline.get("environment") or "").upper() or None

        # Control-plane version. The job rarely carries it; the pipeline always does,
        # and it is the pipeline's control plane the job actually ran against.
        if pipeline and pipeline.get("cpVersion"):
            doc.cp_version = pipeline["cpVersion"]
        else:
            for _cp in ("cp_version", "pr_commit", "Version", "cp_branch", "CP_VERSION"):
                _cpv = get_action(params, "name", _cp)
                if _cpv:
                    doc.cp_version = _cpv
                    break

        # Pipeline context, so the board can group runs by the pipeline that produced
        # them instead of showing them as unrelated jobs.
        if pipeline:
            doc.pipeline_job = pipeline.get("name")
            doc.pipeline_url = pipeline.get("url")
            doc.pipeline_id  = cap.doc_id(pipeline.get("name"), pipeline.get("buildId"))

        # cbVersion, in legacy precedence: the job's own server_version, else the
        # pipeline's (this is the ONLY source for cp-cli-runner and the sdk-* jobs,
        # which pass no version at all), else the generic build params, else "default".
        doc.build = cap.cb_version_from_params(params)
        if doc.build == cap.DEFAULT_CB_VERSION and pipeline:
            doc.build = pipeline.get("cbVersion") or cap.DEFAULT_CB_VERSION
        if doc.build == cap.DEFAULT_CB_VERSION:
            fallback, doc.priority = get_build_and_priority(
                params, view.build_param_names, allow_bare=True)
            doc.build = fallback or get_build_from_image(
                params, view.image_param_names) or cap.DEFAULT_CB_VERSION
        if doc.build == cap.DEFAULT_CB_VERSION:
            # Nothing anywhere gives this run a cluster version — the job passes none
            # and its pipeline build has aged out of Jenkins (the pipeline is wiped
            # independently of the job it launched). The Capella board is version-first,
            # so there is nowhere meaningful to file it; both legacy collectors skipped
            # this case rather than inventing a "default" build, which would otherwise
            # show up as a real entry in the board's build dropdown.
            logger.info("Skipping capella %s/%d — no cluster version (pipeline %s gone)",
                        doc.name, bid,
                        (pipeline or {}).get("name") or "unresolved")
            return

        doc.component = caveat_swap_xdcr(doc.build, doc.component or "")
        if caveat_should_skip(doc.build, doc.os or "", doc.component, doc.name):
            return
        if caveat_should_skip_mobile(doc.component, doc.os or ""):
            return

        _apply_additional_fields(doc, view)

        doc.claim = _get_claim(actions, doc.result, doc.total_count, url + str(bid))
        _update_skip_count(doc, view)
        doc.triage, doc.bugs = storage.get_triage_and_bugs(
            view.bucket, doc.display_name or doc.name, doc.build)

        # Capella sets dedup_runs_per_build=False: every run of a suite against a
        # released version is real history the board is meant to show, so a repeat of
        # (name, build) must NOT delete the earlier run. With the collapsed job name
        # this was catastrophic — every suite shared one key, so a cold walk deleted
        # almost every build it had just read.
        hist_key = doc.name + "-" + doc.build
        if view.dedup_runs_per_build and hist_key in build_hist:
            storage.remove(view.bucket, storage.make_key(doc.name, bid))
            return

        key = storage.make_key(doc.name, bid)
        if storage.upsert(view.bucket, key, doc.to_dict()):
            build_hist[hist_key] = bid
            already_scraped.append(scraped_key)
            self._store_job_details(view, doc, bid, url, pipeline, total_count, fail_count)
        else:
            storage.write_error(str(doc.to_dict()))

    # ------------------------------------------------------------------
    # jobs collection — legacy schema (also powers the fail popup)
    # ------------------------------------------------------------------

    def _store_job_details(self, view: ViewConfig, doc: JobDoc, bid: int, url: str,
                           pipeline: Optional[Dict[str, Any]], total_count: int,
                           fail_count: int) -> None:
        """
        Mirror the run into `<bucket>._default.jobs` with the schema the legacy
        pipeline collector used, and register it on its pipeline doc. greenboard reads
        this collection for the per-test fail popup; keeping the legacy shape means one
        endpoint serves live and historical runs alike.
        """
        passed, failed = _fetch_test_results(url + str(bid))
        job_doc_out = {
            "name":           doc.name,
            "buildId":        bid,
            "url":            f"{url}{bid}/",
            "result":         doc.result,
            "totalCount":     total_count,
            "failCount":      fail_count,
            "passCount":      total_count - fail_count,
            "runDate":        doc.timestamp,
            "duration":       doc.duration,
            "provider":       doc.provider,
            "component":      doc.component,
            "passedTests":    passed,
            "failedTests":    failed,
        }
        if pipeline:
            job_doc_out["pipelineID"]     = doc.pipeline_id
            job_doc_out["pipelineJob"]    = pipeline.get("name")
            job_doc_out["pipelineJobUrl"] = pipeline.get("url")
        storage.upsert_scoped(view.bucket, cap.SCOPE, cap.JOBS_COLLECTION,
                              cap.doc_id(doc.name, bid), job_doc_out)

        # Record the run on its pipeline so the board can list a pipeline's jobs.
        if not pipeline or not pipeline.get("name"):
            return
        jobs = pipeline.setdefault("jobs", {})
        builds = jobs.setdefault(doc.name, [])
        if bid not in builds:
            builds.append(bid)
            builds.sort()
            storage.upsert_scoped(
                view.bucket, cap.SCOPE, cap.PIPELINE_COLLECTION,
                cap.doc_id(pipeline["name"], pipeline["buildId"]), pipeline)


# ---------------------------------------------------------------------------
# OperatorProcessor
# ---------------------------------------------------------------------------

class OperatorProcessor:

    def process(self, task: ProcessTask) -> None:
        job_doc, view, already_scraped = task.job_doc, task.view, task.already_scraped

        res = _jk().get_json(job_doc.url, {"depth": 0})
        if res is None:
            return
        if is_disabled({"color": job_doc.color}):
            storage.purge_disabled_job(job_doc.name, res.get("builds", []), view.bucket)
            return

        executor = is_executor(job_doc.name)
        bids     = _build_ids_for_job(res, executor)
        build_hist: Dict[str, int] = {}

        for bid in bids:
            try:
                self._process_build(bid, job_doc, view, already_scraped, build_hist, executor)
            except Exception as exc:
                logger.exception("Operator error %s build %d: %s", job_doc.name, bid, exc)

    def _process_build(
        self,
        bid: int,
        job_doc: JobDoc,
        view: ViewConfig,
        already_scraped: Any,
        build_hist: Dict[str, int],
        executor: bool,
    ) -> None:
        scraped_key = job_doc.url + str(bid)
        if scraped_key in already_scraped:
            return

        status, res = ServerProcessor._wait_for_build(job_doc.url, bid)
        if status != "ok":
            return

        doc = job_doc.copy()
        doc.build_id  = bid
        doc.result    = res["result"]
        doc.duration  = res["duration"]
        doc.timestamp = res["timestamp"]

        actions = res["actions"]
        params  = extract_params(actions)

        skip_custom = get_action(params, "name", "custom")
        if should_skip_collect(params) or skip_custom:
            return

        total_count = get_action(actions, "totalCount") or 0
        fail_count  = get_action(actions, "failCount")  or 0
        skip_count  = get_action(actions, "skipCount")  or 0

        if total_count == 0 and not executor:
            return
        doc.total_count = total_count - skip_count
        doc.fail_count  = fail_count

        k8s_ver = _process_k8s_version(get_action(params, "name", "kubernetes_version"))
        if not k8s_ver:
            return
        doc.component = k8s_ver

        doc.build = _get_operator_build(params, job_doc.url + str(bid))
        if not doc.build:
            return

        doc.priority = P1
        server_img   = get_action(params, "name", "server_image")
        doc.server_version = _process_operator_server_version(server_img)
        doc.name = f"{doc.name}_{doc.server_version}"

        doc.servers, install_failure = _get_servers(params, job_doc.url + str(bid))
        if install_failure:
            doc.result = "INST_FAIL"

        doc.claim = _get_claim(actions, doc.result, doc.total_count, job_doc.url + str(bid))
        _update_skip_count(doc, view)

        hist_key = doc.name + "-" + doc.build
        if hist_key in build_hist:
            storage.remove(view.bucket, storage.make_key(doc.name, bid))
            return

        key = storage.make_key(doc.name, bid)
        if storage.upsert(view.bucket, key, doc.to_dict()):
            build_hist[hist_key] = bid
            already_scraped.append(scraped_key)
            logger.info("Collected operator %s/%d", doc.name, bid)
        else:
            storage.write_error(str(doc.to_dict()))


def _process_k8s_version(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    parts = raw.split(".")
    return f"{parts[0]}.{parts[1]}" if len(parts) >= 2 else None


def _process_operator_server_version(raw: Optional[str]) -> str:
    if not raw:
        return "N/A"
    parts = raw.split(":")
    return parts[1] if len(parts) > 1 else "N/A"


def _get_operator_build(params: Any, console_url: str) -> Optional[str]:
    version = get_action(params, "name", "operator_image")
    if not version:
        return None
    slices = version.split(":")
    if len(slices) < 2:
        return None
    ver = slices[1]
    if "latest" in ver or "-" not in ver:
        import json as _json
        lines = list(_jk().stream_console_lines(console_url))
        for line in lines:
            m = re.search(r'{"version":.*$', line)
            if m:
                try:
                    obj = _json.loads(m.group())
                    parts = obj["version"].split(" ")
                    return f"{parts[0]}-{parts[2][:-1]}"
                except Exception:
                    pass
        return None
    return ver


# ---------------------------------------------------------------------------
# BuildProcessor  (build_sanity_matrix / unit test jobs)
# ---------------------------------------------------------------------------

class BuildProcessor:

    def process_run(self, run: Dict, name: str, view: ViewConfig) -> None:
        job = _jk().get_json(run["url"], {"depth": 0})
        if not job:
            return
        result = job.get("result")
        if not result:
            return

        actions     = job["actions"]
        total_count = get_action(actions, "totalCount") or 0
        fail_count  = get_action(actions, "failCount")  or 0
        if total_count == 0:
            return

        params  = extract_params(actions)
        os_name = (get_action(params, "name", "DISTRO") or
                   job["fullDisplayName"].split()[2].split(",")[0])
        version = get_action(params, "name", "VERSION")
        build_n = (get_action(params, "name", "CURRENT_BUILD_NUMBER") or
                   get_action(params, "name", "BLD_NUM"))
        if not version or not build_n:
            return

        build = f"{version}-{build_n.zfill(4)}"
        old_name = name

        if name == "build_sanity_matrix":
            node_type = job["fullDisplayName"].split()[2].split(",")[1]
            name = f"{os_name}_{name}_{node_type}"
        else:
            name = f"{os_name}_{name}"

        if get_action(params, "name", "UNIT_TEST"):
            name += "_unit"

        # Build jobs have no component param — look up by job name directly.
        comp = BUILD_COMPONENTS.get(old_name)
        if not comp:
            return

        # OS came from DISTRO param / fullDisplayName; normalise via platforms fallback.
        from parsing import _os_from_job_name
        os_val = _os_from_job_name(name, view) or os_name.upper()
        if not os_val:
            return

        if old_name == "build_sanity_matrix" and os_val == "AMZN2":
            os_val = "AWS"

        duration = int(job.get("duration") or 0)

        run_url = run["url"]
        if run_url.endswith(job["id"] + "/"):
            run_url = run_url.rstrip(job["id"] + "/") + "/"

        claim   = _get_claim(actions, result, total_count, run_url + job["id"])
        servers, install_failure = _get_servers(params, run_url + job["id"])
        if install_failure:
            result = "INST_FAIL"

        doc = JobDoc(
            name=name,
            url=run_url,
            os=os_val,
            component=comp,
            build_id=int(job["id"]),
            build=build,
            result=result,
            duration=duration,
            total_count=total_count,
            fail_count=fail_count,
            priority="P0",
            claim=claim,
            servers=servers,
            timestamp=job["timestamp"],
        )

        doc.variants     = get_variants(params, comp)
        doc.display_name = doc.name
        doc.name         = add_variants_to_name(doc.name, doc.variants)

        if version == "4.1.0":
            storage.remove("server", storage.make_key(doc.name, int(job["id"])))
            return

        key = storage.make_key(doc.name, int(job["id"]))
        logger.info("build %s,%s", key, build)
        if not storage.upsert("server", key, doc.to_dict()):
            storage.write_error(str(doc.to_dict()))


# ---------------------------------------------------------------------------
# CaoProcessor  (cao-testrunner-executor -> `cao` bucket)
#
# Unlike the others, CAO has no Jenkins testReport — results live in a
# `results.json` build artifact as a matrix of executions. We fetch that artifact
# per finished build, normalize it via the pure `cao` module, and write one
# "cao_run" doc per execution/combo. gb-v2 groups those into jobs + reruns in its
# snapshot layer (no eventing function needed).
#
# Two artifact paths: the dispatcher rework moved results.json to the workspace
# root, but older builds (and, for a while, new ones too) still publish it under
# `pipeline/`. Try the new path first and fall back — retention keeps ~5 days of
# builds, so both shapes are live at once during the transition.
#
# The artifact cannot describe its own provenance, so we also read the executor's
# build parameters: RERUN / RERUN_FROM_BUILD give the rerun chain the board's
# right sidebar lists, TRIGGER_TYPE / TRIGGER_VERSION say which pivot (CAO or
# server) kicked the run off, and DISPATCHER_BUILD ties sibling combos together.
# ---------------------------------------------------------------------------

# Newest path first — both are tried per build, cheapest-correct order.
CAO_RESULT_PATHS = ("results.json", "pipeline/results.json")

# Executor parameters worth keeping. Anything else (regions, credentials ids,
# kubectl version) is environment noise that would only bloat every doc.
_CAO_PARAMS = ("RERUN", "RERUN_FROM_BUILD", "RERUN_SELECTION", "DISPATCHER_BUILD",
               "TRIGGER_TYPE", "TRIGGER_VERSION", "COMBO_INDEX",
               "COMPONENT", "SUBCOMPONENT", "PLATFORM")


def _cao_build_params(build_url: str) -> Dict[str, str]:
    """Executor build parameters as a flat dict. Empty on any failure."""
    data = _jk().get_json(
        build_url,
        {"tree": "actions[parameters[name,value]]"},
    )
    out: Dict[str, str] = {}
    for action in (data or {}).get("actions") or []:
        for param in action.get("parameters") or []:
            name = param.get("name")
            if name in _CAO_PARAMS:
                out[name] = param.get("value")
    return out


def _cao_meta(res: Optional[Dict], params: Dict[str, str]) -> Dict[str, Any]:
    """Jenkins-side provenance handed to cao.build_cao_docs as `meta`."""
    def _int(v):
        try:
            return int(str(v).strip())
        except Exception:
            return None

    rerun_from = _int(params.get("RERUN_FROM_BUILD"))
    return {
        "trigger": {
            "type": str(params.get("TRIGGER_TYPE") or ""),
            "version": str(params.get("TRIGGER_VERSION") or ""),
        },
        "dispatcher_build": _int(params.get("DISPATCHER_BUILD")),
        "rerun": {
            # Jenkins hands booleans back as the strings "True"/"False".
            "is_rerun": str(params.get("RERUN") or "").strip().lower() == "true",
            "from_build": rerun_from,
            "selection": str(params.get("RERUN_SELECTION") or ""),
        },
        "jenkins_result": (res or {}).get("result") or "",
        "jenkins_timestamp": (res or {}).get("timestamp") or 0,
        "jenkins_duration": (res or {}).get("duration") or 0,
    }

class CaoProcessor:

    def process(self, task: ProcessTask) -> None:
        job_doc, view, already_scraped = task.job_doc, task.view, task.already_scraped

        res = _jk().get_json(job_doc.url, {"depth": 0})
        if res is None:
            return
        if is_disabled({"color": job_doc.color}):
            return

        # cao-testrunner-executor is a single executor-style job: walk newest→oldest.
        for bid in _build_ids_for_job(res, executor=True):
            try:
                self._process_build(bid, job_doc, view, already_scraped)
            except Exception as exc:
                logger.exception("CAO error %s build %d: %s", job_doc.name, bid, exc)

    def _process_build(
        self,
        bid: int,
        job_doc: JobDoc,
        view: ViewConfig,
        already_scraped: Any,
    ) -> None:
        scraped_key = job_doc.url + str(bid)
        if scraped_key in already_scraped:
            return

        status, res = ServerProcessor._wait_for_build(job_doc.url, bid)
        if status == "missing":
            return                         # past retention — nothing here
        if status != "ok":
            return                         # still running / transient — retry next cycle

        build_url = f"{job_doc.url.rstrip('/')}/{bid}"
        data = None
        for path in CAO_RESULT_PATHS:
            data = _jk().get_json(f"{build_url}/artifact/{path}", append_api=False)
            if data:
                break
        if not data:
            # Finished build with no results.json (aborted before publish, etc.).
            # Mark scraped so we don't refetch a permanently-empty build every cycle.
            already_scraped.append(scraped_key)
            return

        meta = _cao_meta(res, _cao_build_params(build_url))
        docs = cao.build_cao_docs(data, job_doc.url, bid, meta=meta)
        wrote = 0
        for d in docs:
            key = storage.make_key(job_doc.name, bid, suffix=f"-{d['combo_index']}")
            if storage.upsert(view.bucket, key, d):
                wrote += 1
            else:
                storage.write_error(str(d))

        if wrote:
            logger.info("Collected CAO %s/%d (%d combo doc%s)",
                        job_doc.name, bid, wrote, "" if wrote == 1 else "s")
        already_scraped.append(scraped_key)
