"""
Capella pipeline linkage — port of the standalone `pipeline_collector.py`.

WHY THIS EXISTS
───────────────
The legacy Capella board (:4001) was never fed by jinja.py's pollcapella. It was fed by
`pipeline_collector.py`, which walked the Jenkins **PipelineJobs** view and wrote three
collections. That script stopped running on 2026-08-08/09, which is why the Capella
board lost its pipelines, its CP versions, and every job that depends on pipeline
context (cp-cli-runner, the sdk-* family, API_TESTS, SECURITY, TERRAFORM).

The modular collector's CapellaProcessor reproduced only the run docs, so this module
restores the rest, and — importantly — the *identities* the legacy board used:

  * job name      "<provider>_<provider>-<component>-<subcomponent>" for executor builds
                  (the provider really is doubled: pipeline_collector built
                  "<provider>-<component>-<subcomponent>" and then prefixed the provider
                  again). UI/CP-CLI instead get a spec/scenario suffix.
  * cbVersion     from `server_version` (+ `server_build_num`/`cbs_image`), NOT from
                  `version_number` — which is why the legacy build docs are bare
                  releases like "7.6.5". A job whose own params carry no version
                  inherits its UPSTREAM PIPELINE's cbVersion; that is how cp-cli-runner
                  (no version param at all) ended up under 7.6.5 / 8.0.0 on the board.
  * cpVersion     control-plane version: params, else parsed from GIT_BRANCH, else the
                  couchbase-cloud commit SHA at the run timestamp.

THE DISPATCHER INDIRECTION
──────────────────────────
Most jobs name their pipeline directly in `causes.upstreamProject`. A
`test_suite_executor*` build does not: it is launched by `test_suite_dispatcher_cloud`,
and the only link back is its `descriptor` param, which appears in the dispatcher
build's console output. So dispatcher builds are scraped first into a `dispatcher`
collection ({dispatched: [descriptor, ...]}), and executor builds resolve their pipeline
by matching their descriptor against it.
"""
from __future__ import annotations

import logging
import re
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple

import storage
from parsing import get_action

logger = logging.getLogger(__name__)

# Collections live in the capella bucket's default scope.
SCOPE = "_default"
PIPELINE_COLLECTION = "pipeline"
DISPATCHER_COLLECTION = "dispatcher"
JOBS_COLLECTION = "jobs"

GIT_OWNER = "couchbasecloud"
GIT_REPO = "couchbase-cloud"

ENVIRONMENT_PARAMS = ["Environment", "env"]
CP_VERSION_PARAMS = ["pr_commit", "Version", "cp_branch"]
CP_BRANCH_PARAMS = ["GIT_BRANCH"]
CB_VERSION_PARAMS = ["server_version"]
CB_BUILD_PARAMS = ["server_build_num", "cbs_image"]
CLOUD_PROVIDER_PARAMS = ["provider", "Provider", "CLOUD_SERVICE_PROVIDER"]

# Component fallback by job-name token. Deliberately the SMALL capella-specific table
# from pipeline_collector, not the 80-token server FEATURES list: the big list
# false-matches capella job names onto components that never existed on this board
# (a name containing "SANITY" would become component SANITY, and so on).
# "TAF" is intentionally not break-early — a TAF job name often also contains the more
# specific token (V4, SECURITY), and the legacy scan kept looking for it.
COMPONENTS_DICT: Dict[str, str] = {
    "CP-CLI":    "CP-CLI",
    "UI":        "UI",
    "SECURITY":  "SECURITY",
    "V4":        "API_TESTS",
    "TAF":       "FUNCTIONAL",
    "TERRAFORM": "TERRAFORM",
    "VOLUME":    "VOLUME",
    "PERF":      "PERF",
    "SDK":       "SDK",
}

_CB_IMAGE_RE = re.compile(r"\b(\d+(?:\.\d+){2})-?v?(\d+)\b")
_CP_BRANCH_RE = re.compile(r"^[^-]*-[^-]*-(.*)$")
_URL_RE = re.compile(
    r"http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+"
)

DEFAULT_CB_VERSION = "default"


# ---------------------------------------------------------------------------
# GitHub (couchbase-cloud) — control-plane version of record
# ---------------------------------------------------------------------------
# Set once per worker from credentials.ini. The repo section there carries only a
# `password` (a PAT), which is why this uses a token header rather than basic auth —
# the legacy helper asked for a `username` too, got NoOptionError, and silently ended
# up sending EMPTY credentials, so its commit lookups always 401'd.
_github_token: Optional[str] = None


def set_github_token(token: Optional[str]) -> None:
    global _github_token
    _github_token = token


def github_token() -> Optional[str]:
    return _github_token


def load_github_token(credentials_path: str = "credentials.ini") -> Optional[str]:
    import configparser
    cfg = configparser.ConfigParser()
    try:
        cfg.read(credentials_path)
    except Exception:
        return None
    section = f"https://github.com/{GIT_OWNER}/{GIT_REPO}/"
    for name in (section, section.rstrip("/")):
        if cfg.has_section(name):
            return cfg.get(name, "password", fallback=None)
    return None


def fetch_commit_sha(run_date: int, token: Optional[str]
                     ) -> Tuple[Optional[str], Optional[str]]:
    """
    Short SHA (+ URL) of the last couchbase-cloud `main` commit at or before the run.
    Used only when a pipeline pins no explicit control-plane version and names no
    branch. Best-effort: any failure leaves cpVersion as "main" rather than blocking
    collection of the run.
    """
    if not token:
        return None, None
    from datetime import datetime, timezone
    import requests
    try:
        until = datetime.fromtimestamp(run_date / 1000, timezone.utc).isoformat()
        res = requests.get(
            f"https://api.github.com/repos/{GIT_OWNER}/{GIT_REPO}/commits",
            headers={"Authorization": f"token {token}",
                     "Accept": "application/vnd.github.v3+json"},
            params={"until": until, "sha": "main", "per_page": 1},
            timeout=20,
        )
        if res.status_code != 200:
            logger.debug("github commits lookup -> HTTP %s", res.status_code)
            return None, None
        commits = res.json()
        if not commits:
            return None, None
        sha = commits[0]["sha"]
        return sha[:7], f"https://github.com/{GIT_OWNER}/{GIT_REPO}/commit/{sha}"
    except Exception as exc:
        logger.debug("github commits lookup failed: %s", exc)
        return None, None


def get_param(params: Any, names: List[str]) -> Optional[Any]:
    for name in names:
        val = get_action(params, "name", name)
        if val:
            return val
    return None


# ---------------------------------------------------------------------------
# Identity helpers
# ---------------------------------------------------------------------------

def spec_suffix(spec: Optional[str]) -> Optional[str]:
    """
    Job-name suffix for the generic UI / CP-CLI runners, byte-identical to legacy:
    the LAST TWO path segments joined by "_", with a trailing .yaml stripped.

      cypress/e2e/features/provisioned/sanity_01/  -> provisioned_sanity_01
      scenarios/smoke/gcp-smoke3.yaml              -> smoke_gcp-smoke3

    Using only two segments matters: it is what produced the legacy names, so keeping it
    lets new runs land on the same job keys instead of forking every spec.
    """
    if not spec:
        return None
    try:
        parts = str(spec).rstrip("/").split("/")
        suffix = "_".join(parts[-2:])
        if suffix.endswith(".yaml"):
            suffix = suffix[: -len(".yaml")]
        return suffix or None
    except Exception:
        return None


def resolve_provider(params: Any, job_name: str) -> str:
    """Cloud provider: explicit param, else inferred from the scenario/spec or name."""
    provider = get_param(params, CLOUD_PROVIDER_PARAMS)
    if provider:
        return str(provider)
    check = ""
    if "CP-CLI" in job_name.upper():
        spec = get_param(params, ["SPEC", "SCENARIO"])
        if spec:
            check = str(spec).upper()
    else:
        check = job_name.upper()
    for token in ("AWS", "GCP", "AZURE"):
        if token in check:
            return token.lower()
    return "aws"                     # legacy default


def resolve_component(params: Any, job_name: str) -> Optional[str]:
    """`component` param wins; otherwise the job-name token table."""
    component = get_param(params, ["component"])
    if component:
        return str(component)
    # The generic runners must take their component from params only. Their names
    # false-match the table — "test_s(UI)te_executor" hits the UI token — so a build
    # that happened to omit the param would be filed under UI instead of skipped.
    if "test_suite_executor" in job_name or "test_suite_dispatcher" in job_name:
        return None
    found = None
    upper = job_name.upper()
    for token, value in COMPONENTS_DICT.items():
        if token in upper:
            found = value
            if token != "TAF":
                break
    return found


def cb_version_from_params(params: Any) -> str:
    """
    Legacy getCBVersion. Returns DEFAULT_CB_VERSION when the job carries no version —
    the caller is expected to fall back to the upstream pipeline's cbVersion.
    """
    version = get_param(params, CB_VERSION_PARAMS)
    if not version:
        return DEFAULT_CB_VERSION
    build = get_param(params, CB_BUILD_PARAMS)
    if build:
        text = str(build)
        if any(k in text.upper() for k in ("AZURE", "AWS", "GCP")):
            match = _CB_IMAGE_RE.search(text)
            if match:
                return f"{match.group(1)}-{match.group(2)}"
        else:
            return f"{version}-{build}"
    return str(version)


def cp_version_from_params(
    params: Any, run_date: int, github_token: Optional[str] = None,
    fetch_commit: Optional[Any] = None,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Legacy getCPVersion -> (cpVersion, commitUrl).

    Params first; a literal "main" means "whatever was on main at run time", so fall
    back to the GIT_BRANCH suffix and finally to the couchbase-cloud commit SHA at the
    run timestamp. `fetch_commit` is injected so the GitHub call is testable/skippable.
    """
    cp_version = get_param(params, CP_VERSION_PARAMS)
    commit_url = None
    if not cp_version:
        cp_version = "main"
    if cp_version == "main":
        branch = get_param(params, CP_BRANCH_PARAMS)
        if branch:
            match = _CP_BRANCH_RE.search(str(branch))
            if match:
                cp_version = match.group(1)
            commit_url = f"https://github.com/{GIT_OWNER}/{GIT_REPO}/tree/{branch}"
        elif fetch_commit is not None:
            sha, commit_url = fetch_commit(run_date, github_token)
            if sha:
                cp_version = sha
    return cp_version, commit_url


def descriptors_from_console(lines: Any) -> List[str]:
    """
    Every `descriptor=` query param appearing in a dispatcher build's console output.
    These are the exact strings the executor builds carry as their `descriptor` param,
    and the only link from an executor build back to its pipeline.
    """
    found: List[str] = []
    text = lines if isinstance(lines, str) else "\n".join(lines or [])
    for url in _URL_RE.findall(text):
        query = urllib.parse.urlparse(url).query
        values = urllib.parse.parse_qs(query).get("descriptor")
        if values:
            descriptor = urllib.parse.unquote(values[0])
            if descriptor not in found:
                found.append(descriptor)
    return found


# ---------------------------------------------------------------------------
# Document builders
# ---------------------------------------------------------------------------

def doc_id(name: str, build_id: int) -> str:
    return f"{name}_{build_id}"


def store_dispatcher(bucket: str, job_name: str, build_id: int,
                     pipeline: Tuple[Optional[str], Optional[str], Optional[int]],
                     descriptors: List[str]) -> Dict[str, Any]:
    pipeline_job, pipeline_url, pipeline_build = pipeline
    doc = {
        "pipelineJob": pipeline_job,
        "pipelineJobUrl": pipeline_url,
        "pipelineJobID": pipeline_build,
        "dispatched": descriptors,
    }
    storage.upsert_scoped(bucket, SCOPE, DISPATCHER_COLLECTION,
                          doc_id(job_name, build_id), doc)
    return doc


def pipeline_for_descriptor(bucket: str, descriptor: str
                            ) -> Tuple[Optional[str], Optional[str], Optional[int]]:
    """Find the pipeline that dispatched `descriptor` (see module docstring)."""
    rows = storage.query(
        f"SELECT pipelineJob, pipelineJobUrl, pipelineJobID "
        f"FROM `{bucket}`.`{SCOPE}`.`{DISPATCHER_COLLECTION}` WHERE $1 IN dispatched",
        descriptor,
    )
    for row in rows:
        return row.get("pipelineJob"), row.get("pipelineJobUrl"), row.get("pipelineJobID")
    return None, None, None


def upstream_from_causes(actions: Any) -> Tuple[Optional[str], Optional[str], Optional[int]]:
    causes = get_action(actions, "causes")
    return (
        get_action(causes, "upstreamProject"),
        get_action(causes, "upstreamUrl"),
        get_action(causes, "upstreamBuild"),
    )
