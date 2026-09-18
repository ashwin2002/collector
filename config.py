"""
Configuration for the Greenboard collector.

COMPONENT / OS RESOLUTION STRATEGY
────────────────────────────────────
Components and OS names are read directly from Jenkins build parameters
("component", "OS"/"os") and stored as-is.  There is no translation table
and no job-name traversal needed.

  Adding a new component:  zero config changes — Jenkins sends the name,
                           we store it, Greenboard displays it.

  platforms dict (below):  only used as a last-resort OS fallback when a
                           job predates explicit "OS" params.  It is small
                           and stable.

Insertion order in the platforms dicts is the fallback match precedence.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from typing import Dict, List


# ---------------------------------------------------------------------------
# ViewConfig
# ---------------------------------------------------------------------------

@dataclass
class ViewConfig:
    name: str
    urls: List[str]
    bucket: str
    platforms: Dict[str, str]          # {token: canonical} — OS fallback only
    # Keep only ONE run per (job, build) within a collection pass, deleting the rest.
    # Correct for the server board (a build is a point in time; a rerun supersedes),
    # WRONG for Capella: Capella re-runs the same suite against a *released* version
    # continuously, so (job, build) legitimately holds a long run history — the board
    # is time-window based and counts runs. The eventing fold already dedups on
    # build_id, so nothing here can create duplicates; removal only destroys history.
    dedup_runs_per_build: bool         = True
    build_param_names: List[str]       = field(default_factory=list)
    image_param_names: List[str]       = field(default_factory=list)
    env_param_names: List[str]         = field(default_factory=list)
    filters: List[str]                 = field(default_factory=list)
    none_filters: List[str]            = field(default_factory=list)
    exclude_patterns: List[re.Pattern] = field(default_factory=list)
    additional_fields: Dict[str, List] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Platforms — used only when a job has no explicit "OS" / "os" param.
# More-specific tokens come before shorter ones that could false-match.
# ---------------------------------------------------------------------------

SERVER_PLATFORMS: Dict[str, str] = {
    "UBUNTU":  "UBUNTU",
    "DEBIAN":  "DEBIAN",
    "CENTOS":  "CENTOS",
    "AMZN2":   "AMZN2",
    "DOCKER":  "DOCKER",
    "CLOUD":   "CLOUD",
    "RHEL":    "RHEL",
    "SUSE":    "SUSE",
    "WIN":     "WIN",
    "MAC":     "MAC",
    "OEL":     "OEL",
}

SG_PLATFORMS: Dict[str, str] = {
    "CEN7":    "CEN7",
    "CEN006":  "CEN006",
    "CENTOS":  "CENTOS",
    "WINDOWS": "WINDOWS",
    "MACOSX":  "MACOSX",
}

CAPELLA_PLATFORMS: Dict[str, str] = {
    "AWS":   "AWS",
    "GCP":   "GCP",
    "AZURE": "AZURE",
}

OPERATOR_PLATFORMS: Dict[str, str] = {
    "GKE": "GKE",
    "AKS": "AKS",
    "EKS": "EKS",
    "OC":  "OC",
}



# ---------------------------------------------------------------------------
# Views
# ---------------------------------------------------------------------------

_EXCLUDE_TMP: List[re.Pattern] = [
    re.compile(r"t[e]?mp(_|-)"),
    re.compile(r"(_|-)t[e]?mp"),
]

# RTAF runs are not wanted on the greenboard server views. Excluded HERE, at
# discovery, rather than in is_executor(): the name carries the "test_suite_executor"
# substring, so without this it passes the executor gate in _discover_server_jobs()
# and its whole build history gets walked. Kept separate from _EXCLUDE_TMP because
# that list is shared with CAPELLA_VIEW — the views concatenate, never mutate it.
_EXCLUDE_RTAF: List[re.Pattern] = [
    re.compile(r"test_suite_executor-RTAF"),
]

CAPELLA_VIEW = ViewConfig(
    name="capella",
    urls=[
        # PipelineJobs is the view the legacy pipeline collector walked, and it is the
        # only one that lists the pipeline-driven jobs plus the dispatcher:
        # capella_provisioned_v4_api_TAF (API_TESTS), security_capella_*_TAF (SECURITY),
        # terraform-test (TERRAFORM), the five sdk-* jobs, cp-cli-*, Perf-*. None of
        # those appear in view/Cloud, which is why those components vanished from the
        # board. It must come FIRST so test_suite_dispatcher_cloud is discovered before
        # the executor jobs that resolve their pipeline through it.
        "http://qe-jenkins1.sc.couchbase.com/view/PipelineJobs/",
        "http://qe-jenkins1.sc.couchbase.com/view/Cloud/",
        # NOTE: qa.sc.couchbase.com/view/Capella no longer exists (its api/json returns
        # the Jenkins 404 page), so it is dropped rather than retried every cycle.
    ],
    bucket="capella",
    platforms=CAPELLA_PLATFORMS,
    dedup_runs_per_build=False,
    # Order is resolution precedence. The first five are build-numbered params
    # ("7.6.12-0000") and stay first so the executor keeps its exact build. The rest
    # are the release-style params the plain Capella projects send with no build
    # number — UI-Automation-V2 sends full_server_version/SERVER_VERSION, the sdk-*
    # jobs send VERSION, capella_volume and Perf-Provisioned-Pipeline send `version`.
    # They only resolve because CapellaProcessor parses builds with allow_bare=True.
    # `full_server_version` precedes `SERVER_VERSION` because the latter is often
    # 2-part ("8.0") where the former is already the full release ("8.0.0").
    build_param_names=["version_number", "cluster_version", "build",
                       "COUCHBASE_SERVER_VERSION", "CB_VERSION",
                       "cbs_version", "full_server_version", "SERVER_VERSION",
                       "server_version", "VERSION", "version"],
    image_param_names=["IMAGE", "image", "image_name", "cbs_image", "cb_image"],

    env_param_names=["CYPRESS_BASE_URL", "Environment", "CP_CLI_APIURL",
                     "capella_api_url", "ENV_URL", "CP_API_URL",
                     "public_api_url", "CP_URL", "URL", "url"],
    exclude_patterns=_EXCLUDE_TMP,
)

SERVER_VIEW = ViewConfig(
    name="server",
    urls=[
        "http://qa.sc.couchbase.com",
        "http://qa.sc.couchbase.com/view/Cloud",
        "http://sdkbuilds.sc.couchbase.com/view/JAVA/job/server-build-test-java",
        "http://sdkbuilds.sc.couchbase.com/view/.NET/job/server-build-test-net/",
        "http://sdkbuilds.sc.couchbase.com/view/GO/job/server-build-test-go/",
        "http://sdkbuilds.sc.couchbase.com/job/Fast-failover-Java/",
        "http://sdkbuilds.sc.couchbase.com/job/fastfailover-lcb/",
        "http://sdkbuilds.sc.couchbase.com/view/JAVA/job/feature-java",
        "http://qa.sc.couchbase.com/view/OS%20Certification/",
        "http://sdkbuilds.sc.couchbase.com/view/IPV6",
        "http://sdk.jenkins.couchbase.com/view/Greenboard/",
    ],
    bucket="server",
    platforms=SERVER_PLATFORMS,
    build_param_names=["version_number", "columnar_version_number", "cluster_version",
                       "build", "cbs_ver", "COUCHBASE_SERVER_VERSION", "CB_VERSION"],
    exclude_patterns=_EXCLUDE_TMP + _EXCLUDE_RTAF,
)

SERVER_VIEW_2 = ViewConfig(
    name="server_qe",
    urls=["http://qe-jenkins1.sc.couchbase.com"],
    bucket="server",
    platforms=SERVER_PLATFORMS,
    build_param_names=["version_number", "cluster_version", "build",
                       "COUCHBASE_SERVER_VERSION", "columnar_version_number"],
    exclude_patterns=_EXCLUDE_TMP + _EXCLUDE_RTAF,
)

SG_VIEW = ViewConfig(
    name="sync_gateway",
    urls=["http://uberjenkins.sc.couchbase.com:8080/"],
    bucket="sync_gateway",
    platforms=SG_PLATFORMS,
    build_param_names=["SYNC_GATEWAY_VERSION", "sgw_ver",
                       "SYNC_GATEWAY_VERSION_OR_COMMIT"],
    filters=["SYNCGATEWAY", "SYNC-GATEWAY"],
)

CBLITE_JAVA_VIEW = ViewConfig(
    name="cblite_java",
    urls=["http://uberjenkins.sc.couchbase.com:8080/"],
    bucket="cblite",
    platforms={"JAVA": "JAVA"},
    build_param_names=["UPGRADED_CBLITE_VERSION", "COUCHBASE_MOBILE_VERSION",
                       "LITE_JAVA_VERSION", "LITE_JAVAWS_VERSION", "lite_java"],
    additional_fields={
        "secondary_os": [
            ["CENTOS-7", "CENTOS 7"], ["CENTOS-6", "CENTOS 6"],
            ["CENTOS7",  "CENTOS 7"], ["CENTOS6",  "CENTOS 6"],
            ["CENTOS-8", "CENTOS 8"], ["RHEL-7", "RHEL 7"], ["RHEL-8", "RHEL 8"],
            ["WINDOWS",  "WINDOWS"],  ["UBUNTU", "UBUNTU"],
            ["SANITY", "Common"], ["UPGRADE", "Common"],
        ],
        "build_type": [["webservice", "Web Service"], ["desktop", "Desktop"]],
    },
)

CBLITE_ANDROID_VIEW = ViewConfig(
    name="cblite_android",
    urls=["http://uberjenkins.sc.couchbase.com:8080/"],
    bucket="cblite",
    platforms={"ANDROID": "ANDROID"},
    build_param_names=["UPGRADED_CBLITE_VERSION", "COUCHBASE_MOBILE_VERSION",
                       "LITE_ANDROID_VERSION", "a_ver"],
    none_filters=["DOTNET", "XAMARIN"],
)

CBLITE_IOS_VIEW = ViewConfig(
    name="cblite_ios",
    urls=["http://uberjenkins.sc.couchbase.com:8080/"],
    bucket="cblite",
    platforms={"IOS": "IOS"},
    build_param_names=["UPGRADED_CBLITE_VERSION", "COUCHBASE_MOBILE_VERSION",
                       "CBL_iOS_Build", "LITE_IOS_VERSION", "XAMARIN_IOS_VERSION"],
    none_filters=["DOTNET", "XAMARIN"],
)

CBLITE_DOTNET_VIEW = ViewConfig(
    name="cblite_dotnet",
    urls=["http://uberjenkins.sc.couchbase.com:8080/"],
    bucket="cblite",
    platforms={"DOTNET": "DOTNET"},
    build_param_names=["UPGRADED_CBLITE_VERSION", "COUCHBASE_MOBILE_VERSION",
                       "LITE_DOTNET_VERSION", "XAMARIN_IOS_VERSION", "LITE_NET_VERSION"],
    additional_fields={
        "secondary_os": [
            ["ANDROID", "ANDROID"], ["IOS", "IOS"], ["WINDOWS", "WINDOWS"],
            ["SANITY", "Common"], ["UPGRADE", "Common"],
        ],
    },
)

CBLITE_CLIB_VIEW = ViewConfig(
    name="cblite_clib",
    urls=["http://uberjenkins.sc.couchbase.com:8080/"],
    bucket="cblite",
    platforms={"CLIB": "CLIB"},
    build_param_names=["LITE_NET_VERSION", "LITE_CLIB_VERSION",
                       "LITE_ANDROID_VERSION", "LITE_IOS_VERSION"],
    additional_fields={
        "secondary_os": [
            ["ANDROID",  "ANDROID"],  ["IOS",  "IOS"],  ["WINDOWS", "WINDOWS"],
            ["WIN",      "WINDOWS"],  ["DEBIAN", "DEBIAN9"], ["UBUNTU", "UBUNTU"],
            ["Rasbian2", "Rasbian2"], ["Rasbian3", "Rasbian3"], ["MACOS", "MACOS"],
            ["SANITY",   "Common"],   ["UPGRADE", "Common"],
        ],
    },
)

# Explicit component mapping for the small set of build sanity job names.
# These jobs have no "component" param — the name IS the classification.
BUILD_COMPONENTS: Dict[str, str] = {
    "build_sanity_matrix": "BUILD_SANITY",
    "unit-simple-test":    "UNIT",
    "watson-unix":         "UNIT",
}

BUILD_VIEW = ViewConfig(
    name="build",
    urls=[
        "https://server.jenkins.couchbase.com/job/build_sanity_matrix/",
        "http://cv.jenkins.couchbase.com/view/scheduled-unit-tests/job/unit-simple-test/",
        "http://server.jenkins.couchbase.com/job/watson-unix/",
    ],
    bucket="build",
    platforms=SERVER_PLATFORMS,
    # Build jobs carry OS/component in different params; handled explicitly in BuildProcessor.
)

OPERATOR_VIEW = ViewConfig(
    name="operator",
    urls=["http://qa.sc.couchbase.com/view/Cloud"],
    bucket="operator",
    platforms=OPERATOR_PLATFORMS,
    build_param_names=["operator_image"],
)

# CAO (Couchbase Autonomous Operator). Results come from a per-build
# `pipeline/results.json` artifact (a matrix of executions), NOT a testReport —
# handled by CaoProcessor. `urls` points straight at the single executor job.
CAO_VIEW = ViewConfig(
    name="cao",
    urls=["http://qe-jenkins1.sc.couchbase.com/job/cao-testrunner-executor/",
          "http://qe-jenkins1.sc.couchbase.com/job/cao-testrunner-executor-2/"],
    bucket="cao",
    platforms={},               # unused — CAO's axes live in matrixCombo, not OS
)

VIEWS: List[ViewConfig] = [
    SERVER_VIEW_2,
    SERVER_VIEW,
    BUILD_VIEW,
    # uber-jenkins scrapers (sync_gateway + cblite_*) removed — unused, and each cycle
    # wasted time hitting uberjenkins.sc.couchbase.com. Re-add here if ever needed.
    OPERATOR_VIEW,
    CAO_VIEW,
    CAPELLA_VIEW,
]


# ---------------------------------------------------------------------------
# Failure-pattern → label mapping
# ---------------------------------------------------------------------------

CLAIM_MAP: Dict[str, List[str]] = {
    "git error":                    ["hudson.plugins.git.GitException",
                                     "python3: can't open file 'testrunner.py': "
                                     "[Errno 2] No such file or directory"],
    "SSH error":                    ["paramiko.ssh_exception.SSHException",
                                     "Exception SSH session not active occurred on"],
    "IPv6 on IPv4 host":            ["Cannot enable IPv6 on an IPv4 machine"],
    "Python SDK error (CBQE-6230)": ["ImportError: cannot import name 'N1QLQuery' "
                                     "from 'couchbase.n1ql'"],
    "Syntax error":                 ["KeyError:", "TypeError:"],
    "JSON decode error":            ["json.decoder.JSONDecodeError:"],
    "Server unreachable":           ["ServerUnavailableException: unable to reach the host"],
    "Node already in cluster":      ["ServerAlreadyJoinedException:"],
    "CBQ error":                    ["membase.api.exception.CBQError:", "CBQError: CBQError:"],
    "RBAC error":                   ['"roles":"Cannot assign roles to user because the '
                                     'following roles are unknown'],
    "Rebalance error":              ["membase.api.exception.RebalanceFailedException"],
    "Build download failed":        ["Unable to copy build to", "Unable to download build in"],
    "Install not started":          ["INSTALL NOT STARTED ON"],
    "Install failed":               ["INSTALL FAILED ON"],
    "No test report":               ["No test report files were found. Configuration error?"],
}


# ---------------------------------------------------------------------------
# Misc constants
# ---------------------------------------------------------------------------

DEFAULT_BUILD          = "0.0.0-xxxx"
DEFAULT_ARCHITECTURE   = "x86_64"
DEFAULT_SERVER_TYPE    = "VM"
DEFAULT_BUCKET_STORAGE = "COUCHSTORE"
DEFAULT_GSI_TYPE       = "PLASMA"

P0, P1, P2 = "P0", "P1", "P2"

CB_RELEASE_BUILDS: Dict[str, str] = {
    "0.0.0": "0000",
    "2.1.1": "764",  "2.2.0": "821",  "2.5.0": "1059", "2.5.1": "1083",
    "2.5.2": "1154", "3.0.3": "1716", "3.1.5": "1859", "3.1.6": "1904",
    "4.0.0": "4051", "4.1.0": "5005", "4.1.1": "5914", "4.1.2": "6088",
    "4.5.0": "2601", "4.5.1": "2844", "4.6.0": "3573", "4.6.1": "3652",
    "4.6.2": "3905", "4.6.3": "4136", "4.6.4": "4590", "4.7.0": "0000",
    "4.6.5": "4742", "5.0.0": "3519", "5.0.1": "5003", "5.0.2": "5509",
    "5.1.0": "5552", "5.1.1": "5723", "5.1.2": "6030", "5.1.3": "6212",
    "5.5.0": "2958", "5.5.1": "3511", "5.5.2": "3733", "5.5.3": "4041",
    "5.5.4": "4338", "5.5.5": "4521", "6.0.0": "1693", "6.0.1": "2037",
    "6.0.2": "2413", "6.0.3": "0000", "6.5.0": "0000", "6.6.0": "7899",
}


# ---------------------------------------------------------------------------
# Runtime / connectivity
# ---------------------------------------------------------------------------

# Cluster connection — all via env; no credentials committed to source.
# Set CB_HOST / CB_USER / CB_PASS in the environment before running.
COUCHBASE_HOST: str  = os.environ.get("CB_HOST", "172.23.105.219")
COUCHBASE_USER: str  = os.environ.get("CB_USER", "Administrator")
COUCHBASE_PASS: str  = os.environ.get("CB_PASS", "")

# QE-Test-Suites catalog — the curated source of `gb_label` (greenboard display
# section overrides). A SEPARATE cluster from COUCHBASE_HOST. Loaded once at startup
# into a {(component, subcomponent): gb_label} map and stamped onto server docs.
# Blank CATALOG_HOST disables the lookup (docs just carry raw component).
CATALOG_HOST:   str  = os.environ.get("CATALOG_HOST", "172.23.217.21")
CATALOG_BUCKET: str  = os.environ.get("CATALOG_BUCKET", "QE-Test-Suites")
CATALOG_USER:   str  = os.environ.get("CATALOG_USER", COUCHBASE_USER)
CATALOG_PASS:   str  = os.environ.get("CATALOG_PASS", COUCHBASE_PASS)

UBER_USER: str = os.environ.get("UBER_USER", "")
UBER_PASS: str = os.environ.get("UBER_PASS", "")

POLL_INTERVAL_SECONDS: int   = int(os.environ.get("POLL_INTERVAL", "120"))
WORKER_POOL_SIZE: int        = int(os.environ.get("WORKER_POOL_SIZE", "16"))
# Cap how many of the newest builds each job walks. 0 = unlimited (full history).
# A cold first run otherwise walks every historical build of test_suite_executor
# (tens of thousands). For latest-version validation set e.g. MAX_BUILDS_PER_JOB=3000.
MAX_BUILDS_PER_JOB: int      = int(os.environ.get("MAX_BUILDS_PER_JOB", "0"))
# Executor jobs walk newest→oldest. Jenkins wipes builds after ~5 days, so once
# this many consecutive builds are gone (404), every older one is gone too —
# stop the walk. This bounds the executor walk to the live retention window.
STOP_AFTER_MISSING_BUILDS: int = int(os.environ.get("STOP_AFTER_MISSING", "50"))
HTTP_TIMEOUT_SECONDS: int    = int(os.environ.get("HTTP_TIMEOUT", "15"))
CONSOLE_TIMEOUT_SECONDS: int = int(os.environ.get("CONSOLE_TIMEOUT", "5"))

BUILDER_URLS: List[str] = [
    "https://server.jenkins.couchbase.com/job/couchbase-server-build/",
    "https://server.jenkins.couchbase.com/job/watson-build/",
]
CHANGE_LOG_URL: str = "http://172.23.123.43:8282/changelog"
