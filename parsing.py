"""
Parsing utilities shared across all processors.

Covers:
  - Jenkins action / parameter extraction
  - Build version normalisation
  - OS + component resolution (uses priority-sorted mappings)
  - Failure claim detection from console logs and test reports
  - Variant extraction (bucket_storage, gsi_type)
  - Caveats (skip rules inherited from old code)
"""
from __future__ import annotations

import re
import logging
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional, Tuple

from config import (
    CLAIM_MAP, P0, P1, P2,
    DEFAULT_ARCHITECTURE, DEFAULT_SERVER_TYPE,
    DEFAULT_BUCKET_STORAGE, DEFAULT_GSI_TYPE,
    ViewConfig,
)

logger = logging.getLogger(__name__)

_ASCII_CTRL  = re.compile(r"[^ -~]+")
_VERSION_RE  = re.compile(r"^\d\.\d\.\d{1,5}")
# Bare release with no build number, anchored at BOTH ends so loose junk
# ('default', '7.6.3)_smoke(true') is rejected rather than becoming a build doc.
_BARE_VERSION_RE = re.compile(r"^\d+\.\d+\.\d{1,5}$")

_BUILD_NO_RE = re.compile(r"^\d{1,10}")
_TICKET_RE   = re.compile(r"([A-Z]{2,4}[-: ]*\d{4,5})")


# ---------------------------------------------------------------------------
# Jenkins action / parameter helpers
# ---------------------------------------------------------------------------

def get_action(actions: Any, key: str, value: Optional[str] = None) -> Optional[Any]:
    """Extract a value from Jenkins API's `actions` array."""
    if not actions:
        return None
    for a in actions:
        if a is None:
            continue
        if hasattr(a, "keys"):
            keys = a.keys()
        elif a and hasattr(a[0], "keys"):
            keys = a[0].keys()
        else:
            continue
        if "urlName" in keys and a.get("urlName") not in (
                "robot", "testReport", "tapTestReport"):
            continue
        if key in keys:
            if value is not None:
                if a.get("name") == value:
                    return a.get("value")
            else:
                return a[key]
    return None


def extract_params(actions: Any) -> Any:
    """Return the parameters action, handling both old and new Jenkins API shapes."""
    params = get_action(actions, "parameters")
    if params is None and actions and not hasattr(actions, "keys"):
        for a in actions:
            if not hasattr(a, "keys"):
                params = a
                break
    return params


def should_skip_collect(params: Any) -> bool:
    return bool(
        get_action(params, "name", "SKIP_GREENBOARD_COLLECT") or
        get_action(params, "name", "skip_greenboard_collect")
    )


def should_skip_server_collect(params: Any) -> bool:
    return bool(
        get_action(params, "name", "SKIP_SERVER_GREENBOARD_COLLECT") or
        get_action(params, "name", "skip_server_greenboard_collect")
    )


def is_executor(name: str) -> bool:
    return "test_suite_executor" in name


def is_disabled(job: Dict) -> bool:
    return job.get("color") == "disabled"


def build_is_finished(res: Optional[Dict]) -> bool:
    if not res:
        return False
    return (
        res.get("result") in ("SUCCESS", "UNSTABLE", "FAILURE", "ABORTED")
        and not res.get("building", True)
    )


# ---------------------------------------------------------------------------
# Version normalisation
# ---------------------------------------------------------------------------

def parse_build_version(raw: str, allow_bare: bool = False) -> Optional[str]:
    """
    Normalise a raw version string to 'X.Y.Z-NNNN'.

    `allow_bare` additionally accepts a release with no build number ('7.6.5', '8.0')
    and returns it normalised to three parts ('7.6.5', '8.0.0'). That is the Capella
    convention: Capella tests run against a *released* cluster version, so most of its
    jobs pass `version`/`SERVER_VERSION`/`full_server_version` with no build number,
    and the legacy board keyed those runs on the bare release (26 of the build docs in
    capella_gb are bare — 13k UI runs sit under build='7.6.5'). Rejecting them is why
    every non-executor Capella job currently fails the build gate and disappears.

    Still validated strictly: the legacy collector's loose parse is what put junk build
    docs like 'default' and '7.6.3)_smoke(true' into capella_gb, and those become real
    (empty) entries in the board's build dropdown.
    """
    raw = raw.replace("-rel", "").split(",")[0].strip()
    try:
        parts = raw.split("-")
        if len(parts) < 2:
            if not allow_bare:
                return None
            rel = parts[0]
            while rel.count(".") < 2:
                rel += ".0"
            if not _BARE_VERSION_RE.match(rel):
                logger.debug("Unsupported bare version string: %s", raw)
                return None
            return rel
        rel, bno = parts[0], parts[1]
        while rel.count(".") < 2:
            rel += ".0"
        if not _VERSION_RE.match(rel) or not _BUILD_NO_RE.match(bno):
            logger.debug("Unsupported version string: %s", raw)
            return None
        return f"{rel}-{bno.zfill(4)}"
    except Exception:
        logger.debug("Failed to parse version: %s", raw, exc_info=True)
        return None


def get_build_and_priority(
    params: Any, param_names: List[str], allow_bare: bool = False
) -> Tuple[Optional[str], str]:
    if not params:
        return None, P1
    for name in param_names:
        raw = get_action(params, "name", name)
        if raw:
            build = parse_build_version(raw, allow_bare=allow_bare)
            if build:
                priority = get_action(params, "name", "priority") or P1
                if str(priority).upper() not in (P0, P1, P2):
                    priority = P1
                return build, priority
    return None, P1


def get_build_from_image(params: Any, image_param_names: List[str]) -> Optional[str]:
    for name in image_param_names:
        image = get_action(params, "name", name)
        if image:
            try:
                parts = image.split("-")
                return f"{parts[3]}-{parts[4]}"
            except (IndexError, Exception):
                pass
    return None


# ---------------------------------------------------------------------------
# OS / component resolution
# ---------------------------------------------------------------------------

def resolve_os_and_component(
    params: Any,
    job_name: str,
    view: ViewConfig,
    fallback_os: Optional[str] = None,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Resolve (os, component) for one Jenkins build.

    Resolution order:
      1. Read "component" and "OS"/"os" directly from build params — authoritative.
         No translation, no mapping table.  Jenkins sends the name, we store it.
      2. If "component" is absent, try the "test" param (.yml filename → SYSTEST_*).
      3. If "OS" is absent, fall back to job-name substring matching against the
         view's platforms dict (small, stable, OS-only).

    Adding a new component requires zero config changes.
    """
    # --- component from params (the normal case for modern jobs) ---
    component: Optional[str] = (
        get_action(params, "name", "component") or
        get_action(params, "name", "suite_type")
    )
    if component:
        component = component.upper()
    else:
        # Older jobs encode component in a test .yml filename
        test_yml = get_action(params, "name", "test")
        if test_yml and ".yml" in test_yml:
            import os as _os
            stem = _os.path.splitext(_os.path.basename(test_yml.split()[-1]))[0]
            component = f"SYSTEST_{stem.upper()}"

    # --- OS from params ---
    os_name: Optional[str] = (
        get_action(params, "name", "OS") or
        get_action(params, "name", "os") or
        fallback_os
    )
    if os_name:
        os_name = os_name.upper()
    else:
        os_name = _os_from_job_name(job_name, view)

    return os_name, component


def compose_test_name(
    os_name: Optional[str],
    component: Optional[str],
    subcomponent: Optional[str],
    arch: Optional[str] = None,
) -> Optional[str]:
    """
    Compose the per-test identity: "<os>-<component>_<subcomponent>".

    Pure string composition, deliberately kept free of Jenkins `params` so that the
    polling path (build_test_name(), below) and the one-shot push path
    (main.py `push`, used by RTAF, which has no pollable testReport) share exactly ONE
    source of truth for naming. Two implementations would drift, and a drifted name is
    unrecoverable: the doc key is md5(name-build_id), so a renamed job silently starts a
    second history instead of erroring.

    Values are used AS GIVEN — no case folding. The historical greenboard names are
    lower-case ("debian-2i_gsi-composite-vector"), while doc.os / doc.component are
    upper-cased by resolve_os_and_component(). Both spellings coexist on one doc by
    design; normalising here would rename every executor job.

    Returns None when there is no component — the caller should then leave the Jenkins
    job name untouched.
    """
    if not component:
        return None
    os_part = os_name or ""
    if arch and arch != DEFAULT_ARCHITECTURE:
        os_part = f"{os_part}-{arch}"
    return f"{os_part}-{component}_{subcomponent or 'server'}"


def build_test_name(params: Any, fallback_os: Optional[str] = None) -> Optional[str]:
    """
    Per-test identity for executor builds, read out of Jenkins build params.

    `test_suite_executor` is ONE Jenkins job that runs every suite; the actual test
    is identified by its `component` + `subcomponent` params. Reconstruct the name the
    original collector used — "<os>-<component>_<subcomponent>" from the raw param
    values, so it matches the historical greenboard job names (e.g. the lowercase
    "debian-2i_gsi-composite-vector").

    Param extraction only — the composition itself lives in compose_test_name().

    Returns None when there is no component param (a normally-named job) — caller
    should then leave the Jenkins job name untouched.
    """
    component = get_action(params, "name", "component")
    if not component:
        test_yml = get_action(params, "name", "test")
        if test_yml and ".yml" in test_yml:
            import os as _os
            stem = _os.path.splitext(_os.path.basename(test_yml.split()[-1]))[0]
            component = f"systest-{stem}"
    if not component:
        # Early out, exactly as before compose_test_name() was extracted. Nothing below
        # is reachable without a component, and this is the common path: every
        # non-executor build the poller walks lands here, so the lookups below would be
        # pure waste. compose_test_name() returns None for a falsy component anyway --
        # this keeps the WORK done identical, not just the result.
        return None

    os_param = (
        get_action(params, "name", "OS") or
        get_action(params, "name", "os") or
        fallback_os or ""
    )
    return compose_test_name(
        os_param,
        component,
        get_action(params, "name", "subcomponent"),
        get_action(params, "name", "arch"),
    )


def _os_from_job_name(name: str, view: ViewConfig) -> Optional[str]:
    """Last-resort OS extraction from job name using the platforms dict."""
    upper = name.upper().replace("-", "_")
    match = next(
        (canon for token, canon in view.platforms.items() if token.upper() in upper),
        None,
    )
    if match:
        return match
    # 3-char prefix fallback
    match = next(
        (canon for token, canon in view.platforms.items()
         if token[:3].upper() == upper[:3]),
        None,
    )
    if match:
        return match
    # 1-char initial fallback (not for sg/cblite — too ambiguous)
    if view.bucket not in ("sync_gateway", "cblite"):
        match = next(
            (canon for token, canon in view.platforms.items()
             if token[:1].upper() == upper[:1]),
            None,
        )
    return match


def resolve_capella_platform(name: str, view: ViewConfig) -> str:
    upper = name.upper()
    return next(
        (canon for token, canon in view.platforms.items() if token.upper() in upper),
        "AWS",
    )


def resolve_operator_platform(name: str, view: ViewConfig) -> Optional[str]:
    upper = name.upper()
    return next(
        (token.split("-")[0].upper()
         for token in view.platforms
         if token.split("-")[0].upper() in upper),
        None,
    )


# ---------------------------------------------------------------------------
# Failure claim detection
# ---------------------------------------------------------------------------

def _clean_line(line: str, limit: int = 1000) -> str:
    cleaned = _ASCII_CTRL.sub("", line.replace("\\n", "")).lstrip(
        "['Traceback (most recent call last): ")
    return (cleaned[:limit] + "...") if len(cleaned) > limit else cleaned


def _find_claim_label(text: str) -> Optional[str]:
    for label, patterns in CLAIM_MAP.items():
        for pattern in patterns:
            if pattern in text:
                return label
    return None


def get_claim_from_console(
    console_lines: Iterator[str],
) -> Optional[str]:
    reasons = set()
    for line in console_lines:
        cleaned = _clean_line(line)
        label = _find_claim_label(cleaned)
        if label:
            reasons.add(f"{label}: {cleaned}")
    return "<br><br>".join(sorted(reasons)) or None


def get_claim_from_test_report(report: Optional[Dict]) -> Optional[str]:
    if not report:
        return None
    reasons = set()
    for suite in report.get("suites", []):
        for case in suite.get("cases", []):
            if case.get("status") != "FAILED":
                continue
            stack = _clean_line(case.get("errorStackTrace") or "")
            label = _find_claim_label(stack.lower())
            if label:
                reasons.add(f"{label}: {stack}")
            elif stack:
                reasons.add(stack)
    return "<br><br>".join(sorted(reasons)) or None


def linkify_tickets(text: str) -> str:
    """Replace Jira-style ticket IDs with clickable HTML links."""
    rep = {
        m: f'<a href="https://issues.couchbase.com/browse/{m}">{m}</a>'
        for m in _TICKET_RE.findall(text)
    }
    if not rep:
        return text
    pattern = re.compile("|".join(re.escape(k) for k in rep))
    return pattern.sub(lambda x: rep[x.group()], text)


# ---------------------------------------------------------------------------
# Server / install information
# ---------------------------------------------------------------------------

def get_servers_from_params(params: Any) -> Tuple[List[str], bool]:
    raw = get_action(params, "name", "servers")
    if not raw:
        return [], False
    return [s.strip('"') for s in raw.split(",")], False


def get_servers_from_console(console_lines: Iterator[str]) -> Tuple[List[str], bool]:
    ips: set = set()
    install_failure = False
    for line in console_lines:
        if "thread installer-thread-" in line:
            ip = line.replace("thread installer-thread-", "").replace(" finished", "").strip()
            if ip:
                ips.add(ip)
        if any(kw in line for kw in
               ("INSTALL COMPLETED ON", "INSTALL NOT STARTED ON", "INSTALL FAILED ON")):
            parts = line.split(" ")
            if parts:
                ips.add(parts[-1].strip())
        if "INSTALL FAILED ON" in line or "INSTALL NOT STARTED ON" in line:
            install_failure = True
    return list(ips), install_failure


# ---------------------------------------------------------------------------
# Variants
# ---------------------------------------------------------------------------

def _get_variant_from_params(name: str, params: Any) -> Optional[str]:
    raw = get_action(params, "name", "parameters")
    if not raw:
        return None
    for part in raw.split(","):
        if part.startswith(name):
            pieces = part.split("=")
            return pieces[1].upper() if len(pieces) > 1 else None
    return None


# GSI default applies only to indexing components. The OLD collector keyed this off
# the NORMALISED component (2I_MOI / 2I_REBALANCE / PLASMA, produced by getOsComponent).
# This rewrite stores the RAW component param ("2i", "gsi", "2i_rebalance", "memdb",
# "plasma"), so we must match those raw values too — otherwise GSI jobs get
# GSI_type=UNDEFINED instead of PLASMA. Because variants are appended to the job name,
# that wrong default RENAMES every GSI job and breaks parity/history with the old board.
GSI_COMPONENTS = {"2I", "2I_MOI", "2I_REBALANCE", "GSI", "MEMDB", "PLASMA"}


def get_variants(params: Any, component: str) -> Dict[str, str]:
    storage = _get_variant_from_params("bucket_storage", params) or DEFAULT_BUCKET_STORAGE
    gsi = _get_variant_from_params("gsi_type", params)
    if gsi is None:
        gsi = DEFAULT_GSI_TYPE if (component or "").upper() in GSI_COMPONENTS else "UNDEFINED"
    return {"bucket_storage": storage, "GSI_type": gsi}


def add_variants_to_name(doc_name: str, variants: Dict[str, str]) -> str:
    for k, v in variants.items():
        doc_name += f"{k}={v}"
    return doc_name


# ---------------------------------------------------------------------------
# Environment / Capella helpers
# ---------------------------------------------------------------------------

def get_env_from_params(params: Any, env_param_names: List[str]) -> Optional[str]:
    """
    Resolve the Capella environment (PROD / SBX / STAGE / ...) from build params.

    Most env params are an API/UI URL, and the env token is the third-from-last
    dot-separated label — "https://cloudapi.sbx-29.sandbox.nonprod-...com" -> "sandbox".
    But some jobs pass the env *directly* (cp-cli-runner sends `Environment = sbx`),
    and a bare value has fewer than three labels, so the unguarded [-3] raised
    IndexError. That propagated out of the processor and killed every build of those
    jobs — which is why cp-cli-runner produced no docs at all. Short values are the
    env already, so use them as-is.
    """
    mapping = {"cloud": "PROD", "sandbox": "SBX"}
    for name in env_param_names:
        val = get_action(params, "name", name)
        if val:
            labels = str(val).split(".")
            raw = (labels[-3] if len(labels) >= 3 else labels[0]).split("/")[-1]
            if not raw:
                continue
            return mapping.get(raw, raw.upper())
    return None


# ---------------------------------------------------------------------------
# Caveats (business rules inherited from original collector)
# ---------------------------------------------------------------------------

def caveat_swap_xdcr(build: str, component: str) -> str:
    if build >= "4.0.1" and component == "XDCR":
        return "GOXDCR"
    return component


def caveat_should_skip(build: str, os_name: str, component: str, name: str) -> bool:
    # Skip certain WIN components for older builds
    win_skip = (
        build >= "4.1.0"
        and os_name == "WIN"
        and component in ("VIEW", "TUNABLE", "2I", "NSERV", "EP")
        and name.lower().startswith("w01")
    )
    # Skip backup_recovery on exactly 4.1.0
    br_skip = build.startswith("4.1.0") and component == "BACKUP_RECOVERY"
    return win_skip or br_skip


def caveat_should_skip_mobile(component: str, os_name: str) -> bool:
    return "MOBILE" in component and "CEN" not in os_name
