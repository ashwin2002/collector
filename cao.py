"""
CAO (Couchbase Autonomous Operator) results parsing.

The cao-testrunner-executor job publishes a `results.json` artifact per build
(it used to live at `pipeline/results.json` -- both paths are still tried, see
processors.CaoProcessor). Unlike server/operator jobs (which expose a Jenkins
testReport), CAO's results are shaped as a MATRIX:

    {jobId, component, subcomponent, jobName, clusterNames, executions: [...]}

...where every execution pins a matrix combo:

    platform, k8sVersion, openshiftVersion, caoVersion, cbServerVersion,
    cloudProvider, component, subcomponent

...and runs a flat list of scenario `tests`, each with a status, timings, an
error, a stackTrace, and (for upgrade scenarios) an ordered `upgrades` list of
{type, from, to, subType, timestamp} hops.

Since the dispatcher rework, ONE executor build == ONE component/subcomponent ==
ONE matrix combo. The `executions` array is still an array (and we still loop),
but in practice it carries a single entry. The greenboard "job" identity is
therefore:

    caoVersion _ cbServerVersion _ PLATFORM _ orchestratorVersion _ component_subcomponent
    e.g. 2.9.3-141_8.5.0-1074_EKS_1.36_upgrade_operator

and repeated executor builds carrying the same identity are RERUNS of it.

This module is PURE (no Couchbase / Jenkins deps) so it can be unit-tested on a
saved results.json. It turns one build's results.json into a list of "cao_run"
docs -- ONE per execution/combo. gb-v2 groups these into jobs + runs in its
snapshot layer; the collector only writes the normalized raw docs here.

NOTE: keep this file PURE ASCII. A literal micro-sign in the duration regex once
got re-encoded to an invalid UTF-8 byte in transfer, which made Python fail to
import the module -- and since processors.py imports it, that took down the ENTIRE
collector. The micro-second unit is matched via a \\u00b5 escape below instead.

Design note -- WHY dims stay an open dict:
    matrixCombo grew `component`/`subcomponent` without warning, and `upgrades`
    grew a `subType`. Storing the full combo verbatim under `dims` plus named
    slots for the fields we actually pivot on means the next such addition is a
    parse tweak, not a schema migration. Do NOT hard-code an exact column set
    downstream.
"""
from __future__ import annotations

import hashlib
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

# Bumped whenever the doc shape changes in a way gb-v2 must notice. v1 = the
# original suites-only doc; v2 = component/subcomponent, flat scenarios with
# stack traces, per-scenario upgrade hops, rerun/trigger provenance.
SCHEMA = 2

# The known matrix dimensions. Kept as data, not baked into the doc shape --
# everything in matrixCombo is preserved under `dims` regardless.
KNOWN_DIMS = ("platform", "k8sVersion", "openshiftVersion", "caoVersion",
              "cbServerVersion", "cloudProvider", "component", "subcomponent")

# A stack trace is the whole reason the right sidebar exists, but a runaway one
# would bloat every doc (and the /api/cao payload, which ships them all). Keep
# generously more than a human reads, far less than a log file.
MAX_STACK_TRACE = 12000
MAX_ERROR = 2000

# ---------------------------------------------------------------------------
# Small pure helpers
# ---------------------------------------------------------------------------

_MICRO = "\u00b5"          # micro sign via escape -> source stays pure ASCII
# Go durations: "9m18.487112818s", "26.9s", "500ms", "1h2m3s", micro via "us"/"<micro>s".
_DUR_RE = re.compile(r"(\d+(?:\.\d+)?)(ms|us|" + _MICRO + r"s|ns|h|m|s)")
_DUR_MS = {"h": 3600_000.0, "m": 60_000.0, "s": 1000.0,
           "ms": 1.0, "us": 0.001, _MICRO + "s": 0.001, "ns": 0.000001}


def parse_go_duration(s: Optional[str]) -> int:
    """'9m18.487112818s' -> 558487 (ms). Tolerant: returns 0 on junk/empty."""
    if not s or not isinstance(s, str):
        return 0
    total = 0.0
    matched = False
    for num, unit in _DUR_RE.findall(s):
        matched = True
        total += float(num) * _DUR_MS[unit]
    return int(round(total)) if matched else 0


def iso_to_ms(s: Optional[str]) -> int:
    """RFC3339 with nanoseconds ('...687923376-07:00') -> epoch ms. 0 on failure."""
    if not s or not isinstance(s, str):
        return 0
    # datetime.fromisoformat accepts at most microseconds -- truncate the fraction.
    m = re.match(r"^(.*\.\d{6})\d*([+-]\d{2}:\d{2}|Z)?$", s)
    if m:
        s = m.group(1) + (m.group(2) or "")
    s = s.replace("Z", "+00:00")
    try:
        return int(datetime.fromisoformat(s).timestamp() * 1000)
    except Exception:
        return 0


def _clip(s: Any, limit: int) -> str:
    """Trim a long free-text field, marking that it was trimmed."""
    if not s:
        return ""
    text = str(s)
    if len(text) <= limit:
        return text
    return text[:limit] + "\n... [truncated, " + str(len(text) - limit) + " more chars]"


def suite_and_test(scenario_file: str):
    """
    'scenarios/tests/upgrade/operator/2.x.x-2.x.x_sanity.yaml'
        -> ('operator', '2.x.x-2.x.x_sanity')

    Suite = the immediate parent directory, which under the new
    `/scenarios/tests/<component>/<subcomponent>/` layout IS the subcomponent;
    test = the file basename without extension.
    """
    if not scenario_file:
        return ("unknown", "unknown")
    parts = [p for p in scenario_file.replace("\\", "/").split("/") if p]
    base = parts[-1] if parts else scenario_file
    test = re.sub(r"\.(ya?ml|json)$", "", base, flags=re.IGNORECASE)
    suite = parts[-2] if len(parts) >= 2 else "unknown"
    return (suite, test)


# The scenario ROOT directory, under which the layout is
# <component>/[<subcomponent>/]<file>.yaml. `tests` is a level the new layout
# inserts (scenarios/tests/upgrade/operator/...); older runs had neither it nor
# the explicit component fields, and absolute workspace paths used
# `sample_scenarios`. Recognizing all three is what keeps pre-rework builds on
# the board with a real name instead of "?".
_SCENARIO_ROOTS = ("scenarios", "sample_scenarios")


def scenario_component(scenario_file: str):
    """
    Derive (component, subcomponent) from a scenario path.

        scenarios/tests/upgrade/operator/x.yaml  -> ('upgrade', 'operator')
        scenarios/backup_restore/strategies/x.yaml -> ('backup_restore', 'strategies')
        scenarios/xdcr/x.yaml                    -> ('xdcr', '')
        /ws/.../sample_scenarios/dac_tests/x.yaml -> ('dac_tests', '')

    ('', '') when the path has no recognizable root -- callers fall back to the
    explicit results.json fields, which are authoritative when present.
    """
    parts = [p for p in str(scenario_file or "").replace("\\", "/").split("/") if p]
    root = -1
    for i, part in enumerate(parts):
        if part.lower() in _SCENARIO_ROOTS:
            root = i
    if root < 0:
        return ("", "")
    rest = parts[root + 1:]
    if rest and rest[0].lower() == "tests":
        rest = rest[1:]
    dirs = rest[:-1]                      # drop the file itself
    if not dirs:
        return ("", "")
    return (dirs[0], dirs[1] if len(dirs) > 1 else "")


def _derive_identity(scenarios: List[Dict[str, Any]]):
    """
    (component, subcomponent) implied by a run's scenario paths. A run whose
    scenarios disagree (the pre-rework executor bundled several components into
    one build) reports 'mixed' rather than silently picking one.
    """
    comps = {s["scn_component"] for s in scenarios if s.get("scn_component")}
    if not comps:
        return ("", "")
    if len(comps) > 1:
        return ("mixed", "")
    component = comps.pop()
    subs = {s["scn_subcomponent"] for s in scenarios if s.get("scn_subcomponent")}
    return (component, subs.pop() if len(subs) == 1 else "")


def orchestrator(dims: Dict[str, Any]) -> Dict[str, str]:
    """Collapse the mutually-exclusive k8s/openshift version into one facet."""
    plat = (dims.get("platform") or "").lower()
    if plat == "openshift" or dims.get("openshiftVersion"):
        return {"type": "openshift", "version": str(dims.get("openshiftVersion") or "")}
    return {"type": "kubernetes", "version": str(dims.get("k8sVersion") or "")}


# cloud x orchestrator -> the platform short-name the board pivots on.
# EKS, GKE, AKS, OC, OCAz, OCG are the six the CAO matrix actually runs.
_CLOUD_ALIAS = {
    "aws": "aws", "amazon": "aws", "eks": "aws",
    "gcp": "gcp", "google": "gcp", "gke": "gcp",
    "azure": "azure", "az": "azure", "aks": "azure",
}
_PLATFORM_CODE = {
    ("kubernetes", "aws"):   "EKS",
    ("kubernetes", "gcp"):   "GKE",
    ("kubernetes", "azure"): "AKS",
    ("openshift", "aws"):    "OC",
    ("openshift", "azure"):  "OCAz",
    ("openshift", "gcp"):    "OCG",
}


def platform_code(dims: Dict[str, Any], orch: Optional[Dict[str, str]] = None) -> str:
    """
    ('kubernetes','aws') -> 'EKS'. Unknown clouds degrade to a readable stand-in
    ('K8S' / 'OCP') rather than dropping the run out of the platform axis.
    """
    orch = orch or orchestrator(dims)
    otype = orch.get("type") or "kubernetes"
    cloud = _CLOUD_ALIAS.get(str(dims.get("cloudProvider") or "").lower(), "")
    code = _PLATFORM_CODE.get((otype, cloud))
    if code:
        return code
    return "OCP" if otype == "openshift" else "K8S"


# The upgrade `type` vocabulary drifts between producers ("server" vs
# "couchbaseServer", "cao" vs "operator"). Normalize once, here, so the board's
# transition matrix never has to know about the aliases.
_UPGRADE_TYPE = {
    "server": "server", "couchbaseserver": "server", "couchbase": "server",
    "cb": "server", "cbserver": "server",
    "operator": "operator", "cao": "operator",
    "k8s": "k8s", "kubernetes": "k8s",
    "openshift": "openshift", "ocp": "openshift",
}


def upgrade_type(raw: Any) -> str:
    key = re.sub(r"[^a-z0-9]", "", str(raw or "").lower())
    return _UPGRADE_TYPE.get(key, str(raw or "").lower() or "unknown")


def norm_upgrades(raw: Any) -> List[Dict[str, str]]:
    """results.json `upgrades` -> normalized ordered hops. Junk entries dropped."""
    out: List[Dict[str, str]] = []
    for u in raw or []:
        if not isinstance(u, dict):
            continue
        frm, to = str(u.get("from") or ""), str(u.get("to") or "")
        if not frm and not to:
            continue
        out.append({
            "type": upgrade_type(u.get("type")),
            "subType": str(u.get("subType") or ""),
            "from": frm,
            "to": to,
            "timestamp": str(u.get("timestamp") or ""),
            "ts": iso_to_ms(u.get("timestamp")),
        })
    return out


def merge_upgrades(per_test: List[List[Dict[str, str]]]) -> List[Dict[str, str]]:
    """Union of every scenario's hops in first-seen order, deduped on type+from+to."""
    seen = set()
    out: List[Dict[str, str]] = []
    for hops in per_test:
        for u in hops:
            k = (u["type"], u["from"], u["to"])
            if k in seen:
                continue
            seen.add(k)
            out.append(u)
    return out


# Which axis a given upgrade type moves. Used to keep the legacy `upgrade`
# {field, from, to} slot populated for anything still reading it.
_TYPE_TO_FIELD = {"server": "cbServerVersion", "operator": "caoVersion",
                  "k8s": "k8sVersion", "openshift": "openshiftVersion"}
# Order of interest when a run upgrades more than one thing: the server move is
# the headline, then the operator, then the platform.
_PRIMARY_ORDER = ("server", "operator", "openshift", "k8s")


def primary_upgrade(upgrades: List[Dict[str, str]]) -> Optional[Dict[str, str]]:
    """The one transition that best characterizes an upgrade run (or None)."""
    for want in _PRIMARY_ORDER:
        hops = [u for u in upgrades if u["type"] == want]
        if hops:
            return {
                "field": _TYPE_TO_FIELD.get(want, want),
                "type": want,
                "from": hops[0]["from"],
                "to": hops[-1]["to"],
            }
    return None


def combo_id(dims: Dict[str, Any]) -> str:
    """
    Stable id for a combo WITHIN a cbServerVersion (the doc's build key). Hash the
    non-server dims so the same matrix cell across server builds shares an id.
    """
    key = "|".join(str(dims.get(k, "")) for k in
                   ("platform", "k8sVersion", "openshiftVersion", "caoVersion",
                    "cloudProvider", "component", "subcomponent"))
    return hashlib.md5(key.encode()).hexdigest()[:12]


def job_key(dims: Dict[str, Any], job_name: str) -> str:
    """
    The greenboard JOB identity (see module docstring):
        2.9.3-141_8.1.0-2452_EKS_1.36_upgrade_operator
    Executor builds that share this string are reruns of the same job.
    """
    orch = orchestrator(dims)
    return "_".join([
        str(dims.get("caoVersion") or "?"),
        str(dims.get("cbServerVersion") or "?"),
        platform_code(dims, orch),
        str(orch.get("version") or "?"),
        job_name or "?",
    ])


def selection_build(dims: Dict[str, Any], upgrade: Optional[Dict[str, str]]) -> str:
    """
    The primary selection axis = cbServerVersion. For a server-upgrade run we file
    it under the TARGET version (what you're upgrading to), so it surfaces where a
    reader looks for '8.0.0'. The `from` stays queryable via `upgrades`.
    """
    build = str(dims.get("cbServerVersion") or "")
    if build:
        return build
    if upgrade and upgrade["field"] == "cbServerVersion":
        return upgrade["to"] or upgrade["from"]
    return ""


# ---------------------------------------------------------------------------
# Main entry -- one build's results.json -> list of cao_run docs
# ---------------------------------------------------------------------------

# `not_run` shows up when the executor bailed during setup: the scenario was
# never attempted, so it is neither a pass nor a failure.
_STATUS_MAP = {
    "passed": "pass", "pass": "pass", "success": "pass",
    "failed": "fail", "fail": "fail", "failure": "fail",
    "not_run": "not_run", "notrun": "not_run", "skipped": "not_run",
}


def build_cao_docs(
    data: Dict[str, Any],
    job_url: str,
    job_id: Any,
    meta: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Transform results.json -> normalized cao_run docs (one per execution/combo).

    `meta` carries the Jenkins-side provenance the artifact itself cannot know --
    build timestamp/duration/result and the executor parameters that record the
    rerun chain (RERUN / RERUN_FROM_BUILD), the dispatcher build, and which pivot
    triggered the run (TRIGGER_TYPE / TRIGGER_VERSION). It is optional so this
    module stays testable on a bare saved artifact.
    """
    if not isinstance(data, dict):
        return []
    meta = meta or {}
    job_id = str(data.get("jobId") or job_id or "")
    base = job_url.rstrip("/")
    run_url = f"{base}/{job_id}/"

    # Top-level component/subcomponent is the executor-wide truth; matrixCombo
    # repeats it. Prefer the combo (it is the narrower scope) and fall back.
    top_component = str(data.get("component") or "")
    top_sub = str(data.get("subcomponent") or "")
    top_job_name = str(data.get("jobName") or "")
    cluster_names = [str(c) for c in (data.get("clusterNames") or []) if c]

    docs: List[Dict[str, Any]] = []

    for idx, execution in enumerate(data.get("executions") or []):
        dims = dict(execution.get("matrixCombo") or {})
        component = str(dims.get("component") or top_component or "")
        subcomponent = str(dims.get("subcomponent") or top_sub or "")

        suites: Dict[str, Dict[str, Any]] = {}
        scenarios: List[Dict[str, Any]] = []
        per_test_upgrades: List[List[Dict[str, str]]] = []
        passed = failed = other = 0

        for t in execution.get("tests") or []:
            suite, name = suite_and_test(t.get("scenarioFile", ""))
            outcome = _STATUS_MAP.get(str(t.get("status", "")).lower(), "other")
            if outcome == "pass":
                passed += 1
            elif outcome == "fail":
                failed += 1
            else:
                other += 1
            hops = norm_upgrades(t.get("upgrades"))
            per_test_upgrades.append(hops)

            scn_component, scn_sub = scenario_component(t.get("scenarioFile", ""))
            scenario = {
                "name": name,
                "suite": suite,
                "file": t.get("scenarioFile") or "",
                "scn_component": scn_component,
                "scn_subcomponent": scn_sub,
                "status": outcome,               # pass | fail | not_run | other
                "raw_status": t.get("status"),
                "duration_ms": parse_go_duration(t.get("duration")),
                "duration": t.get("duration") or "",
                "error": _clip(t.get("error"), MAX_ERROR),
                "stack_trace": _clip(t.get("stackTrace"), MAX_STACK_TRACE),
                "startTime": t.get("startTime") or "",
                "endTime": t.get("endTime") or "",
                "start_ms": iso_to_ms(t.get("startTime")),
                "end_ms": iso_to_ms(t.get("endTime")),
                "upgrades": hops,
            }
            scenarios.append(scenario)

            s = suites.setdefault(suite, {"total": 0, "passed": 0, "failed": 0,
                                          "other": 0, "tests": []})
            s["total"] += 1
            s[{"pass": "passed", "fail": "failed"}.get(outcome, "other")] += 1
            # Legacy per-suite view kept for anything still reading `suites`.
            s["tests"].append({
                "name": name,
                "status": outcome,
                "raw_status": t.get("status"),
                "duration_ms": scenario["duration_ms"],
                "error": scenario["error"],
                "file": scenario["file"],
            })

        # results.json only started carrying component/subcomponent after the
        # dispatcher rework; older builds encode them in the scenario paths.
        if not component:
            component, derived_sub = _derive_identity(scenarios)
            subcomponent = subcomponent or derived_sub
        job_name = top_job_name or "_".join([p for p in (component, subcomponent) if p])

        upgrades = merge_upgrades(per_test_upgrades)
        upgrade = primary_upgrade(upgrades)
        build = selection_build(dims, upgrade)
        if not build:
            continue  # no cbServerVersion -> nothing to file under

        total = passed + failed + other
        setup_error = _clip(execution.get("setupError"), MAX_ERROR)
        if failed:
            result = "FAILURE"
        elif passed:
            result = "SUCCESS"
        else:
            # Nothing decided: a setup failure or an aborted executor.
            result = "ABORTED"

        start_ms = iso_to_ms(execution.get("startTime"))
        end_ms = iso_to_ms(execution.get("endTime"))
        orch = orchestrator(dims)

        docs.append({
            "doc_type": "cao_run",
            "schema": SCHEMA,
            "job_id": job_id,
            "build_id": int(job_id) if str(job_id).isdigit() else job_id,
            "combo_index": idx,
            "combo_id": combo_id(dims),
            "url": run_url,
            "build": build,                      # cbServerVersion -- PRIMARY selection axis
            "cao": str(dims.get("caoVersion") or ""),   # the second pivot axis
            "component": component,
            "subcomponent": subcomponent,
            "job_name": job_name,                # component_subcomponent
            "job_key": job_key(dims, job_name),  # identity shared by reruns
            "dims": dims,                        # full matrixCombo verbatim (open schema)
            "orchestrator": orch,                # {type, version} -- k8s|openshift collapsed
            "platform": platform_code(dims, orch),   # EKS | GKE | AKS | OC | OCAz | OCG
            "cluster_names": cluster_names,
            "upgrade": upgrade,                  # headline transition (legacy slot)
            "upgrades": upgrades,                # every hop, deduped, ordered
            "upgrade_types": sorted({u["type"] for u in upgrades}),
            "trigger": meta.get("trigger") or {},        # {type, version}
            "dispatcher_build": meta.get("dispatcher_build"),
            "rerun": meta.get("rerun") or {},            # {is_rerun, from_build, selection}
            "result": result,                    # combo-level roll-up
            "jenkins_result": meta.get("jenkins_result") or "",
            "total": total,
            "passed": passed,
            "failed": failed,
            "other": other,
            "setup_error": setup_error,
            "scenarios": scenarios,              # flat, with stack traces + hops
            "suites": suites,                    # suite -> {counts, tests[]}
            "startTime": execution.get("startTime") or "",
            "endTime": execution.get("endTime") or "",
            "duration_ms": max(0, end_ms - start_ms) if (start_ms and end_ms) else
                           int(meta.get("jenkins_duration") or 0),
            "timestamp": start_ms or int(meta.get("jenkins_timestamp") or 0),
            "deleted": False,
            "olderBuild": False,
        })
    return docs
