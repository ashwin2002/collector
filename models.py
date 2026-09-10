"""Typed dataclasses for documents flowing through the collector pipeline."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class JobDoc:
    """Mutable document assembled while processing one Jenkins build."""

    # --- identity (set from Jenkins job listing) ---
    name: str
    url: str
    color: Optional[str] = None

    # --- classification ---
    os: Optional[str] = None
    component: Optional[str] = None
    # Raw `subcomponent` param. Kept on the doc so a mis-named run can be re-keyed
    # offline later: the Capella naming bug was unrecoverable for a month of data
    # precisely because subcomponent was never persisted and Jenkins only retains
    # ~100 builds, so the per-suite identity could not be reconstructed after the fact.
    sub_component: Optional[str] = None
    # Curated greenboard display section (from QE-Test-Suites catalog). The eventing
    # nests by `gb_label || component`, so this overrides the displayed grouping while
    # leaving `component` as the raw truth. Absent => greenboard uses `component`.
    gb_label: Optional[str] = None

    # --- per-build fields (populated during scrape) ---
    build_id: Optional[int] = None
    build: Optional[str] = None
    result: Optional[str] = None
    duration: int = 0
    timestamp: int = 0

    # --- test counts ---
    total_count: int = 0
    fail_count: int = 0
    skip_count: int = 0

    # --- metadata ---
    priority: str = "P1"
    claim: str = ""
    triage: str = ""
    bugs: List[str] = field(default_factory=list)
    servers: List[str] = field(default_factory=list)
    variants: Dict[str, str] = field(default_factory=dict)
    display_name: Optional[str] = None

    # --- product-specific extras ---
    server_version: Optional[str] = None
    sync_gateway_version: Optional[str] = None
    cp_version: Optional[str] = None
    # Pipeline context. A Capella run is only meaningful with the pipeline that
    # launched it (it owns the environment, control-plane version and cluster version),
    # and the board groups runs by pipeline.
    pipeline_job: Optional[str] = None
    pipeline_url: Optional[str] = None
    pipeline_id: Optional[str] = None
    env: Optional[str] = None
    provider: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to the shape Couchbase / Greenboard expects."""
        d: Dict[str, Any] = {
            "name":        self.name,
            "url":         self.url,
            "color":       self.color,
            "os":          self.os,
            "component":   self.component,
            "build_id":    self.build_id,
            "build":       self.build,
            "result":      self.result,
            "duration":    self.duration,
            "timestamp":   self.timestamp,
            "totalCount":  self.total_count,
            "failCount":   self.fail_count,
            "skipCount":   self.skip_count,
            "priority":    self.priority,
            "claim":       self.claim,
            "triage":      self.triage,
            "bugs":        self.bugs,
            "servers":     self.servers,
        }
        if self.variants:
            d["variants"] = self.variants
        if self.gb_label is not None:
            d["gb_label"] = self.gb_label
        if self.sub_component is not None:
            d["subComponent"] = self.sub_component
        if self.display_name is not None:
            d["displayName"] = self.display_name
        if self.server_version is not None:
            d["server_version"] = self.server_version
        if self.sync_gateway_version is not None:
            d["sync_gateway_version"] = self.sync_gateway_version
        if self.cp_version is not None:
            d["cp_version"] = self.cp_version
        if self.pipeline_job is not None:
            d["pipeline_job"] = self.pipeline_job
        if self.pipeline_url is not None:
            d["pipeline_url"] = self.pipeline_url
        if self.pipeline_id is not None:
            d["pipeline_id"] = self.pipeline_id
        if self.env is not None:
            d["env"] = self.env
        if self.provider is not None:
            d["provider"] = self.provider
        return d

    def copy(self) -> "JobDoc":
        """Shallow copy — safe because list/dict fields are re-initialized."""
        import copy
        return copy.deepcopy(self)
