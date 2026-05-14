"""Unified event model bridging Codex and OpenCode signal sources.

The daemon receives events from one or both sources (Codex IPC hooks and
OpenCode ACP HTTP events).  This module defines a single-event representation
so the rest of the daemon can stay source-agnostic.
"""

from __future__ import annotations

from enum import Enum
from dataclasses import dataclass, field
from typing import Any


class SignalSource(str, Enum):
    """Where an event originated."""

    CODEX = "codex"
    OPENCODE = "opencode"


@dataclass(frozen=True)
class ApprovalEvent:
    """A permission-approval event, normalised across sources."""

    source: SignalSource
    permission_id: str
    session_id: str
    tool: str
    hint: str
    metadata: dict[str, Any] = field(default_factory=dict)
    source_specific: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SessionEvent:
    """Session lifecycle event (created / status / idle)."""

    source: SignalSource
    session_id: str
    kind: str  # "created" | "status" | "idle" | "stopped"
    running: int = 0
    total: int = 0
