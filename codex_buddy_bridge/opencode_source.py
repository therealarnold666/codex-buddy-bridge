"""OpenCode ACP event source — converts ACP events to unified ApprovalEvent."""

from __future__ import annotations

import logging
from typing import Any

from .events import ApprovalEvent, SessionEvent, SignalSource


class OpenCodeEventSource:
    """Listens to OpenCode ACP /event SSE stream and dispatches
    unified events to the daemon."""

    def __init__(self, daemon, log: logging.Logger | None = None):
        self.daemon = daemon
        self._log = log or logging.getLogger("codex-buddy.opencode")

    async def on_event(self, event: dict[str, Any]) -> None:
        event_type = event.get("type", "")

        if event_type == "permission.updated":
            await self._handle_permission_updated(event)
        elif event_type == "permission.replied":
            self._handle_permission_replied(event)
        elif event_type == "session.created":
            self._handle_session_created(event)
        elif event_type == "session.status":
            self._handle_session_status(event)

    async def _handle_permission_updated(self, event: dict[str, Any]) -> None:
        props = event.get("properties", {})
        if not isinstance(props, dict):
            return

        permission_id = props.get("id", "")
        session_id = props.get("sessionID", "")
        tool = props.get("type", "unknown")
        hint = props.get("title", "")
        metadata = props.get("metadata", {}) or {}

        approval = ApprovalEvent(
            source=SignalSource.OPENCODE,
            permission_id=str(permission_id),
            session_id=str(session_id),
            tool=str(tool)[:30],
            hint=str(hint)[:100],
            metadata={k: str(v) for k, v in metadata.items()},
            source_specific=event,
        )
        await self.daemon._handle_approval(approval)

    def _handle_permission_replied(self, event: dict[str, Any]) -> None:
        props = event.get("properties", {})
        if isinstance(props, dict):
            self.daemon._session.on_approved()
            self.daemon._request_state_sync()

    def _handle_session_created(self, event: dict[str, Any]) -> None:
        props = event.get("properties", {})
        if isinstance(props, dict) and props.get("id"):
            self.daemon._session.set_total(
                self.daemon._session.total + 1
            )
            self.daemon._request_state_sync()

    def _handle_session_status(self, event: dict[str, Any]) -> None:
        props = event.get("properties", {})
        if not isinstance(props, dict):
            return
        status = props.get("status", {})
        if isinstance(status, dict) and status.get("type") == "idle":
            self.daemon._request_state_sync()
