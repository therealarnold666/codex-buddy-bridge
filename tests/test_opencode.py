"""Tests for OpenCode ACP client and event source."""

import asyncio
import json
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from codex_buddy_bridge.events import ApprovalEvent, SignalSource
from codex_buddy_bridge.opencode_source import OpenCodeEventSource
from codex_buddy_bridge.__main__ import _resolve_opencode_url


class FakeOpencodeClient:
    """Mock OpenCodeClient for testing event source."""

    def __init__(self):
        self.replies = []

    async def reply_permission(self, session_id, permission_id, response):
        self.replies.append((session_id, permission_id, response))


class OpenCodeSourceTests(unittest.IsolatedAsyncioTestCase):
    """Test OpenCodeEventSource dispatches correct unified events."""

    async def test_permission_updated_creates_approval_event(self):
        daemon = MagicMock()
        daemon._handle_approval = AsyncMock()
        source = OpenCodeEventSource(daemon)

        event = {
            "type": "permission.updated",
            "properties": {
                "id": "perm-123",
                "sessionID": "sess-456",
                "type": "Bash",
                "title": "ls -la",
                "metadata": {},
            },
        }
        await source.on_event(event)

        daemon._handle_approval.assert_called_once()
        call_args = daemon._handle_approval.call_args[0][0]
        self.assertIsInstance(call_args, ApprovalEvent)
        self.assertEqual(call_args.source, SignalSource.OPENCODE)
        self.assertEqual(call_args.permission_id, "perm-123")
        self.assertEqual(call_args.session_id, "sess-456")
        self.assertEqual(call_args.tool, "Bash")
        self.assertEqual(call_args.hint, "ls -la")

    async def test_permission_replied_triggers_state_sync(self):
        daemon = MagicMock()
        daemon._session = MagicMock()
        daemon._request_state_sync = MagicMock()
        source = OpenCodeEventSource(daemon)

        event = {
            "type": "permission.replied",
            "properties": {
                "sessionID": "sess-456",
                "permissionID": "perm-123",
                "response": "once",
            },
        }
        await source.on_event(event)

        daemon._session.on_approved.assert_called_once()
        daemon._request_state_sync.assert_called_once()

    async def test_session_created_increments_total(self):
        daemon = MagicMock()
        daemon._session = MagicMock()
        daemon._session.total = 3
        daemon._request_state_sync = MagicMock()
        source = OpenCodeEventSource(daemon)

        event = {
            "type": "session.created",
            "properties": {"id": "new-sess"},
        }
        await source.on_event(event)

        daemon._session.set_total.assert_called_once_with(4)
        daemon._request_state_sync.assert_called_once()

    async def test_session_status_idle_triggers_sync(self):
        daemon = MagicMock()
        daemon._request_state_sync = MagicMock()
        source = OpenCodeEventSource(daemon)

        event = {
            "type": "session.status",
            "properties": {
                "sessionID": "sess-456",
                "status": {"type": "idle"},
            },
        }
        await source.on_event(event)

        daemon._request_state_sync.assert_called_once()

    async def test_unknown_event_types_ignored(self):
        daemon = MagicMock()
        source = OpenCodeEventSource(daemon)

        event = {"type": "unknown.stuff", "properties": {}}
        await source.on_event(event)

        daemon._handle_approval.assert_not_called()
        daemon._session.on_approved.assert_not_called()

    async def test_permission_with_metadata(self):
        daemon = MagicMock()
        daemon._handle_approval = AsyncMock()
        source = OpenCodeEventSource(daemon)

        event = {
            "type": "permission.updated",
            "properties": {
                "id": "perm-789",
                "sessionID": "sess-abc",
                "type": "Read",
                "title": "/etc/passwd",
                "metadata": {"path": "/etc/passwd", "size": 2048},
            },
        }
        await source.on_event(event)

        call_args = daemon._handle_approval.call_args[0][0]
        self.assertEqual(call_args.metadata, {"path": "/etc/passwd", "size": "2048"})


class ApprovalEventTests(unittest.TestCase):
    """Test ApprovalEvent field normalisation."""

    def test_opencode_event_has_permission_id(self):
        event = ApprovalEvent(
            source=SignalSource.OPENCODE,
            permission_id="perm-123",
            session_id="sess-456",
            tool="Bash",
            hint="ls -la",
        )
        self.assertEqual(event.permission_id, "perm-123")
        self.assertEqual(event.source, SignalSource.OPENCODE)

    def test_codex_event_defaults_to_codex_source(self):
        from codex_buddy_bridge.protocol import ApprovalRequest

        req = ApprovalRequest(id="c-1", tool="Bash", hint="ls")
        self.assertEqual(req.source, SignalSource.CODEX)

    def test_opencode_event_defaults_to_opencode_source(self):
        event = ApprovalEvent(
            source=SignalSource.OPENCODE,
            permission_id="p-1",
            session_id="s-1",
            tool="Bash",
            hint="ls",
        )
        self.assertEqual(event.source, SignalSource.OPENCODE)


class ResolveOpencodeUrlTests(unittest.IsolatedAsyncioTestCase):
    """Test _resolve_opencode_url CLI value resolution."""

    async def test_none_input_returns_none(self):
        result = await _resolve_opencode_url(None)
        self.assertIsNone(result)

    async def test_explicit_url_passes_through(self):
        result = await _resolve_opencode_url("http://127.0.0.1:48337")
        self.assertEqual(result, "http://127.0.0.1:48337")

    async def test_auto_with_discovery_success(self):
        with patch(
            "codex_buddy_bridge.__main__.discover_opencode_url",
            new_callable=AsyncMock,
            return_value="http://127.0.0.1:48337",
        ):
            result = await _resolve_opencode_url("auto")
        self.assertEqual(result, "http://127.0.0.1:48337")

    async def test_auto_with_discovery_failure(self):
        with patch(
            "codex_buddy_bridge.__main__.discover_opencode_url",
            new_callable=AsyncMock,
            return_value=None,
        ):
            result = await _resolve_opencode_url("auto")
        self.assertIsNone(result)

    async def test_auto_case_insensitive(self):
        with patch(
            "codex_buddy_bridge.__main__.discover_opencode_url",
            new_callable=AsyncMock,
            return_value="http://127.0.0.1:48337",
        ):
            result = await _resolve_opencode_url("AUTO")
        self.assertEqual(result, "http://127.0.0.1:48337")


if __name__ == "__main__":
    unittest.main()
