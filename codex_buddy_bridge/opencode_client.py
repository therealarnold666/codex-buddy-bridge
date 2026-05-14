"""OpenCode ACP (Agent Client Protocol) HTTP client.

Connects to a running OpenCode instance (TUI or serve/acp mode) via its
embedded HTTP API to subscribe to permission events and reply to them.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Awaitable, Callable

import httpx

EventCallback = Callable[[dict[str, Any]], Awaitable[None]]


class OpenCodeClient:
    """HTTP client for OpenCode's ACP server."""

    def __init__(self, base_url: str, log: logging.Logger | None = None):
        self.base_url = base_url.rstrip("/")
        self._log = log or logging.getLogger("codex-buddy.opencode")
        self._client: httpx.AsyncClient | None = None
        self._event_callback: EventCallback | None = None
        self._running = False
        self._event_task: asyncio.Task[None] | None = None

    @property
    def is_connected(self) -> bool:
        return self._client is not None

    async def connect(self) -> None:
        self._client = httpx.AsyncClient(base_url=self.base_url, timeout=10.0)
        resp = await self._client.get("/config")
        resp.raise_for_status()
        self._log.info("Connected to OpenCode ACP at %s", self.base_url)

    async def subscribe_events(self, callback: EventCallback) -> None:
        """Subscribe to the /event SSE stream."""
        self._event_callback = callback
        self._running = True
        self._event_task = asyncio.create_task(
            self._event_loop(), name="opencode-event-subscriber"
        )

    async def _event_loop(self) -> None:
        assert self._client is not None
        while self._running:
            try:
                async with self._client.stream(
                    "GET", "/event", timeout=30.0
                ) as resp:
                    resp.raise_for_status()
                    async for line in resp.aiter_lines():
                        if not self._running:
                            break
                        if not line.startswith("data:"):
                            continue
                        try:
                            data = json.loads(line[5:].strip())
                        except json.JSONDecodeError:
                            continue
                        if self._event_callback:
                            try:
                                await self._event_callback(data)
                            except Exception:  # noqa: BLE001
                                self._log.exception("Event callback error")
            except httpx.ReadError:
                if self._running:
                    self._log.debug("SSE stream disconnected, retrying…")
                    await asyncio.sleep(2.0)
            except Exception:  # noqa: BLE001
                if self._running:
                    self._log.exception("SSE stream error")
                    await asyncio.sleep(2.0)

    async def reply_permission(
        self, session_id: str, permission_id: str, response: str
    ) -> bool:
        """Reply to a permission: response is 'once', 'always', or 'reject'."""
        assert self._client is not None
        try:
            resp = await self._client.post(
                f"/session/{session_id}/permissions/{permission_id}",
                json={"response": response},
            )
            return resp.status_code == 200
        except Exception:  # noqa: BLE001
            self._log.warning(
                "Failed to reply permission %s/%s", session_id, permission_id
            )
            return False

    async def get_sessions(self) -> list[dict[str, Any]]:
        """List active sessions."""
        assert self._client is not None
        try:
            resp = await self._client.get("/session")
            if resp.status_code == 200:
                return resp.json()
        except Exception:  # noqa: BLE001
            pass
        return []

    async def get_session_status(self, session_id: str) -> dict[str, Any] | None:
        """Get status of a specific session."""
        assert self._client is not None
        try:
            resp = await self._client.get(f"/session/{session_id}/status")
            if resp.status_code == 200:
                return resp.json()
        except Exception:  # noqa: BLE001
            pass
        return None

    async def stop(self) -> None:
        """Shut down the client."""
        self._running = False
        if self._event_task is not None:
            self._event_task.cancel()
            try:
                await self._event_task
            except asyncio.CancelledError:
                pass
        if self._client is not None:
            await self._client.aclose()
            self._client = None
