"""On-demand bridge daemon.

The IPC server stays up persistently (cheap, doesn't touch BLE). The buddy
peripheral only supports one BLE central at a time, so we *do not* hold a
persistent BLE connection — that would lock out the Claude Hardware Buddy.
Instead, BLE is acquired per-approval:

    permission_request → lock → connect → time/owner/prompt frames →
    await decision (or timeout) → send clear → disconnect → release lock.

If acquiring BLE fails (Claude has the device, advertising is paused, etc.),
the hook gets `no_buddy` and Codex falls back to its native approval prompt.
"""

from __future__ import annotations

import asyncio
import getpass
import hashlib
import json
import logging
import os
import pwd
import signal
import time
from dataclasses import dataclass
from typing import Any, Optional

from . import ipc
from .ble_transport import BleTransport
from .protocol import (
    ApprovalRequest,
    PROMPT_HINT_LIMIT,
    PROMPT_ID_LIMIT,
    PROMPT_TOOL_LIMIT,
    PermissionDecision,
    build_clear_snapshot,
    build_owner_frame,
    build_prompt_snapshot,
    build_session_state_snapshot,
    build_time_frame,
    parse_permission_decision,
)

DEFAULT_PERMISSION_WAIT_SECONDS = 105.0  # < hook timeout (115s) in hooks.json
DEFAULT_CONNECT_TIMEOUT_SECONDS = 8.0    # short scan/connect window so the hook
                                         # can give up quickly if Claude has the buddy
DEFAULT_STATE_SYNC_INTERVAL_SECONDS = 12.0
DEFAULT_STATE_SYNC_CONNECT_TIMEOUT_SECONDS = 3.0


class SessionState:
    """Tracks total/running/waiting across session and turn hooks.

    `total` is the number of sessions started during this daemon lifetime.
    `running` reflects active turns: UserPromptSubmit marks a turn active and
    Stop clears it again.
    """

    __slots__ = ("_active_turn_ids", "_anonymous_running", "total", "waiting")

    def __init__(self) -> None:
        self.total = 0
        self.waiting = 0
        self._active_turn_ids: set[str] = set()
        self._anonymous_running = 0

    def on_session_start(self, source: str) -> None:
        self.total += 1
        if source == "clear":
            self._active_turn_ids.clear()
            self._anonymous_running = 0

    def on_user_prompt_submit(self, turn_id: str | None) -> bool:
        if turn_id:
            if turn_id in self._active_turn_ids:
                return False
            self._active_turn_ids.add(turn_id)
            return True
        self._anonymous_running += 1
        return True

    def on_waiting(self) -> None:
        self.waiting += 1

    def on_approved(self) -> None:
        if self.waiting > 0:
            self.waiting -= 1

    def on_stop(self, turn_id: str | None) -> bool:
        if turn_id and turn_id in self._active_turn_ids:
            self._active_turn_ids.remove(turn_id)
            return True
        if self._anonymous_running > 0:
            self._anonymous_running -= 1
            return True
        if self._active_turn_ids:
            self._active_turn_ids.pop()
            return True
        return False

    @property
    def running(self) -> int:
        return len(self._active_turn_ids) + self._anonymous_running

    @property
    def is_idle(self) -> bool:
        return self.running == 0 and self.waiting == 0


@dataclass
class DaemonConfig:
    socket_path: str
    device_prefix: str
    address: Optional[str]
    permission_wait: float = DEFAULT_PERMISSION_WAIT_SECONDS
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT_SECONDS
    state_sync_interval: float = DEFAULT_STATE_SYNC_INTERVAL_SECONDS
    state_sync_connect_timeout: float = DEFAULT_STATE_SYNC_CONNECT_TIMEOUT_SECONDS


class Daemon:
    def __init__(self, config: DaemonConfig):
        self.config = config
        self._ble_lock = asyncio.Lock()
        self._session = SessionState()
        self._server: asyncio.AbstractServer | None = None
        self._state_sync_event = asyncio.Event()
        self._state_sync_task: asyncio.Task[None] | None = None
        self._last_state_sync_monotonic = 0.0
        self._stop_event = asyncio.Event()
        self._log = logging.getLogger("codex-buddy.daemon")

    async def run(self) -> None:
        self._server = await ipc.serve(self.config.socket_path, self._handle_event)
        self._ensure_background_tasks()

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._stop_event.set)
            except NotImplementedError:
                pass

        self._log.info(
            "Daemon ready (on-demand BLE): socket=%s device_prefix=%s",
            self.config.socket_path,
            self.config.device_prefix,
        )
        try:
            await self._stop_event.wait()
        finally:
            await self._shutdown()

    async def _shutdown(self) -> None:
        self._log.info("Daemon shutting down")
        if self._state_sync_task is not None:
            self._state_sync_task.cancel()
            try:
                await self._state_sync_task
            except asyncio.CancelledError:
                pass
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
        try:
            os.unlink(self.config.socket_path)
        except FileNotFoundError:
            pass

    async def _handle_event(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        event = payload.get("event")
        body = payload.get("payload") or {}
        if not isinstance(body, dict):
            body = {}

        if event == "permission_request":
            return await self._handle_permission_request(body)

        if event == "session_start":
            self._ensure_background_tasks()
            source = body.get("source", "startup")
            self._session.on_session_start(source)
            self._log.debug(
                "Session started: total=%d running=%d source=%s",
                self._session.total, self._session.running, source,
            )
            self._request_state_sync()
            return None

        if event == "user_prompt_submit":
            self._ensure_background_tasks()
            turn_id = _string_or_none(body.get("turn_id"))
            if self._session.on_user_prompt_submit(turn_id):
                self._log.debug(
                    "Turn started: session=%s turn=%s running=%d",
                    body.get("session_id"),
                    turn_id,
                    self._session.running,
                )
                self._request_state_sync()
            return None

        if event == "stop":
            self._ensure_background_tasks()
            turn_id = _string_or_none(body.get("turn_id"))
            if self._session.on_stop(turn_id):
                self._log.debug(
                    "Turn stopped: session=%s turn=%s total=%d running=%d",
                    body.get("session_id"),
                    turn_id,
                    self._session.total,
                    self._session.running,
                )
                self._request_state_sync()
            return None

        return None

    async def _handle_permission_request(self, body: dict[str, Any]) -> dict[str, Any]:
        request = _request_from_payload(body)
        # Serialize: only one BLE session at a time. A second request waits
        # behind the first; if the queue exceeds the hook timeout (115s),
        # the second hook gives up and Codex falls back to native UI.
        async with self._ble_lock:
            return await self._run_approval(request)

    async def _run_approval(self, request: ApprovalRequest) -> dict[str, Any]:
        transport = BleTransport(
            device_name_prefix=self.config.device_prefix,
            address=self.config.address,
        )
        loop = asyncio.get_running_loop()
        decision_future: asyncio.Future[PermissionDecision] = loop.create_future()

        def on_line(line: bytes):
            return self._consume_decision(line, request.id, decision_future)

        try:
            await asyncio.wait_for(
                transport.connect(on_line, scan_timeout=self.config.connect_timeout),
                timeout=self.config.connect_timeout + 5.0,
            )
        except asyncio.TimeoutError:
            self._log.warning("BLE connect timed out for %s", request.id)
            return {"decision": "no_buddy", "reason": "BLE connect timed out"}
        except Exception as exc:  # noqa: BLE001 - Claude may hold the device
            self._log.warning("BLE connect failed for %s: %s", request.id, exc)
            return {"decision": "no_buddy", "reason": str(exc)}

        try:
            await self._send_greeting(transport)
            self._session.on_waiting()
            await transport.write_line(build_session_state_snapshot(
                running=self._session.running,
                waiting=self._session.waiting,
                total=self._session.total,
            ))
            await transport.write_line(build_prompt_snapshot(
                request,
                running=self._session.running,
                waiting=self._session.waiting,
                total=self._session.total,
            ))
            self._log.info(
                "Pending approval %s for %s: %s (total=%d running=%d waiting=%d)",
                request.id, request.tool, request.hint,
                self._session.total, self._session.running, self._session.waiting,
            )
            try:
                decision = await asyncio.wait_for(decision_future, timeout=self.config.permission_wait)
            except asyncio.TimeoutError:
                self._log.warning("Approval %s timed out after %.0fs", request.id, self.config.permission_wait)
                self._session.on_approved()
                await transport.write_line(build_session_state_snapshot(
                    running=self._session.running,
                    waiting=self._session.waiting,
                    total=self._session.total,
                ))
                return {"decision": "timeout"}
            self._session.on_approved()
            await transport.write_line(build_session_state_snapshot(
                running=self._session.running,
                waiting=self._session.waiting,
                total=self._session.total,
            ))
            # Final clear if no more sessions active
            if self._session.is_idle:
                await transport.write_line(build_session_state_snapshot())
            return {
                "decision": "allow" if decision is PermissionDecision.APPROVE_ONCE else "deny",
                "request_id": request.id,
            }
        finally:
            try:
                await transport.close()
                self._log.info("Released BLE for %s", request.id)
            except Exception as exc:  # noqa: BLE001
                self._log.debug("close() raised: %s", exc)
            self._request_state_sync()

    async def _consume_decision(
        self,
        line: bytes,
        expected_id: str,
        decision_future: "asyncio.Future[PermissionDecision]",
    ) -> None:
        decision = parse_permission_decision(line)
        if decision is None:
            return
        if decision.id != expected_id:
            self._log.warning(
                "Buddy decision id mismatch: got %r expected %r", decision.id, expected_id
            )
            return
        if not decision_future.done():
            decision_future.set_result(decision.decision)

    async def _send_greeting(self, transport: BleTransport) -> None:
        epoch = int(time.time())
        tz_offset = -time.timezone if time.daylight == 0 else -time.altzone
        try:
            await transport.write_line(build_time_frame(epoch, tz_offset))
            await transport.write_line(build_owner_frame(_owner_name()))
        except Exception as exc:  # noqa: BLE001
            self._log.debug("Greeting frames failed (non-fatal): %s", exc)

    def _ensure_background_tasks(self) -> None:
        if self._state_sync_task is None or self._state_sync_task.done():
            self._state_sync_task = asyncio.create_task(
                self._state_sync_loop(),
                name="codex-buddy-state-sync",
            )

    def _request_state_sync(self) -> None:
        self._state_sync_event.set()

    async def _state_sync_loop(self) -> None:
        loop = asyncio.get_running_loop()
        while not self._stop_event.is_set():
            timeout = None
            if self._session.running > 0:
                elapsed = loop.time() - self._last_state_sync_monotonic
                timeout = max(0.0, self.config.state_sync_interval - elapsed)

            triggered = False
            try:
                await asyncio.wait_for(self._state_sync_event.wait(), timeout=timeout)
                triggered = True
            except asyncio.TimeoutError:
                pass

            if triggered:
                self._state_sync_event.clear()

            if self._stop_event.is_set():
                break

            if not triggered and self._session.running <= 0:
                continue

            reason = "event" if triggered else "heartbeat"
            await self._sync_state_once(reason)

    async def _sync_state_once(self, reason: str) -> None:
        if self._ble_lock.locked():
            self._log.debug("Skipping %s state sync while BLE is busy", reason)
            return

        async with self._ble_lock:
            transport = BleTransport(
                device_name_prefix=self.config.device_prefix,
                address=self.config.address,
            )
            try:
                await asyncio.wait_for(
                    transport.connect(lambda _line: None, scan_timeout=self.config.state_sync_connect_timeout),
                    timeout=self.config.state_sync_connect_timeout + 3.0,
                )
            except asyncio.TimeoutError:
                self._log.debug("State sync (%s) BLE connect timed out", reason)
                return
            except Exception as exc:  # noqa: BLE001
                self._log.debug("State sync (%s) BLE connect failed: %s", reason, exc)
                return

            try:
                await self._send_greeting(transport)
                await transport.write_line(build_session_state_snapshot(
                    running=self._session.running,
                    waiting=self._session.waiting,
                    total=self._session.total,
                ))
                self._last_state_sync_monotonic = asyncio.get_running_loop().time()
                self._log.debug(
                    "Pushed state sync (%s): total=%d running=%d waiting=%d",
                    reason,
                    self._session.total,
                    self._session.running,
                    self._session.waiting,
                )
            finally:
                try:
                    await transport.close()
                except Exception as exc:  # noqa: BLE001
                    self._log.debug("State sync close() raised: %s", exc)


def _request_from_payload(body: dict[str, Any]) -> ApprovalRequest:
    turn_id = str(body.get("turn_id") or body.get("session_id") or "unknown")
    tool_name = str(body.get("tool_name") or "tool")
    tool_input = body.get("tool_input") or {}
    if not isinstance(tool_input, dict):
        tool_input = {"raw": str(tool_input)}

    digest_src = json.dumps(tool_input, sort_keys=True, default=str).encode("utf-8")
    digest = hashlib.sha1(digest_src).hexdigest()[:6]
    # Firmware caps promptId at 39 chars (src/data.h: char promptId[40]). Keep
    # the last 16 of turn_id (UUID suffix is unique enough) plus 6-char input
    # hash so the same turn issuing distinct tool calls still gets a unique id.
    turn_short = turn_id[-16:] if len(turn_id) > 16 else turn_id
    rid = f"c-{turn_short}-{digest}"
    if len(rid) > PROMPT_ID_LIMIT:
        rid = rid[:PROMPT_ID_LIMIT]

    hint = (
        tool_input.get("description")
        or tool_input.get("command")
        or tool_input.get("path")
        or tool_input.get("query")
        or _first_string_value(tool_input)
        or tool_name
    )
    hint = " ".join(str(hint).split())

    return ApprovalRequest(
        id=rid,
        tool=tool_name[:PROMPT_TOOL_LIMIT],
        hint=hint[:PROMPT_HINT_LIMIT],
    )


def _first_string_value(d: dict[str, Any]) -> str | None:
    for v in d.values():
        if isinstance(v, str) and v.strip():
            return v
    return None


def _string_or_none(value: Any) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None


def _owner_name() -> str:
    try:
        gecos = pwd.getpwuid(os.getuid()).pw_gecos
        first = (gecos or "").split(",", 1)[0].strip()
        if first:
            return first.split(" ", 1)[0]
    except Exception:  # noqa: BLE001
        pass
    return getpass.getuser() or "Codex"


async def main(config: DaemonConfig) -> None:
    daemon = Daemon(config)
    await daemon.run()
