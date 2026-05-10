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
from pathlib import Path
import re
import pwd
import signal
import time
from datetime import datetime
from dataclasses import dataclass
from typing import Any, Optional

from . import ipc
from .ble_transport import BleTransport
from .protocol import (
    ApprovalRequest,
    INTERACTIVE_OPTION_LIMIT,
    INTERACTIVE_QUESTION_LIMIT,
    INTERACTIVE_TEXT_LIMIT,
    PROMPT_HINT_LIMIT,
    PROMPT_ID_LIMIT,
    PROMPT_TOOL_LIMIT,
    PermissionDecision,
    InteractivePrompt,
    InteractiveQuestion,
    InteractiveSelection,
    build_clear_snapshot,
    build_owner_frame,
    build_prompt_snapshot,
    build_session_state_snapshot,
    build_time_frame,
    parse_interactive_selection,
    parse_permission_decision,
)

DEFAULT_PERMISSION_WAIT_SECONDS = 105.0  # < hook timeout (115s) in hooks.json
DEFAULT_CONNECT_TIMEOUT_SECONDS = 8.0    # short scan/connect window so the hook
                                         # can give up quickly if Claude has the buddy
DEFAULT_STATE_SYNC_INTERVAL_SECONDS = 12.0
DEFAULT_IDLE_STATE_SYNC_INTERVAL_SECONDS = 180.0
DEFAULT_STATE_SYNC_CONNECT_TIMEOUT_SECONDS = 3.0
DEFAULT_SESSION_RESCAN_INTERVAL_SECONDS = 60.0
DEFAULT_SESSION_SCAN_PATH = os.path.expanduser("~/.codex/sessions")
DEFAULT_TOKEN_LEDGER_PATH = os.path.expanduser("~/.local/state/codex-buddy/token-ledger.json")
DEFAULT_EVENT_RETRY_WINDOW_SECONDS = 20.0
DEFAULT_EVENT_RETRY_DELAY_SECONDS = 5.0
DEFAULT_INTERACTIVE_SCAN_INTERVAL_SECONDS = 1.0
DEFAULT_INTERACTIVE_IDLE_SCAN_INTERVAL_SECONDS = 5.0
DEFAULT_INTERACTIVE_CONNECT_TIMEOUT_SECONDS = 8.0
SESSION_ID_RE = re.compile(r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.jsonl$")


@dataclass(frozen=True)
class InteractiveCallState:
    prompt: InteractivePrompt
    kind: str


@dataclass
class InteractiveSnapshot:
    prompt: InteractivePrompt | None = None
    version: int = 0


class SessionState:
    """Tracks total/running/waiting across session and turn hooks.

    `total` is the number of on-disk Codex session files currently present.
    `running` reflects active turns: UserPromptSubmit marks a turn active and
    Stop clears it again.
    """

    __slots__ = (
        "_active_turn_ids",
        "_anonymous_running",
        "_session_turn_ids",
        "_turn_session_ids",
        "_interactive_waiting",
        "_interactive_waiting_keys",
        "_interactive_waiting_kind",
        "pending_tokens",
        "total_tokens",
        "today_tokens",
        "total",
        "waiting",
    )

    def __init__(self) -> None:
        self.total = 0
        self.pending_tokens = 0
        self.total_tokens = 0
        self.today_tokens = 0
        self.waiting = 0
        self._active_turn_ids: set[str] = set()
        self._session_turn_ids: dict[str, str] = {}
        self._turn_session_ids: dict[str, str] = {}
        self._anonymous_running = 0
        self._interactive_waiting = 0
        self._interactive_waiting_keys: set[str] = set()
        self._interactive_waiting_kind: dict[str, str] = {}

    def on_session_start(self, source: str) -> None:
        if source == "clear":
            self._active_turn_ids.clear()
            self._session_turn_ids.clear()
            self._turn_session_ids.clear()
            self._anonymous_running = 0
            self._interactive_waiting = 0
            self._interactive_waiting_keys.clear()
            self._interactive_waiting_kind.clear()

    def set_total(self, total: int) -> bool:
        total = max(0, total)
        if self.total == total:
            return False
        self.total = total
        return True

    def set_total_tokens(self, tokens: int) -> bool:
        tokens = max(0, tokens)
        if self.total_tokens == tokens:
            return False
        self.total_tokens = tokens
        return True

    def set_today_tokens(self, tokens: int) -> bool:
        tokens = max(0, tokens)
        if self.today_tokens == tokens:
            return False
        self.today_tokens = tokens
        return True

    def add_pending_tokens(self, delta: int, today_total: int | None = None) -> bool:
        delta = max(0, delta)
        if delta == 0:
            return False
        self.pending_tokens += delta
        self.total_tokens += delta
        if today_total is not None:
            self.today_tokens = max(0, today_total)
        return True

    def clear_pending_tokens(self) -> None:
        self.pending_tokens = 0

    def on_user_prompt_submit(self, session_id: str | None, turn_id: str | None) -> bool:
        if turn_id:
            replaced = False
            if session_id:
                previous_turn_id = self._session_turn_ids.get(session_id)
                if previous_turn_id == turn_id:
                    return False
                if previous_turn_id:
                    self._active_turn_ids.discard(previous_turn_id)
                    self._turn_session_ids.pop(previous_turn_id, None)
                    replaced = True
                self._session_turn_ids[session_id] = turn_id
                self._turn_session_ids[turn_id] = session_id
            elif turn_id in self._active_turn_ids:
                return False
            elif turn_id in self._turn_session_ids:
                return False
            if turn_id in self._active_turn_ids and not replaced:
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

    def on_interactive_start(
        self,
        session_id: str | None,
        turn_id: str | None,
        kind: str | None,
    ) -> tuple[bool, str]:
        key = _interactive_key(session_id, turn_id)
        normalized_kind = _normalize_interactive_kind(kind)
        if key in self._interactive_waiting_keys:
            self._interactive_waiting_kind[key] = normalized_kind
            return False, normalized_kind
        self._interactive_waiting_keys.add(key)
        self._interactive_waiting_kind[key] = normalized_kind
        self._interactive_waiting += 1
        return True, normalized_kind

    def on_interactive_end(self, session_id: str | None, turn_id: str | None) -> bool:
        key = _interactive_key(session_id, turn_id)
        if key not in self._interactive_waiting_keys:
            return False
        self._interactive_waiting_keys.discard(key)
        self._interactive_waiting_kind.pop(key, None)
        if self._interactive_waiting > 0:
            self._interactive_waiting -= 1
        return True

    def on_stop(self, session_id: str | None, turn_id: str | None) -> bool:
        if turn_id:
            mapped_session_id = self._turn_session_ids.pop(turn_id, None)
            if mapped_session_id and self._session_turn_ids.get(mapped_session_id) == turn_id:
                self._session_turn_ids.pop(mapped_session_id, None)
            if session_id and self._session_turn_ids.get(session_id) == turn_id:
                self._session_turn_ids.pop(session_id, None)
            if turn_id in self._active_turn_ids:
                self._active_turn_ids.remove(turn_id)
                return True
        if session_id:
            previous_turn_id = self._session_turn_ids.pop(session_id, None)
            if previous_turn_id:
                self._turn_session_ids.pop(previous_turn_id, None)
                self._active_turn_ids.discard(previous_turn_id)
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
    def interactive_waiting(self) -> int:
        return self._interactive_waiting

    @property
    def waiting_out(self) -> int:
        return self.waiting + self._interactive_waiting

    @property
    def interactive_message(self) -> str:
        if not self._interactive_waiting_kind:
            return "input needed"
        if any(kind == "choice" for kind in self._interactive_waiting_kind.values()):
            return "choice needed"
        return "input needed"

    @property
    def is_idle(self) -> bool:
        return self.running == 0 and self.waiting_out == 0


@dataclass
class DaemonConfig:
    socket_path: str
    device_prefix: str
    address: Optional[str]
    permission_wait: float = DEFAULT_PERMISSION_WAIT_SECONDS
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT_SECONDS
    state_sync_interval: float = DEFAULT_STATE_SYNC_INTERVAL_SECONDS
    idle_state_sync_interval: float = DEFAULT_IDLE_STATE_SYNC_INTERVAL_SECONDS
    state_sync_connect_timeout: float = DEFAULT_STATE_SYNC_CONNECT_TIMEOUT_SECONDS
    session_rescan_interval: float = DEFAULT_SESSION_RESCAN_INTERVAL_SECONDS
    session_scan_path: str = DEFAULT_SESSION_SCAN_PATH
    token_ledger_path: str = DEFAULT_TOKEN_LEDGER_PATH
    event_retry_window: float = DEFAULT_EVENT_RETRY_WINDOW_SECONDS
    event_retry_delay: float = DEFAULT_EVENT_RETRY_DELAY_SECONDS
    interactive_scan_interval: float = DEFAULT_INTERACTIVE_SCAN_INTERVAL_SECONDS
    interactive_idle_scan_interval: float = DEFAULT_INTERACTIVE_IDLE_SCAN_INTERVAL_SECONDS
    interactive_connect_timeout: float = DEFAULT_INTERACTIVE_CONNECT_TIMEOUT_SECONDS


class TokenLedger:
    def __init__(self, path: str):
        self.path = Path(path).expanduser()
        self.total_tokens = 0
        self.daily_tokens: dict[str, int] = {}
        self.session_output_totals: dict[str, int] = {}
        self.load()

    def load(self) -> None:
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return
        except (OSError, json.JSONDecodeError):
            return

        total = payload.get("total_tokens")
        daily = payload.get("daily_tokens")
        sessions = payload.get("session_output_totals")
        if isinstance(total, int) and total >= 0:
            self.total_tokens = total
        if isinstance(daily, dict):
            clean_daily: dict[str, int] = {}
            for key, value in daily.items():
                if isinstance(key, str) and isinstance(value, int) and value >= 0:
                    clean_daily[key] = value
            self.daily_tokens = clean_daily
        if isinstance(sessions, dict):
            clean: dict[str, int] = {}
            for key, value in sessions.items():
                if isinstance(key, str) and isinstance(value, int) and value >= 0:
                    clean[key] = value
            self.session_output_totals = clean

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "total_tokens": self.total_tokens,
            "daily_tokens": self.daily_tokens,
            "session_output_totals": self.session_output_totals,
        }
        self.path.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")

    def today_tokens(self) -> int:
        return self.daily_tokens.get(_local_today_key(), 0)

    def record_session_total(self, session_id: str, absolute_total: int) -> int:
        absolute_total = max(0, absolute_total)
        previous = self.session_output_totals.get(session_id, 0)
        if absolute_total <= previous:
            return 0
        delta = absolute_total - previous
        self.session_output_totals[session_id] = absolute_total
        self.total_tokens += delta
        day = _local_today_key()
        self.daily_tokens[day] = self.daily_tokens.get(day, 0) + delta
        self.save()
        return delta


class Daemon:
    def __init__(self, config: DaemonConfig):
        self.config = config
        self._ble_lock = asyncio.Lock()
        self._session = SessionState()
        self._token_ledger = TokenLedger(config.token_ledger_path)
        self._session.set_total_tokens(self._token_ledger.total_tokens)
        self._session.set_today_tokens(self._token_ledger.today_tokens())
        self._server: asyncio.AbstractServer | None = None
        self._state_sync_event = asyncio.Event()
        self._state_sync_task: asyncio.Task[None] | None = None
        self._session_scan_task: asyncio.Task[None] | None = None
        self._last_state_sync_monotonic = 0.0
        self._event_retry_deadline_monotonic = 0.0
        self._stop_event = asyncio.Event()
        self._log = logging.getLogger("codex-buddy.daemon")
        self._session_file_offsets: dict[str, int] = {}
        self._session_turn_by_file: dict[str, str] = {}
        self._interactive_calls: dict[str, InteractiveCallState] = {}
        self._interactive_snapshot = InteractiveSnapshot()
        self._interactive_event = asyncio.Event()
        self._interactive_task: asyncio.Task[None] | None = None

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
        if self._session_scan_task is not None:
            self._session_scan_task.cancel()
            try:
                await self._session_scan_task
            except asyncio.CancelledError:
                pass
        if self._interactive_task is not None:
            self._interactive_task.cancel()
            try:
                await self._interactive_task
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
            if source == "clear":
                self._interactive_calls.clear()
                self._refresh_interactive_snapshot()
            await self._rescan_session_total(trigger_sync=True)
            self._log.debug(
                "Session started: total=%d host_tokens=%d tokens_today=%d running=%d source=%s",
                self._session.total,
                self._session.total_tokens,
                self._session.today_tokens,
                self._session.running,
                source,
            )
            self._request_state_sync()
            return None

        if event == "user_prompt_submit":
            self._ensure_background_tasks()
            session_id = _string_or_none(body.get("session_id"))
            turn_id = _string_or_none(body.get("turn_id"))
            if self._session.on_user_prompt_submit(session_id, turn_id):
                self._log.debug(
                    "Turn started: session=%s turn=%s running=%d waiting_out=%d",
                    session_id,
                    turn_id,
                    self._session.running,
                    self._session.waiting_out,
                )
                self._request_state_sync()
            return None

        if event == "interactive_start":
            self._ensure_background_tasks()
            session_id = _string_or_none(body.get("session_id"))
            turn_id = _string_or_none(body.get("turn_id"))
            kind = _string_or_none(body.get("kind"))
            changed, resolved_kind = self._session.on_interactive_start(session_id, turn_id, kind)
            self._refresh_interactive_snapshot()
            msg = _interactive_message(resolved_kind)
            self._log.debug(
                "Interactive wait start: session=%s turn=%s kind=%s waiting_out=%d running=%d changed=%s",
                session_id,
                turn_id,
                resolved_kind,
                self._session.waiting_out,
                self._session.running,
                changed,
            )
            self._request_state_sync()
            return {"ok": True, "waiting": self._session.waiting_out, "msg": msg}

        if event == "interactive_end":
            self._ensure_background_tasks()
            session_id = _string_or_none(body.get("session_id"))
            turn_id = _string_or_none(body.get("turn_id"))
            changed = self._session.on_interactive_end(session_id, turn_id)
            self._refresh_interactive_snapshot()
            self._log.debug(
                "Interactive wait end: session=%s turn=%s waiting_out=%d running=%d changed=%s",
                session_id,
                turn_id,
                self._session.waiting_out,
                self._session.running,
                changed,
            )
            self._request_state_sync()
            return {"ok": True, "waiting": self._session.waiting_out}

        if event == "stop":
            self._ensure_background_tasks()
            session_id = _string_or_none(body.get("session_id"))
            turn_id = _string_or_none(body.get("turn_id"))
            token_delta = await self._collect_stop_token_delta(session_id)
            if token_delta:
                self._session.add_pending_tokens(token_delta, self._token_ledger.today_tokens())
            if self._session.on_stop(session_id, turn_id):
                self._log.debug(
                    "Turn stopped: session=%s turn=%s total=%d delta_tokens=%d host_tokens=%d tokens_today=%d running=%d waiting_out=%d",
                    session_id,
                    turn_id,
                    self._session.total,
                    token_delta,
                    self._session.total_tokens,
                    self._session.today_tokens,
                    self._session.running,
                    self._session.waiting_out,
                )
                self._request_state_sync()
            elif token_delta:
                self._log.debug(
                    "Turn token update without running change: session=%s delta_tokens=%d host_tokens=%d tokens_today=%d",
                    session_id,
                    token_delta,
                    self._session.total_tokens,
                    self._session.today_tokens,
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
                waiting=self._session.waiting_out,
                total=self._session.total,
                tokens=self._session.pending_tokens,
                tokens_today=self._session.today_tokens,
                msg=self._state_msg(),
                interactive=self._interactive_snapshot.prompt,
            ))
            await transport.write_line(build_prompt_snapshot(
                request,
                running=self._session.running,
                waiting=self._session.waiting_out,
                total=self._session.total,
                tokens=self._session.pending_tokens,
                tokens_today=self._session.today_tokens,
            ))
            self._log.info(
                "Pending approval %s for %s: %s (total=%d token_delta=%d host_tokens=%d tokens_today=%d running=%d waiting=%d)",
                request.id, request.tool, request.hint,
                self._session.total,
                self._session.pending_tokens,
                self._session.total_tokens,
                self._session.today_tokens,
                self._session.running,
                self._session.waiting_out,
            )
            try:
                decision = await asyncio.wait_for(decision_future, timeout=self.config.permission_wait)
            except asyncio.TimeoutError:
                self._log.warning("Approval %s timed out after %.0fs", request.id, self.config.permission_wait)
                self._session.on_approved()
                await transport.write_line(build_session_state_snapshot(
                    running=self._session.running,
                    waiting=self._session.waiting_out,
                    total=self._session.total,
                    tokens=self._session.pending_tokens,
                    tokens_today=self._session.today_tokens,
                    msg=self._state_msg(),
                    interactive=self._interactive_snapshot.prompt,
                ))
                return {"decision": "timeout"}
            self._session.on_approved()
            await transport.write_line(build_session_state_snapshot(
                running=self._session.running,
                waiting=self._session.waiting_out,
                total=self._session.total,
                tokens=self._session.pending_tokens,
                tokens_today=self._session.today_tokens,
                msg=self._state_msg(),
                interactive=self._interactive_snapshot.prompt,
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
        if self._session_scan_task is None or self._session_scan_task.done():
            self._session_scan_task = asyncio.create_task(
                self._session_scan_loop(),
                name="codex-buddy-session-scan",
            )
        if self._interactive_task is None or self._interactive_task.done():
            self._interactive_task = asyncio.create_task(
                self._interactive_loop(),
                name="codex-buddy-interactive",
            )

    def _request_state_sync(self) -> None:
        self._event_retry_deadline_monotonic = max(
            self._event_retry_deadline_monotonic,
            time.monotonic() + self.config.event_retry_window,
        )
        self._state_sync_event.set()

    async def _session_scan_loop(self) -> None:
        await self._rescan_session_total(trigger_sync=True)
        while not self._stop_event.is_set():
            await self._scan_interactive_from_sessions()
            try:
                interval = (
                    self.config.interactive_scan_interval
                    if self._session.running > 0
                    else self.config.interactive_idle_scan_interval
                )
                timeout = min(self.config.session_rescan_interval, interval)
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=timeout,
                )
                break
            except asyncio.TimeoutError:
                pass
            await self._rescan_session_total(trigger_sync=False)

    async def _rescan_session_total(self, trigger_sync: bool) -> None:
        total = await asyncio.to_thread(_count_session_files, self.config.session_scan_path)
        changed = self._session.set_total(total)
        if changed:
            self._log.debug("Session total refreshed from disk: total=%d", total)
            if trigger_sync:
                self._request_state_sync()

    async def _scan_interactive_from_sessions(self) -> None:
        changed = await asyncio.to_thread(
            _scan_interactive_events_from_files,
            self.config.session_scan_path,
            self._session_file_offsets,
            self._session_turn_by_file,
            self._interactive_calls,
            self._session,
            self._log,
        )
        if changed:
            self._refresh_interactive_snapshot()
            self._request_state_sync()

    async def _collect_stop_token_delta(self, session_id: str | None) -> int:
        if not session_id:
            return 0
        session_path = await asyncio.to_thread(_find_session_file, self.config.session_scan_path, session_id)
        if session_path is None:
            self._log.debug("No session file found for token scan: session=%s", session_id)
            return 0
        absolute_total = await asyncio.to_thread(_scan_session_output_tokens, session_path)
        delta = await asyncio.to_thread(self._token_ledger.record_session_total, session_id, absolute_total)
        return delta

    async def _state_sync_loop(self) -> None:
        loop = asyncio.get_running_loop()
        while not self._stop_event.is_set():
            interval = (
                self.config.state_sync_interval
                if self._session.running > 0
                else self.config.idle_state_sync_interval
            )
            elapsed = loop.time() - self._last_state_sync_monotonic
            timeout = max(0.0, interval - elapsed)

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

            reason = "event" if triggered else "heartbeat"
            success = await self._sync_state_once(reason)
            self._last_state_sync_monotonic = loop.time()

            if success:
                self._event_retry_deadline_monotonic = 0.0
                continue

            if reason != "event":
                continue

            if loop.time() >= self._event_retry_deadline_monotonic:
                continue

            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self.config.event_retry_delay,
                )
                break
            except asyncio.TimeoutError:
                self._state_sync_event.set()

    async def _sync_state_once(self, reason: str) -> bool:
        if self._ble_lock.locked():
            self._log.debug("Skipping %s state sync while BLE is busy", reason)
            return False

        sent_pending_tokens = self._session.pending_tokens
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
                return False
            except Exception as exc:  # noqa: BLE001
                self._log.debug("State sync (%s) BLE connect failed: %s", reason, exc)
                return False

            try:
                await self._send_greeting(transport)
                await transport.write_line(build_session_state_snapshot(
                    running=self._session.running,
                    waiting=self._session.waiting_out,
                    total=self._session.total,
                    tokens=self._session.pending_tokens,
                    tokens_today=self._session.today_tokens,
                    msg=self._state_msg(),
                    interactive=self._interactive_snapshot.prompt,
                ))
                self._last_state_sync_monotonic = asyncio.get_running_loop().time()
                self._log.debug(
                    "Pushed state sync (%s): total=%d token_delta=%d host_tokens=%d tokens_today=%d running=%d waiting=%d",
                    reason,
                    self._session.total,
                    sent_pending_tokens,
                    self._session.total_tokens,
                    self._session.today_tokens,
                    self._session.running,
                    self._session.waiting_out,
                )
                if sent_pending_tokens > 0:
                    self._session.clear_pending_tokens()
                return True
            finally:
                try:
                    await transport.close()
                except Exception as exc:  # noqa: BLE001
                    self._log.debug("State sync close() raised: %s", exc)

    async def _interactive_loop(self) -> None:
        while not self._stop_event.is_set():
            prompt = self._interactive_snapshot.prompt
            if prompt is None:
                self._interactive_event.clear()
                try:
                    await asyncio.wait_for(self._interactive_event.wait(), timeout=30.0)
                except asyncio.TimeoutError:
                    pass
                continue

            if self._ble_lock.locked():
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=1.0)
                    break
                except asyncio.TimeoutError:
                    continue

            try:
                await self._run_interactive_session(prompt)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self._log.warning("Interactive session failed for %s: %s", prompt.id, exc)
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=3.0)
                    break
                except asyncio.TimeoutError:
                    pass

    async def _run_interactive_session(self, initial_prompt: InteractivePrompt) -> None:
        selection_future: asyncio.Future[InteractiveSelection] = asyncio.get_running_loop().create_future()

        def on_line(line: bytes):
            return self._consume_interactive_selection(line, selection_future)

        async with self._ble_lock:
            transport = BleTransport(
                device_name_prefix=self.config.device_prefix,
                address=self.config.address,
            )
            try:
                await asyncio.wait_for(
                    transport.connect(on_line, scan_timeout=self.config.interactive_connect_timeout),
                    timeout=self.config.interactive_connect_timeout + 5.0,
                )
            except asyncio.TimeoutError:
                self._log.debug("Interactive BLE connect timed out for %s", initial_prompt.id)
                return
            except Exception as exc:  # noqa: BLE001
                self._log.debug("Interactive BLE connect failed for %s: %s", initial_prompt.id, exc)
                return

            try:
                await self._send_greeting(transport)
                last_version = -1
                while not self._stop_event.is_set():
                    snapshot = self._interactive_snapshot
                    if snapshot.prompt is None:
                        await transport.write_line(
                            build_session_state_snapshot(
                                running=self._session.running,
                                waiting=self._session.waiting_out,
                                total=self._session.total,
                                tokens=self._session.pending_tokens,
                                tokens_today=self._session.today_tokens,
                                msg=self._state_msg(),
                            )
                        )
                        return

                    if snapshot.version != last_version:
                        await transport.write_line(
                            build_session_state_snapshot(
                                running=self._session.running,
                                waiting=self._session.waiting_out,
                                total=self._session.total,
                                tokens=self._session.pending_tokens,
                                tokens_today=self._session.today_tokens,
                                msg=self._state_msg(),
                                interactive=snapshot.prompt,
                            )
                        )
                        last_version = snapshot.version

                    wait_tasks = [
                        asyncio.create_task(self._interactive_event.wait()),
                        asyncio.create_task(self._stop_event.wait()),
                    ]
                    try:
                        done, pending = await asyncio.wait(
                            [selection_future, *wait_tasks],
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                    finally:
                        for task in wait_tasks:
                            if not task.done():
                                task.cancel()
                    if selection_future in done:
                        selection = selection_future.result()
                        if selection.id != snapshot.prompt.id:
                            selection_future = asyncio.get_running_loop().create_future()
                            continue
                        success = await self._submit_interactive_selection(snapshot.prompt, selection)
                        if success:
                            self._clear_interactive_prompt(snapshot.prompt.call_id, optimistic=True)
                        return
                    if self._stop_event.is_set():
                        return
                    if self._interactive_event.is_set():
                        self._interactive_event.clear()
            finally:
                try:
                    await transport.close()
                except Exception as exc:  # noqa: BLE001
                    self._log.debug("Interactive close() raised: %s", exc)

    async def _consume_interactive_selection(
        self,
        line: bytes,
        selection_future: "asyncio.Future[InteractiveSelection]",
    ) -> None:
        selection = parse_interactive_selection(line)
        if selection is None:
            return
        if not selection_future.done():
            selection_future.set_result(selection)

    async def _submit_interactive_selection(
        self,
        prompt: InteractivePrompt,
        selection: InteractiveSelection,
    ) -> bool:
        if len(selection.answers) != len(prompt.questions):
            self._log.warning(
                "Interactive selection answer count mismatch: prompt=%s expected=%d got=%d",
                prompt.id,
                len(prompt.questions),
                len(selection.answers),
            )
            return False

        result: dict[str, Any] = {"answers": {}}
        for answer_idx, question in zip(selection.answers, prompt.questions, strict=True):
            if answer_idx < 0 or answer_idx >= len(question.options):
                self._log.warning(
                    "Interactive selection index out of range: prompt=%s question=%s idx=%d options=%d",
                    prompt.id,
                    question.id,
                    answer_idx,
                    len(question.options),
                )
                return False
            result["answers"][question.id] = {"answers": [question.options[answer_idx]]}

        payload = {
            "jsonrpc": "2.0",
            "id": int(time.time() * 1000),
            "method": "thread/inject_items",
            "params": {
                "threadId": prompt.thread_id,
                "items": [
                    {
                        "type": "function_call_output",
                        "call_id": prompt.call_id,
                        "output": json.dumps(result, separators=(",", ":"), ensure_ascii=False),
                    }
                ],
            },
        }
        ok = await self._send_app_server_request(payload)
        if ok:
            self._log.info("Interactive selection submitted: prompt=%s turn=%s", prompt.id, prompt.turn_id)
        return ok

    async def _send_app_server_request(self, payload: dict[str, Any]) -> bool:
        proc = await asyncio.create_subprocess_exec(
            "codex",
            "app-server",
            "proxy",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        request = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8") + b"\n"
        stdout, stderr = await proc.communicate(request)
        if proc.returncode != 0:
            self._log.warning("app-server proxy failed (%s): %s", proc.returncode, stderr.decode("utf-8", "ignore").strip())
            return False
        text = stdout.decode("utf-8", "ignore").strip()
        if not text:
            return False
        try:
            response = json.loads(text.splitlines()[-1])
        except json.JSONDecodeError:
            self._log.warning("Invalid app-server proxy response: %r", text)
            return False
        if response.get("error"):
            self._log.warning("app-server proxy returned error: %s", response["error"])
            return False
        return "result" in response

    def _refresh_interactive_snapshot(self) -> None:
        latest = None
        if self._interactive_calls:
            latest = next(reversed(self._interactive_calls.items()))[1]
        latest_prompt = latest.prompt if latest is not None else None
        current = self._interactive_snapshot.prompt
        if current == latest_prompt:
            return
        self._interactive_snapshot = InteractiveSnapshot(
            prompt=latest_prompt,
            version=self._interactive_snapshot.version + 1,
        )
        self._interactive_event.set()

    def _clear_interactive_prompt(self, call_id: str, optimistic: bool = False) -> None:
        state = self._interactive_calls.pop(call_id, None)
        if state is None:
            self._refresh_interactive_snapshot()
            return
        self._session.on_interactive_end(state.prompt.session_id, state.prompt.turn_id)
        if optimistic:
            self._log.debug(
                "Interactive prompt cleared optimistically: session=%s turn=%s call=%s waiting_out=%d",
                state.prompt.session_id,
                state.prompt.turn_id,
                call_id,
                self._session.waiting_out,
            )
        self._refresh_interactive_snapshot()
        self._request_state_sync()

    def _state_msg(self) -> str:
        if self._session.waiting > 0:
            return "approve: tool"
        if self._session.interactive_waiting > 0:
            return self._session.interactive_message
        return "Codex running" if self._session.running else "Codex idle"


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


def _normalize_interactive_kind(kind: str | None) -> str:
    if kind == "choice":
        return "choice"
    return "input"


def _interactive_message(kind: str) -> str:
    if kind == "choice":
        return "choice needed"
    return "input needed"


def _interactive_key(session_id: str | None, turn_id: str | None) -> str:
    if turn_id:
        return f"turn:{turn_id}"
    if session_id:
        return f"session:{session_id}"
    return "anon"


def _count_session_files(scan_path: str) -> int:
    root = Path(scan_path).expanduser()
    if not root.exists():
        return 0
    return sum(1 for path in root.rglob("*.jsonl") if path.is_file())


def _scan_session_output_tokens(path: Path) -> int:
    best = 0
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                if '"type":"token_count"' not in line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                payload = obj.get("payload")
                if not isinstance(payload, dict) or payload.get("type") != "token_count":
                    continue
                info = payload.get("info")
                if not isinstance(info, dict):
                    continue
                total_usage = info.get("total_token_usage")
                if not isinstance(total_usage, dict):
                    continue
                output_tokens = total_usage.get("output_tokens")
                if isinstance(output_tokens, int) and output_tokens > best:
                    best = output_tokens
    except OSError:
        return 0
    return best


def _find_session_file(scan_path: str, session_id: str) -> Path | None:
    root = Path(scan_path).expanduser()
    if not root.exists():
        return None
    matches = sorted(root.rglob(f"*{session_id}*.jsonl"))
    return matches[-1] if matches else None


def _scan_interactive_events_from_files(
    scan_path: str,
    offsets: dict[str, int],
    turn_by_file: dict[str, str],
    interactive_calls: dict[str, InteractiveCallState],
    session_state: SessionState,
    log: logging.Logger,
) -> bool:
    root = Path(scan_path).expanduser()
    if not root.exists():
        return False
    changed = False
    files = [str(path) for path in root.rglob("*.jsonl") if path.is_file()]
    known = set(offsets.keys())
    current = set(files)
    for removed in known - current:
        offsets.pop(removed, None)
        turn_by_file.pop(removed, None)
    for fp in files:
        session_id = _session_id_from_path(fp)
        if session_id is None:
            continue
        try:
            with open(fp, "r", encoding="utf-8", errors="ignore") as handle:
                size = handle.seek(0, os.SEEK_END)
                if fp not in offsets:
                    offsets[fp] = size
                    continue
                offset = offsets.get(fp, 0)
                if offset > size:
                    offset = 0
                handle.seek(offset)
                for raw in handle:
                    line = raw.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if _consume_interactive_event(
                        obj,
                        session_id,
                        fp,
                        turn_by_file,
                        interactive_calls,
                        session_state,
                        log,
                    ):
                        changed = True
                offsets[fp] = handle.tell()
        except OSError:
            continue
    return changed


def _consume_interactive_event(
    obj: dict[str, Any],
    session_id: str,
    file_path: str,
    turn_by_file: dict[str, str],
    interactive_calls: dict[str, InteractiveCallState],
    session_state: SessionState,
    log: logging.Logger,
) -> bool:
    changed = False
    entry_type = obj.get("type")
    payload = obj.get("payload")
    if not isinstance(payload, dict):
        return False
    payload_type = payload.get("type")

    turn_id = _string_or_none(payload.get("turn_id"))
    if turn_id and payload_type in {"task_started", "turn_started"}:
        previous = turn_by_file.get(file_path)
        if previous and previous != turn_id:
            changed = _end_interactive_for_turn(previous, interactive_calls, session_state, log) or changed
        turn_by_file[file_path] = turn_id
    elif entry_type == "turn_context" and turn_id:
        previous = turn_by_file.get(file_path)
        if previous and previous != turn_id:
            changed = _end_interactive_for_turn(previous, interactive_calls, session_state, log) or changed
        turn_by_file[file_path] = turn_id

    if entry_type == "response_item" and payload_type == "function_call":
        name = payload.get("name")
        call_id = _string_or_none(payload.get("call_id"))
        if name == "request_user_input" and call_id:
            turn = turn_by_file.get(file_path)
            prompt = _interactive_prompt_from_payload(session_id, turn, call_id, payload)
            if prompt is None:
                return changed
            kind = "choice" if any(question.options for question in prompt.questions) else "input"
            previous_call_id = next(
                (
                    existing_call_id
                    for existing_call_id, state in interactive_calls.items()
                    if state.prompt.turn_id == prompt.turn_id
                ),
                None,
            )
            if previous_call_id and previous_call_id != call_id:
                interactive_calls.pop(previous_call_id, None)
            interactive_calls[call_id] = InteractiveCallState(prompt=prompt, kind=kind)
            did_change, kind = session_state.on_interactive_start(session_id, turn, kind)
            changed = True
            if did_change:
                log.debug(
                    "Interactive inferred start: session=%s turn=%s call=%s kind=%s waiting_out=%d questions=%d",
                    session_id,
                    turn,
                    call_id,
                    kind,
                    session_state.waiting_out,
                    len(prompt.questions),
                )
                changed = True

    if entry_type == "response_item" and payload_type == "function_call_output":
        call_id = _string_or_none(payload.get("call_id"))
        if call_id and call_id in interactive_calls:
            state = interactive_calls.pop(call_id)
            did_change = session_state.on_interactive_end(state.prompt.session_id, state.prompt.turn_id)
            if did_change:
                log.debug(
                    "Interactive inferred end (function output): session=%s turn=%s call=%s waiting_out=%d",
                    state.prompt.session_id,
                    state.prompt.turn_id,
                    call_id,
                    session_state.waiting_out,
                )
            changed = True

    if entry_type == "event_msg" and payload_type in {"turn_aborted", "task_complete", "turn_completed"}:
        if turn_id:
            changed = _end_interactive_for_turn(turn_id, interactive_calls, session_state, log) or changed

    if entry_type == "response_item" and payload_type == "message" and payload.get("role") == "user":
        active_turn = turn_by_file.get(file_path)
        if active_turn:
            changed = _end_interactive_for_turn(active_turn, interactive_calls, session_state, log) or changed

    return changed


def _end_interactive_for_turn(
    turn_id: str,
    interactive_calls: dict[str, InteractiveCallState],
    session_state: SessionState,
    log: logging.Logger,
) -> bool:
    changed = False
    matched_calls = [call_id for call_id, state in interactive_calls.items() if state.prompt.turn_id == turn_id]
    for call_id in matched_calls:
        state = interactive_calls.pop(call_id)
        if session_state.on_interactive_end(state.prompt.session_id, state.prompt.turn_id):
            log.debug(
                "Interactive inferred end (turn finished): session=%s turn=%s call=%s waiting_out=%d",
                state.prompt.session_id,
                state.prompt.turn_id,
                call_id,
                session_state.waiting_out,
            )
            changed = True
    return changed


def _interactive_prompt_from_payload(
    session_id: str,
    turn_id: str | None,
    call_id: str,
    payload: dict[str, Any],
) -> InteractivePrompt | None:
    if not turn_id:
        return None
    arguments = payload.get("arguments")
    if not isinstance(arguments, str) or not arguments.strip():
        return None
    try:
        raw = json.loads(arguments)
    except json.JSONDecodeError:
        return None
    if not isinstance(raw, dict):
        return None
    thread_id = _string_or_none(raw.get("threadId")) or session_id
    questions_raw = raw.get("questions")
    if not isinstance(questions_raw, list):
        return None

    questions: list[InteractiveQuestion] = []
    for question_raw in questions_raw[:INTERACTIVE_QUESTION_LIMIT]:
        if not isinstance(question_raw, dict):
            continue
        question_id = _string_or_none(question_raw.get("id"))
        question_text = _string_or_none(question_raw.get("question"))
        if not question_id or not question_text:
            continue
        header = _string_or_none(question_raw.get("header")) or "Input"
        options_raw = question_raw.get("options")
        options = _normalize_interactive_options(options_raw)
        questions.append(
            InteractiveQuestion(
                id=question_id,
                header=" ".join(header.split()),
                question=" ".join(question_text.split()),
                options=tuple(options),
            )
        )
    if not questions:
        return None
    return InteractivePrompt(
        id=_interactive_prompt_id(turn_id, call_id),
        call_id=call_id,
        thread_id=thread_id,
        turn_id=turn_id,
        session_id=session_id,
        questions=tuple(questions),
    )


def _normalize_interactive_options(options_raw: Any) -> list[str]:
    if not isinstance(options_raw, list):
        return []
    normalized: list[str] = []
    for option_raw in options_raw[: INTERACTIVE_OPTION_LIMIT + 1]:
        if not isinstance(option_raw, dict):
            continue
        label = _string_or_none(option_raw.get("label"))
        if not label:
            continue
        normalized.append(" ".join(label.split())[:INTERACTIVE_TEXT_LIMIT].rstrip())
    while normalized and not normalized[-1].strip():
        normalized.pop()
    if normalized and len(normalized) > INTERACTIVE_OPTION_LIMIT:
        normalized = normalized[:INTERACTIVE_OPTION_LIMIT]
    return normalized


def _interactive_prompt_id(turn_id: str, call_id: str) -> str:
    turn_short = turn_id[-8:] if len(turn_id) > 8 else turn_id
    call_short = call_id[-8:] if len(call_id) > 8 else call_id
    return f"i-{turn_short}-{call_short}"[:31]


def _session_id_from_path(path: str) -> str | None:
    m = SESSION_ID_RE.search(path)
    if not m:
        return None
    return m.group(1)


def _local_today_key() -> str:
    return datetime.now().astimezone().date().isoformat()


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
