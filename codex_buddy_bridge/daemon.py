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
DEFAULT_IDLE_STATE_SYNC_INTERVAL_SECONDS = 60.0
DEFAULT_STATE_SYNC_CONNECT_TIMEOUT_SECONDS = 3.0
DEFAULT_SESSION_RESCAN_INTERVAL_SECONDS = 60.0
DEFAULT_SESSION_SCAN_PATH = os.path.expanduser("~/.codex/sessions")
DEFAULT_TOKEN_LEDGER_PATH = os.path.expanduser("~/.local/state/codex-buddy/token-ledger.json")


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

    def on_session_start(self, source: str) -> None:
        if source == "clear":
            self._active_turn_ids.clear()
            self._session_turn_ids.clear()
            self._turn_session_ids.clear()
            self._anonymous_running = 0

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
    idle_state_sync_interval: float = DEFAULT_IDLE_STATE_SYNC_INTERVAL_SECONDS
    state_sync_connect_timeout: float = DEFAULT_STATE_SYNC_CONNECT_TIMEOUT_SECONDS
    session_rescan_interval: float = DEFAULT_SESSION_RESCAN_INTERVAL_SECONDS
    session_scan_path: str = DEFAULT_SESSION_SCAN_PATH
    token_ledger_path: str = DEFAULT_TOKEN_LEDGER_PATH


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
        if self._session_scan_task is not None:
            self._session_scan_task.cancel()
            try:
                await self._session_scan_task
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
                    "Turn started: session=%s turn=%s running=%d",
                    session_id,
                    turn_id,
                    self._session.running,
                )
                self._request_state_sync()
            return None

        if event == "stop":
            self._ensure_background_tasks()
            session_id = _string_or_none(body.get("session_id"))
            turn_id = _string_or_none(body.get("turn_id"))
            token_delta = await self._collect_stop_token_delta(session_id)
            if token_delta:
                self._session.add_pending_tokens(token_delta, self._token_ledger.today_tokens())
            if self._session.on_stop(session_id, turn_id):
                self._log.debug(
                    "Turn stopped: session=%s turn=%s total=%d delta_tokens=%d host_tokens=%d tokens_today=%d running=%d",
                    session_id,
                    turn_id,
                    self._session.total,
                    token_delta,
                    self._session.total_tokens,
                    self._session.today_tokens,
                    self._session.running,
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
                waiting=self._session.waiting,
                total=self._session.total,
                tokens=self._session.pending_tokens,
                tokens_today=self._session.today_tokens,
            ))
            await transport.write_line(build_prompt_snapshot(
                request,
                running=self._session.running,
                waiting=self._session.waiting,
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
                self._session.waiting,
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
                    tokens=self._session.pending_tokens,
                    tokens_today=self._session.today_tokens,
                ))
                return {"decision": "timeout"}
            self._session.on_approved()
            await transport.write_line(build_session_state_snapshot(
                running=self._session.running,
                waiting=self._session.waiting,
                total=self._session.total,
                tokens=self._session.pending_tokens,
                tokens_today=self._session.today_tokens,
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

    def _request_state_sync(self) -> None:
        self._state_sync_event.set()

    async def _session_scan_loop(self) -> None:
        await self._rescan_session_total(trigger_sync=True)
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self.config.session_rescan_interval,
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
            await self._sync_state_once(reason)

    async def _sync_state_once(self, reason: str) -> None:
        if self._ble_lock.locked():
            self._log.debug("Skipping %s state sync while BLE is busy", reason)
            return

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
                    tokens=self._session.pending_tokens,
                    tokens_today=self._session.today_tokens,
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
                    self._session.waiting,
                )
                if sent_pending_tokens > 0:
                    self._session.clear_pending_tokens()
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
