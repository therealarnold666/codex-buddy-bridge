from __future__ import annotations

import argparse
import asyncio
import logging

from .daemon import DaemonConfig, main as daemon_main
from .ipc import DEFAULT_SOCKET_PATH
from .opencode_discovery import discover_opencode_url


async def _resolve_opencode_url(raw: str | None) -> str | None:
    if raw is None:
        return None
    if raw.lower() == "auto":
        url = await discover_opencode_url()
        if url:
            return url
        return None
    return raw


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="codex_buddy_bridge",
        description="Daemon bridging Codex/OpenCode to ClaudeCodeBuddy BLE hardware.",
    )
    parser.add_argument("--device-prefix", default="Claude-", help="BLE device name prefix to scan for.")
    parser.add_argument("--address", help="BLE address/identifier (skips scan).")
    parser.add_argument("--socket", default=DEFAULT_SOCKET_PATH, help="Unix socket path for Codex hook IPC.")
    parser.add_argument(
        "--opencode-url",
        default=None,
        help=(
            "OpenCode ACP server URL (e.g. http://127.0.0.1:48337). "
            "Use 'auto' to scan for a running instance."
        ),
    )
    parser.add_argument("--debug", action="store_true", help="Enable verbose logging.")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    opencode_url = asyncio.run(_resolve_opencode_url(args.opencode_url))
    if args.opencode_url == "auto" and opencode_url is None:
        logging.getLogger("codex-buddy").warning(
            "No OpenCode ACP instance discovered (tried port scan, mDNS, state files). "
            "OpenCode mode will be disabled; pass --opencode-url=http://HOST:PORT to enable."
        )

    config = DaemonConfig(
        socket_path=args.socket,
        device_prefix=args.device_prefix,
        address=args.address,
        opencode_url=opencode_url,
    )
    asyncio.run(daemon_main(config))


if __name__ == "__main__":
    main()
