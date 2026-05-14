"""Auto-discover OpenCode TUI's ACP port.

Tries several strategies in order:
1. Port scan (48330–48400 range)
2. mDNS (opencode.local)
3. State file inspection
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path

import httpx

DEFAULT_PORTS = list(range(48330, 48400))

logger = logging.getLogger("codex-buddy.discovery")


async def discover_opencode_url(
    ports: list[int] | None = None, timeout: float = 1.0
) -> str | None:
    """Return the first reachable ACP URL, or None."""
    port_list = ports or DEFAULT_PORTS

    # Strategy 1: port scan
    for port in port_list:
        url = f"http://127.0.0.1:{port}"
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.get(f"{url}/config")
                if resp.status_code == 200:
                    logger.info("Discovered OpenCode ACP at %s", url)
                    return url
        except Exception:  # noqa: BLE001
            pass

    # Strategy 2: mDNS
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get("http://opencode.local/config")
            if resp.status_code == 200:
                logger.info("Discovered OpenCode ACP via mDNS")
                return "http://opencode.local"
    except Exception:  # noqa: BLE001
        pass

    # Strategy 3: state file
    state_dir = Path.home() / ".local" / "state" / "opencode"
    if state_dir.exists():
        for fpath in sorted(state_dir.iterdir()):
            if not fpath.is_file():
                continue
            try:
                text = fpath.read_text(errors="ignore")
                if "http" in text and "127.0.0.1" in text:
                    import re

                    match = re.search(r'http://[^\s"\']+', text)
                    if match:
                        url = match.group()
                        # Verify it's reachable
                        try:
                            async with httpx.AsyncClient(timeout=timeout) as client:
                                resp = await client.get(f"{url}/config")
                                if resp.status_code == 200:
                                    logger.info(
                                        "Discovered OpenCode ACP from state file: %s",
                                        url,
                                    )
                                    return url
                        except Exception:  # noqa: BLE001
                            pass
            except OSError:
                pass

    return None
