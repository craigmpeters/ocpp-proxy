import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import websockets

LISTEN_PORT = int(os.getenv("LISTEN_PORT", "9000"))
UPSTREAM_BASE = os.getenv("UPSTREAM_BASE", "wss://ocpp.ocpplab.invalid").rstrip("/")
LOG_DIR = Path(os.getenv("LOG_DIR", "/data/logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)

SUBPROTOCOLS = ("ocpp1.6",)

def log(direction: str, path: str, raw: str):
    try:
        payload = json.loads(raw)
    except Exception:
        payload = raw

    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "direction": direction,
        "path": path,
        "payload": payload,
    }

    line = json.dumps(entry, separators=(",", ":"))
    print(line, flush=True)

    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with open(LOG_DIR / f"ocpp-{date}.jsonl", "a") as f:
        f.write(line + "\n")

async def pipe(src, dst, direction, path):
    try:
        async for message in src:
            log(direction, path, message)
            await dst.send(message)
    except websockets.ConnectionClosed:
        pass

async def handler(client_ws):
    path = client_ws.request.path
    upstream_url = f"{UPSTREAM_BASE}{path}"

    print(f"Charger connected: {path} -> {upstream_url}", flush=True)

    try:
        async with websockets.connect(
            upstream_url,
            subprotocols=SUBPROTOCOLS,
        ) as upstream_ws:
            await asyncio.gather(
                pipe(client_ws, upstream_ws, "charger_to_octopus", path),
                pipe(upstream_ws, client_ws, "octopus_to_charger", path),
            )
    except Exception as e:
        print(f"Proxy error for {path}: {e!r}", flush=True)
    finally:
        print(f"Charger disconnected: {path}", flush=True)

async def main():
    async with websockets.serve(
        handler,
        host="0.0.0.0",
        port=LISTEN_PORT,
        subprotocols=SUBPROTOCOLS,
    ):
        print(f"OCPP logging proxy listening on 0.0.0.0:{LISTEN_PORT}", flush=True)
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
