#!/usr/bin/env python3
"""
fluID Edge Gateway
──────────────────
Runs on-prem at the plant (industrial PC / Raspberry Pi).

Responsibilities:
  1. Poll sensors on local plant network (Modbus TCP/RTU)
  2. Buffer readings locally if cloud connection drops
  3. Forward readings to fluID cloud API when online
  4. Never expose plant network to internet (outbound-only HTTPS)

Deploy with:
  pip install pymodbus httpx
  python edge/gateway.py --config edge/config.json

Hardware: Advantech UNO-2372G or Raspberry Pi 4 (4GB)
           with DIN rail case + industrial SD card
"""

import asyncio
import json
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import httpx

logger = logging.getLogger(__name__)


class LocalBuffer:
    def __init__(self, db_path: str = "/var/fluid/buffer.db"):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._init_schema()

    def _init_schema(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS buffered_readings (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                sensor_key  TEXT NOT NULL,
                value       REAL NOT NULL,
                raw_value   REAL,
                quality     TEXT DEFAULT 'GOOD',
                timestamp   TEXT NOT NULL,
                forwarded   INTEGER DEFAULT 0
            )
        """)
        self.conn.commit()

    def write(self, readings: list[dict]):
        self.conn.executemany(
            """INSERT INTO buffered_readings
               (sensor_key, value, raw_value, quality, timestamp)
               VALUES (:sensor_key, :value, :raw_value, :quality, :timestamp)""",
            readings,
        )
        self.conn.commit()

    def get_pending(self, limit: int = 500) -> list[dict]:
        cursor = self.conn.execute(
            "SELECT id, sensor_key, value, raw_value, quality, timestamp "
            "FROM buffered_readings WHERE forwarded=0 ORDER BY id LIMIT ?",
            (limit,),
        )
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]

    def mark_forwarded(self, ids: list[int]):
        placeholders = ",".join("?" * len(ids))
        self.conn.execute(
            f"UPDATE buffered_readings SET forwarded=1 WHERE id IN ({placeholders})",
            ids,
        )
        self.conn.commit()

    def pending_count(self) -> int:
        return self.conn.execute(
            "SELECT COUNT(*) FROM buffered_readings WHERE forwarded=0"
        ).fetchone()[0]


async def poll_sensors(sensor_configs: list[dict]) -> list[dict]:
    from pymodbus.client import AsyncModbusTcpClient
    import struct

    readings = []
    for cfg in sensor_configs:
        try:
            client = AsyncModbusTcpClient(
                host=cfg["host"],
                port=cfg.get("port", 502),
                timeout=5,
            )
            await asyncio.wait_for(client.connect(), timeout=5)

            resp = await client.read_holding_registers(
                address=cfg["register_address"],
                count=cfg.get("register_count", 2),
                slave=cfg.get("unit_id", 1),
            )

            if resp.isError():
                logger.warning(f"Modbus error on {cfg['sensor_key']}")
                client.close()
                continue

            if len(resp.registers) >= 2:
                raw_bytes = struct.pack(">HH", resp.registers[0], resp.registers[1])
                raw = struct.unpack(">f", raw_bytes)[0]
            else:
                raw = float(resp.registers[0])

            value = raw * cfg.get("scale_factor", 1.0)
            value = max(cfg["min_range"], min(cfg["max_range"], value))
            value = round(value, cfg.get("decimals", 2))

            readings.append({
                "sensor_key": cfg["sensor_key"],
                "value": value,
                "raw_value": raw,
                "quality": "GOOD",
                "timestamp": datetime.utcnow().isoformat(),
            })

            client.close()

        except asyncio.TimeoutError:
            logger.warning(f"Timeout on sensor {cfg['sensor_key']} @ {cfg['host']}")
        except Exception as e:
            logger.error(f"Error reading {cfg['sensor_key']}: {e}")

    return readings


async def forward_to_cloud(
    buffer: LocalBuffer,
    api_url: str,
    api_key: str,
    plant_id: str,
):
    pending = buffer.get_pending(limit=200)
    if not pending:
        return

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"{api_url}/api/edge/ingest",
                json={"plant_id": plant_id, "readings": pending},
                headers={"X-Edge-Key": api_key},
            )
            resp.raise_for_status()
            ids = [r["id"] for r in pending]
            buffer.mark_forwarded(ids)
            logger.info(f"Forwarded {len(ids)} readings to cloud")

    except httpx.HTTPError as e:
        logger.warning(f"Cloud unreachable, {buffer.pending_count()} readings buffered locally: {e}")


async def run_gateway(config: dict):
    logger.info(f"fluID Edge Gateway starting — plant: {config['plant_name']}")

    buffer = LocalBuffer(config.get("buffer_db", "/var/fluid/buffer.db"))
    poll_interval = config.get("poll_interval_seconds", 30)
    forward_interval = config.get("forward_interval_seconds", 60)
    last_forward = 0.0

    while True:
        try:
            readings = await poll_sensors(config["sensors"])
            if readings:
                buffer.write(readings)
                logger.debug(f"Polled {len(readings)} sensors — buffered locally")

            if time.monotonic() - last_forward >= forward_interval:
                await forward_to_cloud(
                    buffer,
                    config["api_url"],
                    config["api_key"],
                    config["plant_id"],
                )
                last_forward = time.monotonic()

        except Exception:
            logger.exception("Gateway loop error — continuing")

        await asyncio.sleep(poll_interval)


def main():
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
    )

    parser = argparse.ArgumentParser(description="fluID Edge Gateway")
    parser.add_argument(
        "--config",
        default="edge/config.json",
        help="Path to JSON config file",
    )
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        logger.error(f"Config not found: {config_path}")
        sys.exit(1)

    config = json.loads(config_path.read_text())
    asyncio.run(run_gateway(config))


if __name__ == "__main__":
    main()
