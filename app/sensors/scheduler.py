"""
Sensor polling scheduler for fluID.
"""

import logging
import hashlib
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.db.models import SensorConfig, SensorReading, AuditLog, AlertLevel
from app.db.session import AsyncSessionLocal
from app.sensors.readers import SensorReaderFactory
from app.alerts.engine import AlertEngine

logger = logging.getLogger(__name__)
settings = get_settings()

_scheduler: AsyncIOScheduler | None = None
_alert_engine = AlertEngine()


async def _poll_all_sensors():
    async with AsyncSessionLocal() as db:
        try:
            result = await db.execute(
                select(SensorConfig).where(SensorConfig.is_active == True)
            )
            sensors = result.scalars().all()

            if not sensors:
                logger.debug("No active sensors configured")
                return

            readings = []
            for sensor in sensors:
                reader = SensorReaderFactory.create(sensor)
                result = await reader.read()

                if result.quality == "BAD":
                    logger.warning(
                        f"Bad read on {sensor.label}: {result.error}"
                    )
                    await _write_audit(db, sensor.plant_id, AlertLevel.WARN,
                        "SENSOR", f"{sensor.label} read failure: {result.error}")
                    continue

                reading = SensorReading(
                    time=result.timestamp,
                    sensor_id=sensor.id,
                    value=result.value,
                    raw_value=result.raw_value,
                    quality=result.quality,
                )
                db.add(reading)
                readings.append((sensor, result))

            await db.flush()
            await _alert_engine.evaluate(db, readings)
            await db.commit()
            logger.debug(f"Poll complete — {len(readings)} readings persisted")

        except Exception:
            logger.exception("Polling job failed")
            await db.rollback()


async def _write_audit(
    db: AsyncSession,
    plant_id,
    level: AlertLevel,
    category: str,
    message: str,
):
    now = datetime.utcnow()
    checksum_input = f"{now.isoformat()}|{message}"
    checksum = hashlib.sha256(checksum_input.encode()).hexdigest()

    entry = AuditLog(
        time=now,
        plant_id=plant_id,
        level=level,
        category=category,
        message=message,
        checksum=checksum,
    )
    db.add(entry)


def start_scheduler():
    global _scheduler
    _scheduler = AsyncIOScheduler(timezone="Asia/Kolkata")
    _scheduler.add_job(
        _poll_all_sensors,
        trigger="interval",
        seconds=settings.sensor_poll_interval,
        id="sensor_poll",
        max_instances=1,
        coalesce=True,
        replace_existing=True,
    )
    _scheduler.start()
    logger.info(
        f"Sensor polling started — interval: {settings.sensor_poll_interval}s"
    )


def stop_scheduler():
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("Sensor polling stopped")
