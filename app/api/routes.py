"""
REST API routes for fluID dashboard.
"""

from datetime import datetime, timedelta
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.db.models import (
    PlantConfig, SensorConfig, SensorReading,
    AlertEvent, AlertStatus, AuditLog, AlertLevel
)
from app.alerts.trend import TrendDetector

router = APIRouter(prefix="/api")
_trend_detector = TrendDetector()


@router.get("/plants")
async def list_plants(db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(PlantConfig).where(PlantConfig.is_active == True)
    )
    plants = result.scalars().all()
    return [
        {
            "id": str(p.id),
            "name": p.name,
            "location": p.location,
            "system_type": p.system_type,
            "standard": p.standard,
        }
        for p in plants
    ]


@router.get("/plants/{plant_id}/sensors")
async def list_sensors(
    plant_id: UUID,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(SensorConfig)
        .where(SensorConfig.plant_id == plant_id, SensorConfig.is_active == True)
    )
    sensors = result.scalars().all()
    return [_sensor_schema(s) for s in sensors]


def _sensor_schema(s: SensorConfig) -> dict:
    return {
        "id": str(s.id),
        "sensor_key": s.sensor_key,
        "label": s.label,
        "unit": s.unit,
        "protocol": s.protocol,
        "nominal_value": s.nominal_value,
        "warn_threshold": s.warn_threshold,
        "alert_threshold": s.alert_threshold,
        "min_range": s.min_range,
        "max_range": s.max_range,
        "decimals": s.decimals,
        "last_calibration": s.last_calibration.isoformat() if s.last_calibration else None,
        "calibration_due": s.calibration_due.isoformat() if s.calibration_due else None,
    }


@router.get("/plants/{plant_id}/readings")
async def get_live_readings(
    plant_id: UUID,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(SensorConfig)
        .where(SensorConfig.plant_id == plant_id, SensorConfig.is_active == True)
    )
    sensors = result.scalars().all()

    readings = []
    for sensor in sensors:
        latest = await db.execute(
            select(SensorReading)
            .where(SensorReading.sensor_id == sensor.id)
            .order_by(desc(SensorReading.time))
            .limit(1)
        )
        reading = latest.scalar_one_or_none()
        if reading:
            status = _compute_status(sensor, reading.value)
            readings.append({
                "sensor_id": str(sensor.id),
                "sensor_key": sensor.sensor_key,
                "label": sensor.label,
                "unit": sensor.unit,
                "value": reading.value,
                "quality": reading.quality,
                "status": status,
                "timestamp": reading.time.isoformat(),
                "warn_threshold": sensor.warn_threshold,
                "alert_threshold": sensor.alert_threshold,
                "nominal_value": sensor.nominal_value,
                "min_range": sensor.min_range,
                "max_range": sensor.max_range,
                "decimals": sensor.decimals,
            })

    return readings


@router.get("/plants/{plant_id}/history")
async def get_history(
    plant_id: UUID,
    minutes: int = Query(default=60, ge=5, le=1440),
    db: AsyncSession = Depends(get_db),
):
    since = datetime.utcnow() - timedelta(minutes=minutes)

    result = await db.execute(
        select(SensorConfig)
        .where(SensorConfig.plant_id == plant_id, SensorConfig.is_active == True)
    )
    sensors = result.scalars().all()

    history = {}
    for sensor in sensors:
        readings_result = await db.execute(
            select(SensorReading)
            .where(
                SensorReading.sensor_id == sensor.id,
                SensorReading.time >= since,
            )
            .order_by(SensorReading.time)
        )
        readings = readings_result.scalars().all()
        history[sensor.sensor_key] = {
            "label": sensor.label,
            "unit": sensor.unit,
            "warn_threshold": sensor.warn_threshold,
            "alert_threshold": sensor.alert_threshold,
            "readings": [
                {"time": r.time.isoformat(), "value": r.value, "quality": r.quality}
                for r in readings
            ],
        }

    return history


@router.get("/plants/{plant_id}/alerts")
async def get_alerts(
    plant_id: UUID,
    status: Optional[str] = None,
    limit: int = Query(default=50, le=200),
    db: AsyncSession = Depends(get_db),
):
    query = (
        select(AlertEvent, SensorConfig)
        .join(SensorConfig, AlertEvent.sensor_id == SensorConfig.id)
        .where(AlertEvent.plant_id == plant_id)
        .order_by(desc(AlertEvent.triggered_at))
        .limit(limit)
    )
    if status:
        query = query.where(AlertEvent.status == status)

    result = await db.execute(query)
    rows = result.all()

    return [
        {
            "id": str(alert.id),
            "sensor_label": sensor.label,
            "sensor_unit": sensor.unit,
            "level": alert.level,
            "status": alert.status,
            "triggered_value": alert.triggered_value,
            "threshold_value": alert.threshold_value,
            "message": alert.message,
            "triggered_at": alert.triggered_at.isoformat(),
            "resolved_at": alert.resolved_at.isoformat() if alert.resolved_at else None,
            "acknowledged_by": alert.acknowledged_by,
        }
        for alert, sensor in rows
    ]


@router.patch("/alerts/{alert_id}/acknowledge")
async def acknowledge_alert(
    alert_id: UUID,
    user: str = Query(..., description="Name/ID of the person acknowledging"),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(AlertEvent).where(AlertEvent.id == alert_id)
    )
    alert = result.scalar_one_or_none()
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")

    alert.status = AlertStatus.ACKNOWLEDGED
    alert.acknowledged_by = user
    await db.commit()
    return {"status": "acknowledged", "alert_id": str(alert_id), "by": user}


@router.get("/plants/{plant_id}/audit")
async def get_audit_log(
    plant_id: UUID,
    limit: int = Query(default=100, le=500),
    since_hours: int = Query(default=24, le=720),
    db: AsyncSession = Depends(get_db),
):
    since = datetime.utcnow() - timedelta(hours=since_hours)
    result = await db.execute(
        select(AuditLog)
        .where(
            AuditLog.plant_id == plant_id,
            AuditLog.time >= since,
        )
        .order_by(desc(AuditLog.time))
        .limit(limit)
    )
    entries = result.scalars().all()
    return [
        {
            "id": str(e.id),
            "time": e.time.isoformat(),
            "level": e.level,
            "category": e.category,
            "message": e.message,
            "checksum": e.checksum,
        }
        for e in entries
    ]


@router.get("/plants/{plant_id}/risk-score")
async def get_risk_score(
    plant_id: UUID,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(SensorConfig)
        .where(SensorConfig.plant_id == plant_id, SensorConfig.is_active == True)
    )
    sensors = result.scalars().all()

    sensor_scores = []
    for sensor in sensors:
        latest = await db.execute(
            select(SensorReading)
            .where(SensorReading.sensor_id == sensor.id)
            .order_by(desc(SensorReading.time))
            .limit(1)
        )
        reading = latest.scalar_one_or_none()
        if reading:
            span = sensor.alert_threshold - sensor.nominal_value
            score = max(0.0, min(1.0, (reading.value - sensor.nominal_value) / span if span else 0))
            sensor_scores.append({
                "sensor_key": sensor.sensor_key,
                "label": sensor.label,
                "score": round(score * 100, 1),
                "value": reading.value,
                "unit": sensor.unit,
            })

    composite = round(sum(s["score"] for s in sensor_scores) / len(sensor_scores), 1) if sensor_scores else 0.0

    return {
        "composite_score": composite,
        "risk_level": "HIGH" if composite > 65 else "MEDIUM" if composite > 35 else "LOW",
        "sensor_breakdown": sensor_scores,
        "evaluated_at": datetime.utcnow().isoformat(),
        "note": "Phase 1 rule-based scoring. ML model pending 6-month data collection.",
    }


@router.get("/plants/{plant_id}/trends")
async def get_trends(
    plant_id: UUID,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(SensorConfig)
        .where(SensorConfig.plant_id == plant_id, SensorConfig.is_active == True)
    )
    sensors = result.scalars().all()

    trends = []
    for sensor in sensors:
        latest = await db.execute(
            select(SensorReading)
            .where(SensorReading.sensor_id == sensor.id)
            .order_by(desc(SensorReading.time))
            .limit(1)
        )
        reading = latest.scalar_one_or_none()
        if not reading:
            continue

        trend = await _trend_detector.analyze(db, sensor, reading.value)

        if trend is None:
            trends.append({
                "sensor_key": sensor.sensor_key,
                "label": sensor.label,
                "status": "INSUFFICIENT_DATA",
                "note": "Accumulating readings — trend analysis available after ~40 minutes of data.",
            })
            continue

        trends.append({
            "sensor_key": sensor.sensor_key,
            "label": sensor.label,
            "unit": sensor.unit,
            "current_value": trend.current_value,
            "direction": trend.direction,
            "rate_per_hour": trend.rate_per_hour,
            "trend_confidence": trend.r_squared,
            "is_significant": trend.is_significant,
            "hours_to_warn": trend.hours_to_warn,
            "hours_to_alert": trend.hours_to_alert,
            "status": "PREDICTIVE_WARNING" if (trend.hours_to_warn or trend.hours_to_alert) else "STABLE",
        })

    return trends

def _compute_status(sensor: SensorConfig, value: float) -> str:
    if value >= sensor.alert_threshold:
        return "ALERT"
    if value >= sensor.warn_threshold:
        return "WARN"
    return "NORMAL"
