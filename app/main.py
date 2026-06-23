"""
fluID — Pharma Water Intelligence Platform
FastAPI application entry point.
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import get_settings
from app.db.session import init_db, AsyncSessionLocal, seed_demo_plant
from app.sensors.scheduler import start_scheduler, stop_scheduler
from app.api.routes import router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("fluID starting up...")

    await init_db()

    if settings.app_env == "development":
        async with AsyncSessionLocal() as db:
            await seed_demo_plant(db)

    start_scheduler()

    logger.info(
        f"fluID ready — {settings.plant_name} | "
        f"poll interval: {settings.sensor_poll_interval}s | "
        f"env: {settings.app_env}"
    )

    yield

    stop_scheduler()
    logger.info("fluID shut down cleanly")


app = FastAPI(
    title="fluID — Pharma Water Intelligence",
    description=(
        "Sensor-agnostic water quality monitoring and predictive contamination "
        "prevention for pharmaceutical manufacturing."
    ),
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:5173",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "plant": settings.plant_name,
        "env": settings.app_env,
    }
