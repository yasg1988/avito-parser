import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks

from app.database import init_db, close_db, get_house, get_houses, search_houses, get_listings, get_stats
from app.models import ScanStatus, StatsResponse, MonitoringResponse
from app.scanner import run_full_scan, get_scan_status, request_stop

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await init_db()
        logger.info("Database initialized")
    except Exception as e:
        logger.error("Failed to connect to database: %s", e)
    yield
    await close_db()


app = FastAPI(
    title="Avito Parser API",
    description="REST API для данных домов Йошкар-Олы с Avito",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/")
async def health():
    return {
        "status": "ok",
        "service": "avito-parser",
        "version": "1.0.0",
        "endpoints": [
            "GET /houses", "GET /houses/search?q=", "GET /houses/{address_id}",
            "GET /listings", "GET /stats", "GET /monitoring",
            "POST /scan/start", "POST /scan/stop", "GET /scan/status",
        ],
    }


# --- Houses ---

@app.get("/houses")
async def list_houses(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    house_type: str | None = Query(None),
):
    return await get_houses(limit=limit, offset=offset, house_type=house_type)


@app.get("/houses/search")
async def search_houses_endpoint(
    q: str = Query(..., min_length=2),
    limit: int = Query(50, ge=1, le=500),
):
    results = await search_houses(q, limit)
    if not results:
        raise HTTPException(404, f"Дома по запросу '{q}' не найдены")
    return results


@app.get("/houses/{address_id}")
async def house_detail(address_id: int):
    house = await get_house(address_id)
    if not house:
        raise HTTPException(404, f"Дом с address_id={address_id} не найден")
    return house


# --- Listings ---

@app.get("/listings")
async def list_listings(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    listing_type: str | None = Query(None),
    address_id: int | None = Query(None),
):
    return await get_listings(limit=limit, offset=offset, listing_type=listing_type, address_id=address_id)


# --- Scan ---

@app.post("/scan/start")
async def start_scan(
    background_tasks: BackgroundTasks,
    phase: str | None = Query(None, description="1 = only search, 2 = only houses, None = full"),
):
    status = get_scan_status()
    if status["status"] == "running":
        raise HTTPException(409, "Scan already running")

    background_tasks.add_task(run_full_scan, phase)
    return {"message": "Scan started", "phase": phase or "full"}


@app.post("/scan/stop")
async def stop_scan():
    if request_stop():
        return {"message": "Stop requested"}
    raise HTTPException(400, "No scan running")


@app.get("/scan/status")
async def scan_status():
    return get_scan_status()


# --- Stats ---

@app.get("/stats", response_model=StatsResponse)
async def stats():
    data = await get_stats()
    return StatsResponse(**data)


# --- Monitoring ---

@app.get("/monitoring", response_model=MonitoringResponse)
async def monitoring():
    data = await get_stats()
    alerts = []

    last_scan = data.get("last_scan")
    if last_scan:
        if last_scan.tzinfo is None:
            last_scan = last_scan.replace(tzinfo=timezone.utc)
        age = datetime.now(timezone.utc) - last_scan
        if age > timedelta(hours=48):
            alerts.append(f"Data stale: last scan {age.days}d {age.seconds//3600}h ago")

    if data.get("total_houses", 0) == 0:
        alerts.append("No houses in database")

    status = "ok" if not alerts else "warning"

    return MonitoringResponse(
        status=status,
        total_houses=data.get("total_houses", 0),
        total_listings=data.get("total_listings", 0),
        last_scan=last_scan,
        alerts=alerts,
    )
