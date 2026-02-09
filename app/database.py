import json
import logging
from datetime import datetime, timezone

import asyncpg

from app.config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

logger = logging.getLogger(__name__)

_pool: asyncpg.Pool | None = None
_migrated: bool = False

SCHEMA = "avito"


async def _ensure_pool() -> asyncpg.Pool | None:
    global _pool, _migrated
    if _pool:
        return _pool
    try:
        _pool = await asyncpg.create_pool(
            host=DB_HOST, port=DB_PORT, database=DB_NAME,
            user=DB_USER, password=DB_PASSWORD,
            min_size=1, max_size=5,
        )
        logger.info("Database pool created: %s@%s:%s/%s", DB_USER, DB_HOST, DB_PORT, DB_NAME)
        if not _migrated:
            await _auto_migrate(_pool)
            _migrated = True
    except Exception as e:
        logger.warning("Database unavailable: %s", e)
        _pool = None
    return _pool


async def _auto_migrate(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")

        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.houses (
                address_id INTEGER PRIMARY KEY,
                slug TEXT,
                address TEXT,
                lat DOUBLE PRECISION,
                lng DOUBLE PRECISION,
                build_year TEXT,
                floors TEXT,
                heating TEXT,
                hot_water TEXT,
                cold_water TEXT,
                electricity TEXT,
                gas TEXT,
                sewerage TEXT,
                ventilation TEXT,
                passenger_lift TEXT,
                freight_lift TEXT,
                house_type TEXT,
                floor_type TEXT,
                foundation TEXT,
                energy_class TEXT,
                playground TEXT,
                sports_ground TEXT,
                parking TEXT,
                rating REAL,
                review_count INTEGER,
                price_min INTEGER,
                price_max INTEGER,
                active_listings INTEGER,
                raw_data JSONB,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.listings (
                item_id BIGINT PRIMARY KEY,
                address_id INTEGER,
                title TEXT,
                price INTEGER,
                listing_type TEXT,
                address TEXT,
                lat DOUBLE PRECISION,
                lng DOUBLE PRECISION,
                rooms INTEGER,
                area REAL,
                floor INTEGER,
                total_floors INTEGER,
                url TEXT,
                raw_data JSONB,
                first_seen_at TIMESTAMPTZ DEFAULT NOW(),
                last_seen_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        await conn.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_listings_address_id
            ON {SCHEMA}.listings(address_id)
        """)
        await conn.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_listings_type
            ON {SCHEMA}.listings(listing_type)
        """)

        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.scan_progress (
                id SERIAL PRIMARY KEY,
                scan_id TEXT NOT NULL,
                phase TEXT NOT NULL,
                category TEXT,
                page INTEGER,
                status TEXT DEFAULT 'pending',
                items_found INTEGER DEFAULT 0,
                error_message TEXT,
                started_at TIMESTAMPTZ,
                finished_at TIMESTAMPTZ
            )
        """)
        await conn.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_scan_progress_scan_id
            ON {SCHEMA}.scan_progress(scan_id)
        """)

    logger.info("Auto-migration complete for schema %s", SCHEMA)


async def init_db() -> None:
    await _ensure_pool()


async def close_db() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


# --- Houses ---

HOUSE_FIELDS = [
    "slug", "address", "lat", "lng",
    "build_year", "floors",
    "heating", "hot_water", "cold_water", "electricity", "gas", "sewerage", "ventilation",
    "passenger_lift", "freight_lift",
    "house_type", "floor_type", "foundation", "energy_class",
    "playground", "sports_ground", "parking",
    "rating", "review_count",
    "price_min", "price_max", "active_listings",
    "raw_data",
]


async def upsert_house(house: dict) -> None:
    pool = await _ensure_pool()
    if not pool:
        return

    address_id = house["address_id"]
    values = [address_id]
    for f in HOUSE_FIELDS:
        val = house.get(f)
        if f == "raw_data" and val is not None:
            val = json.dumps(val, ensure_ascii=False)
        values.append(val)

    cols = "address_id, " + ", ".join(HOUSE_FIELDS)
    placeholders = ", ".join(f"${i+1}" for i in range(len(values)))
    updates = ", ".join(f"{f} = EXCLUDED.{f}" for f in HOUSE_FIELDS)

    query = f"""
        INSERT INTO {SCHEMA}.houses ({cols}, created_at, updated_at)
        VALUES ({placeholders}, NOW(), NOW())
        ON CONFLICT (address_id) DO UPDATE SET
            {updates},
            updated_at = NOW()
    """
    await pool.execute(query, *values)


async def get_existing_address_ids() -> set[int]:
    pool = await _ensure_pool()
    if not pool:
        return set()
    rows = await pool.fetch(f"SELECT address_id FROM {SCHEMA}.houses")
    return {r["address_id"] for r in rows}


async def get_house(address_id: int) -> dict | None:
    pool = await _ensure_pool()
    if not pool:
        return None
    row = await pool.fetchrow(
        f"SELECT * FROM {SCHEMA}.houses WHERE address_id = $1", address_id
    )
    if not row:
        return None
    data = dict(row)
    if isinstance(data.get("raw_data"), str):
        try:
            data["raw_data"] = json.loads(data["raw_data"])
        except (json.JSONDecodeError, TypeError):
            pass
    return data


async def search_houses(q: str, limit: int = 50) -> list[dict]:
    pool = await _ensure_pool()
    if not pool:
        return []
    rows = await pool.fetch(
        f"SELECT * FROM {SCHEMA}.houses WHERE address ILIKE $1 ORDER BY address LIMIT $2",
        f"%{q}%", limit,
    )
    return [dict(r) for r in rows]


async def get_houses(limit: int = 50, offset: int = 0, house_type: str | None = None) -> list[dict]:
    pool = await _ensure_pool()
    if not pool:
        return []
    if house_type:
        rows = await pool.fetch(
            f"SELECT * FROM {SCHEMA}.houses WHERE house_type = $1 ORDER BY address LIMIT $2 OFFSET $3",
            house_type, limit, offset,
        )
    else:
        rows = await pool.fetch(
            f"SELECT * FROM {SCHEMA}.houses ORDER BY address LIMIT $1 OFFSET $2",
            limit, offset,
        )
    return [dict(r) for r in rows]


# --- Listings ---

async def upsert_listing(listing: dict) -> None:
    pool = await _ensure_pool()
    if not pool:
        return

    item_id = listing["item_id"]
    raw = listing.get("raw_data")
    if raw is not None:
        raw = json.dumps(raw, ensure_ascii=False)

    await pool.execute(f"""
        INSERT INTO {SCHEMA}.listings (
            item_id, address_id, title, price, listing_type,
            address, lat, lng, rooms, area, floor, total_floors,
            url, raw_data, first_seen_at, last_seen_at
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,NOW(),NOW())
        ON CONFLICT (item_id) DO UPDATE SET
            price = EXCLUDED.price,
            last_seen_at = NOW()
    """,
        item_id,
        listing.get("address_id"),
        listing.get("title"),
        listing.get("price"),
        listing.get("listing_type"),
        listing.get("address"),
        listing.get("lat"),
        listing.get("lng"),
        listing.get("rooms"),
        listing.get("area"),
        listing.get("floor"),
        listing.get("total_floors"),
        listing.get("url"),
        raw,
    )


async def get_listings(
    limit: int = 50, offset: int = 0,
    listing_type: str | None = None,
    address_id: int | None = None,
) -> list[dict]:
    pool = await _ensure_pool()
    if not pool:
        return []

    conditions = []
    params = []
    idx = 1

    if listing_type:
        conditions.append(f"listing_type = ${idx}")
        params.append(listing_type)
        idx += 1
    if address_id:
        conditions.append(f"address_id = ${idx}")
        params.append(address_id)
        idx += 1

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    params.extend([limit, offset])

    rows = await pool.fetch(
        f"SELECT * FROM {SCHEMA}.listings {where} ORDER BY last_seen_at DESC LIMIT ${idx} OFFSET ${idx+1}",
        *params,
    )
    return [dict(r) for r in rows]


# --- Stats ---

async def get_stats() -> dict:
    pool = await _ensure_pool()
    if not pool:
        return {"total_houses": 0, "total_listings": 0}

    h = await pool.fetchrow(f"SELECT COUNT(*) as cnt FROM {SCHEMA}.houses")
    l_total = await pool.fetchrow(f"SELECT COUNT(*) as cnt FROM {SCHEMA}.listings")
    l_sale = await pool.fetchrow(
        f"SELECT COUNT(*) as cnt FROM {SCHEMA}.listings WHERE listing_type = 'sale'"
    )
    l_rent_long = await pool.fetchrow(
        f"SELECT COUNT(*) as cnt FROM {SCHEMA}.listings WHERE listing_type = 'rent_long'"
    )
    l_rent_short = await pool.fetchrow(
        f"SELECT COUNT(*) as cnt FROM {SCHEMA}.listings WHERE listing_type = 'rent_short'"
    )
    detailed = await pool.fetchrow(
        f"SELECT COUNT(*) as cnt FROM {SCHEMA}.houses WHERE build_year IS NOT NULL OR house_type IS NOT NULL"
    )
    last_scan = await pool.fetchrow(
        f"SELECT MAX(updated_at) as ts FROM {SCHEMA}.houses"
    )

    return {
        "total_houses": h["cnt"],
        "total_listings": l_total["cnt"],
        "listings_sale": l_sale["cnt"],
        "listings_rent_long": l_rent_long["cnt"],
        "listings_rent_short": l_rent_short["cnt"],
        "houses_with_details": detailed["cnt"],
        "last_scan": last_scan["ts"],
    }


# --- Scan progress ---

async def save_scan_progress(scan_id: str, phase: str, category: str, page: int,
                              status: str, items_found: int = 0, error_message: str | None = None) -> None:
    pool = await _ensure_pool()
    if not pool:
        return
    now = datetime.now(timezone.utc)
    finished = now if status in ("done", "error") else None
    await pool.execute(f"""
        INSERT INTO {SCHEMA}.scan_progress (scan_id, phase, category, page, status, items_found, error_message, started_at, finished_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    """, scan_id, phase, category, page, status, items_found, error_message, now, finished)
