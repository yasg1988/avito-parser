"""Scan orchestrator: Phase 1 (search listings) + Phase 2 (house details).

Manages incremental scanning with progress tracking and resume support.
"""

import asyncio
import logging
import threading
import uuid
from datetime import datetime, timezone

from app.config import (
    SCAN_DELAY_SEARCH, SCAN_DELAY_HOUSE, MAX_CONSECUTIVE_ERRORS,
    SEARCH_CATEGORIES,
)
from app import database, parser

logger = logging.getLogger(__name__)

# Global scan state (thread-safe)
scan_state = {
    "status": "idle",  # idle / running / completed / error / stopped
    "phase": None,
    "category": None,
    "total_pages": 0,
    "done_pages": 0,
    "total_houses": 0,
    "done_houses": 0,
    "new_houses": 0,
    "listings_found": 0,
    "errors": 0,
    "started_at": None,
    "message": None,
    "stop_requested": False,
    "lock": threading.Lock(),
}


def get_scan_status() -> dict:
    with scan_state["lock"]:
        return {
            "status": scan_state["status"],
            "phase": scan_state["phase"],
            "category": scan_state["category"],
            "total_pages": scan_state["total_pages"],
            "done_pages": scan_state["done_pages"],
            "total_houses": scan_state["total_houses"],
            "done_houses": scan_state["done_houses"],
            "new_houses": scan_state["new_houses"],
            "listings_found": scan_state["listings_found"],
            "errors": scan_state["errors"],
            "started_at": scan_state["started_at"],
            "message": scan_state["message"],
        }


def request_stop() -> bool:
    with scan_state["lock"]:
        if scan_state["status"] == "running":
            scan_state["stop_requested"] = True
            scan_state["message"] = "Stop requested, finishing current operation..."
            return True
    return False


def _update_state(**kwargs) -> None:
    with scan_state["lock"]:
        for k, v in kwargs.items():
            if k in scan_state:
                scan_state[k] = v


def _is_stop_requested() -> bool:
    with scan_state["lock"]:
        return scan_state["stop_requested"]


async def run_full_scan(phase: str | None = None) -> None:
    """Run full scan: Phase 1 (listings) + Phase 2 (house details)."""
    with scan_state["lock"]:
        if scan_state["status"] == "running":
            logger.warning("Scan already running")
            return
        scan_state["status"] = "running"
        scan_state["phase"] = None
        scan_state["category"] = None
        scan_state["total_pages"] = 0
        scan_state["done_pages"] = 0
        scan_state["total_houses"] = 0
        scan_state["done_houses"] = 0
        scan_state["new_houses"] = 0
        scan_state["listings_found"] = 0
        scan_state["errors"] = 0
        scan_state["started_at"] = datetime.now(timezone.utc)
        scan_state["message"] = "Starting scan..."
        scan_state["stop_requested"] = False

    scan_id = str(uuid.uuid4())[:8]

    try:
        client = parser._get_client()
        async with client:
            # Phase 1: Scan search pages for listings
            if phase in (None, "1"):
                await _run_phase1(client, scan_id)

            if _is_stop_requested():
                _update_state(status="stopped", message="Scan stopped by user")
                return

            # Phase 2: Fetch house details for new houses
            if phase in (None, "2"):
                await _run_phase2(client, scan_id)

        final_status = "stopped" if _is_stop_requested() else "completed"
        _update_state(
            status=final_status,
            message=f"Scan {final_status}. Houses: {scan_state['new_houses']} new, Listings: {scan_state['listings_found']}",
        )
        logger.info("Scan %s %s: %d listings, %d new houses",
                     scan_id, final_status, scan_state["listings_found"], scan_state["new_houses"])

    except Exception as e:
        logger.error("Scan failed: %s", e)
        _update_state(status="error", message=f"Scan error: {e}")


async def _run_phase1(client, scan_id: str) -> None:
    """Phase 1: Scan search pages, collect listings and addressIds."""
    _update_state(phase="phase1_search", message="Phase 1: Scanning search pages...")

    # Collect all addressId -> slug mappings
    address_slugs: dict[int, str] = {}

    for cat_name, cat_slug in SEARCH_CATEGORIES.items():
        if _is_stop_requested():
            return

        _update_state(category=cat_name, message=f"Phase 1: Scanning {cat_name}...")
        consecutive_errors = 0
        page = 1

        while True:
            if _is_stop_requested():
                return

            _update_state(message=f"Phase 1: {cat_name} page {page}...")

            html = await parser.fetch_search_page(client, cat_slug, page)
            if not html:
                consecutive_errors += 1
                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    logger.warning("Too many errors for %s, stopping category", cat_name)
                    break
                await asyncio.sleep(SCAN_DELAY_SEARCH)
                page += 1
                continue

            items = parser.parse_search_page(html)
            if not items:
                # No more results — end of pagination
                logger.info("Category %s: no items on page %d, done", cat_name, page)
                break

            consecutive_errors = 0

            # Process items
            for item in items:
                item_id = item.get("item_id")
                if not item_id:
                    continue

                # We need to visit the listing page to get addressId
                # But first, save the listing from search results
                listing_type = item.get("listing_type", "sale")
                if cat_name == "rent":
                    # Default to rent_long if not determined
                    if listing_type == "sale":
                        listing_type = "rent_long"

                item["listing_type"] = listing_type

                try:
                    await database.upsert_listing(item)
                except Exception as e:
                    logger.debug("Failed to upsert listing %s: %s", item_id, e)

            _update_state(
                listings_found=scan_state["listings_found"] + len(items),
                done_pages=scan_state["done_pages"] + 1,
            )

            await database.save_scan_progress(
                scan_id, "phase1", cat_name, page, "done", len(items)
            )

            logger.info("Phase 1: %s page %d → %d items", cat_name, page, len(items))

            await asyncio.sleep(SCAN_DELAY_SEARCH)
            page += 1

    # Now visit individual listing pages to collect addressIds
    _update_state(message="Phase 1: Collecting addressIds from listing pages...")
    await _collect_address_ids(client, scan_id, address_slugs)

    logger.info("Phase 1 complete: %d address IDs collected", len(address_slugs))


async def _collect_address_ids(client, scan_id: str, address_slugs: dict) -> None:
    """Visit listing pages to extract addressId and slug."""
    pool = await database._ensure_pool()
    if not pool:
        return

    # Get listings without address_id
    rows = await pool.fetch(f"""
        SELECT item_id, url FROM {database.SCHEMA}.listings
        WHERE address_id IS NULL AND url IS NOT NULL
        ORDER BY item_id
    """)

    if not rows:
        logger.info("No listings without address_id to process")
        return

    total = len(rows)
    _update_state(message=f"Phase 1: Resolving addressIds for {total} listings...")

    consecutive_errors = 0

    for idx, row in enumerate(rows):
        if _is_stop_requested():
            return

        url = row["url"]
        item_id = row["item_id"]

        if idx % 50 == 0:
            _update_state(message=f"Phase 1: addressId {idx}/{total}...")

        html = await parser.fetch_listing_page(client, url)
        if not html:
            consecutive_errors += 1
            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                logger.warning("Too many errors resolving addressIds, stopping")
                break
            await asyncio.sleep(SCAN_DELAY_SEARCH)
            continue

        consecutive_errors = 0
        data = parser.parse_listing_page(html)

        if data and data.get("address_id"):
            aid = data["address_id"]
            slug = data.get("slug", "")
            address_slugs[aid] = slug

            # Update listing with address_id
            await pool.execute(
                f"UPDATE {database.SCHEMA}.listings SET address_id = $1 WHERE item_id = $2",
                aid, item_id,
            )

            # Pre-create house record with basic info
            house_data = {
                "address_id": aid,
                "slug": slug,
                "address": data.get("address"),
                "lat": data.get("lat"),
                "lng": data.get("lng"),
            }
            try:
                await database.upsert_house(house_data)
            except Exception as e:
                logger.debug("Failed to pre-create house %d: %s", aid, e)

        await asyncio.sleep(SCAN_DELAY_SEARCH)


async def _run_phase2(client, scan_id: str) -> None:
    """Phase 2: Fetch full details for houses without detailed data."""
    _update_state(phase="phase2_houses", message="Phase 2: Fetching house details...")

    pool = await database._ensure_pool()
    if not pool:
        return

    # Get houses that lack detailed info
    rows = await pool.fetch(f"""
        SELECT address_id, slug FROM {database.SCHEMA}.houses
        WHERE build_year IS NULL AND house_type IS NULL
        AND slug IS NOT NULL
        ORDER BY address_id
    """)

    if not rows:
        logger.info("Phase 2: All houses already have details")
        _update_state(message="Phase 2: No new houses to process")
        return

    total = len(rows)
    _update_state(total_houses=total, message=f"Phase 2: {total} houses to process...")

    consecutive_errors = 0

    for idx, row in enumerate(rows):
        if _is_stop_requested():
            return

        address_id = row["address_id"]
        slug = row["slug"]

        _update_state(
            done_houses=idx,
            message=f"Phase 2: House {idx+1}/{total} (id={address_id})...",
        )

        html = await parser.fetch_house_page(client, slug, address_id)
        if not html:
            consecutive_errors += 1
            _update_state(errors=scan_state["errors"] + 1)
            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                logger.warning("Too many consecutive errors in Phase 2, stopping")
                break
            await asyncio.sleep(SCAN_DELAY_HOUSE)
            continue

        consecutive_errors = 0
        house_data = parser.parse_house_page(html)

        if house_data:
            house_data["address_id"] = address_id
            house_data["slug"] = slug
            house_data["raw_data"] = house_data.copy()

            try:
                await database.upsert_house(house_data)
                _update_state(new_houses=scan_state["new_houses"] + 1)
            except Exception as e:
                logger.error("Failed to save house %d: %s", address_id, e)
                _update_state(errors=scan_state["errors"] + 1)

            await database.save_scan_progress(
                scan_id, "phase2", f"house_{address_id}", 0, "done", 1,
            )
        else:
            logger.warning("No data extracted for house %d", address_id)
            await database.save_scan_progress(
                scan_id, "phase2", f"house_{address_id}", 0, "error", 0,
                "No data extracted",
            )
            _update_state(errors=scan_state["errors"] + 1)

        await asyncio.sleep(SCAN_DELAY_HOUSE)

    _update_state(done_houses=total)
    logger.info("Phase 2 complete: %d/%d houses processed", scan_state["new_houses"], total)
