"""Browser-based data extraction from Avito pages.

Uses Playwright (headed + Xvfb) to bypass datacenter IP blocking.
Extracts structured JSON from window.__staticRouterHydrationData.
"""

import re
import json
import logging

from playwright.async_api import async_playwright, Browser, BrowserContext

from app.config import BASE_URL, CITY, USER_AGENT

logger = logging.getLogger(__name__)

# Regex to extract hydration data from HTML
HYDRATION_RE = re.compile(
    r'window\.__staticRouterHydrationData\s*=\s*({.+?})\s*;\s*</script>',
    re.DOTALL,
)

# Shared browser instance
_browser: Browser | None = None
_context: BrowserContext | None = None


async def get_browser_context() -> BrowserContext:
    """Get or create shared browser context."""
    global _browser, _context
    if _context:
        return _context

    p = await async_playwright().start()
    _browser = await p.chromium.launch(
        headless=False,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
        ],
    )
    _context = await _browser.new_context(
        user_agent=USER_AGENT,
        locale="ru-RU",
        viewport={"width": 1920, "height": 1080},
    )
    await _context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    """)
    logger.info("Browser context created")
    return _context


async def close_browser() -> None:
    """Close shared browser."""
    global _browser, _context
    if _context:
        await _context.close()
        _context = None
    if _browser:
        await _browser.close()
        _browser = None


def _extract_hydration_data(html: str) -> dict | None:
    """Extract JSON from window.__staticRouterHydrationData in HTML."""
    m = HYDRATION_RE.search(html)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except json.JSONDecodeError as e:
        logger.warning("Failed to parse hydration JSON: %s", e)
        return None


def _get_loader_data(hydration: dict) -> dict | None:
    """Get loaderData from hydration structure."""
    loader = hydration.get("loaderData", {})
    for key in ["catalog-or-main-or-item", "root"]:
        if key in loader:
            return loader[key]
    if loader:
        return next(iter(loader.values()))
    return None


# --- Search page parsing ---

def parse_search_page(html: str) -> list[dict]:
    """Parse search results page, extract listing summaries."""
    hydration = _extract_hydration_data(html)
    if not hydration:
        logger.warning("No hydration data found on search page")
        return []

    loader = _get_loader_data(hydration)
    if not loader:
        return []

    items = []
    search_result = loader.get("searchResult", {})
    raw_items = search_result.get("items", [])

    if not raw_items:
        raw_items = loader.get("items", [])

    for item in raw_items:
        try:
            parsed = _parse_search_item(item)
            if parsed:
                items.append(parsed)
        except Exception as e:
            logger.debug("Failed to parse search item: %s", e)

    return items


def _parse_search_item(item: dict) -> dict | None:
    """Parse a single search result item."""
    item_id = item.get("id")
    if not item_id:
        return None

    # Extract price
    price = None
    price_text = None
    price_detail = item.get("priceDetailed") or item.get("price")
    if isinstance(price_detail, dict):
        price = price_detail.get("value")
        price_text = price_detail.get("postfix", "")
    elif isinstance(price_detail, (int, float)):
        price = int(price_detail)

    # Determine listing type from price postfix
    listing_type = "sale"
    if price_text:
        if "/мес" in price_text:
            listing_type = "rent_long"
        elif "/сут" in price_text:
            listing_type = "rent_short"

    # Extract geo
    lat, lng = None, None
    geo = item.get("geo", {})
    if isinstance(geo, dict):
        coords = geo.get("coords") or geo.get("coordinates", {})
        if isinstance(coords, dict):
            lat = coords.get("lat")
            lng = coords.get("lng") or coords.get("lon")

    # Extract address
    address = None
    geo_refs = geo.get("geoReferences", [])
    if geo_refs and isinstance(geo_refs, list):
        for ref in geo_refs:
            content = ref.get("content")
            if content:
                address = content
                break
    if not address:
        address = item.get("address") or item.get("location", {}).get("name")

    url_path = item.get("urlPath", "")
    title = item.get("title", "")

    # Parse title "1-к. квартира, 37,5 м², 8/10 эт."
    rooms, area, floor_val, total_floors = None, None, None, None
    title_match = re.match(r'(\d+)-к.*?(\d+[.,]?\d*)\s*м.*?(\d+)/(\d+)', title)
    if title_match:
        rooms = int(title_match.group(1))
        area = float(title_match.group(2).replace(",", "."))
        floor_val = int(title_match.group(3))
        total_floors = int(title_match.group(4))

    return {
        "item_id": int(item_id),
        "title": title,
        "price": int(price) if price else None,
        "listing_type": listing_type,
        "address": address,
        "lat": lat,
        "lng": lng,
        "rooms": rooms,
        "area": area,
        "floor": floor_val,
        "total_floors": total_floors,
        "url": f"{BASE_URL}{url_path}" if url_path else None,
    }


# --- Listing page parsing (for addressId extraction) ---

def parse_listing_page(html: str) -> dict | None:
    """Parse a single listing page to extract addressId and house params."""
    hydration = _extract_hydration_data(html)
    if not hydration:
        return None

    loader = _get_loader_data(hydration)
    if not loader:
        return None

    buyer = loader.get("buyerItem", {})
    if not buyer:
        return None

    item = buyer.get("item", {})
    if not item:
        return None

    address_id = None
    slug = None
    house_url = item.get("houseCatalogPageUrl", "")
    if house_url:
        parts = house_url.rstrip("/").split("/")
        if len(parts) >= 2:
            try:
                address_id = int(parts[-1])
                slug = parts[-2]
            except ValueError:
                pass

    lat, lng = None, None
    geo = item.get("geo", {})
    if isinstance(geo, dict):
        coords = geo.get("coords", {})
        if isinstance(coords, dict):
            lat = coords.get("lat")
            lng = coords.get("lng")

    house_params = {}
    hp = item.get("houseParams", {})
    if isinstance(hp, dict):
        hp_data = hp.get("data", {})
        hp_items = hp_data.get("items", [])
        for hp_item in hp_items:
            title = hp_item.get("title", "")
            description = hp_item.get("description", "")
            if title and description:
                house_params[title] = description

        rating_preview = hp_data.get("ratingPreview", {})
        if isinstance(rating_preview, dict):
            house_params["_rating"] = rating_preview.get("scoreValue")
            house_params["_address_id"] = rating_preview.get("addressId")

    return {
        "address_id": address_id,
        "slug": slug,
        "address": item.get("address"),
        "lat": lat,
        "lng": lng,
        "house_params": house_params,
        "item_id": item.get("id"),
        "title": item.get("title"),
        "price": item.get("price"),
    }


# --- House catalog page parsing ---

HOUSE_FIELD_MAP = {
    "Год постройки": "build_year",
    "Этажей": "floors",
    "Отопление": "heating",
    "Горячее водоснабжение": "hot_water",
    "Холодное водоснабжение": "cold_water",
    "Электроснабжение": "electricity",
    "Газоснабжение": "gas",
    "Канализация": "sewerage",
    "Система вентиляции": "ventilation",
    "Пассажирский лифт": "passenger_lift",
    "Грузовой лифт": "freight_lift",
    "Тип дома": "house_type",
    "Перекрытия": "floor_type",
    "Фундамент": "foundation",
    "Класс энергоэффективности": "energy_class",
    "Детская площадка": "playground",
    "Спортивная площадка": "sports_ground",
    "Парковка": "parking",
}


def parse_house_page(html: str) -> dict | None:
    """Parse house catalog page for full characteristics."""
    hydration = _extract_hydration_data(html)
    if not hydration:
        logger.warning("No hydration data on house page")
        return None

    loader = _get_loader_data(hydration)
    if not loader:
        return None

    result = {}

    house_info = loader.get("houseInfo") or loader.get("house") or loader.get("aboutHouse")
    if isinstance(house_info, dict):
        items = house_info.get("items", [])
        if isinstance(items, list):
            for item in items:
                _extract_house_field(item, result)
        sections = house_info.get("sections", [])
        if isinstance(sections, list):
            for section in sections:
                sec_items = section.get("items", [])
                if isinstance(sec_items, list):
                    for item in sec_items:
                        _extract_house_field(item, result)

    about_block = loader.get("aboutHouseBlock") or loader.get("aboutHouse")
    if isinstance(about_block, dict) and not result:
        sections = about_block.get("sections", [])
        if isinstance(sections, list):
            for section in sections:
                sec_items = section.get("items", [])
                if isinstance(sec_items, list):
                    for item in sec_items:
                        _extract_house_field(item, result)

    if not result:
        result = _deep_search_house_fields(loader)

    rating_data = loader.get("rating") or loader.get("houseRating")
    if isinstance(rating_data, dict):
        result["rating"] = rating_data.get("value") or rating_data.get("score")
        result["review_count"] = rating_data.get("count") or rating_data.get("reviewCount")

    price_range = loader.get("priceRange") or loader.get("priceSummary")
    if isinstance(price_range, dict):
        result["price_min"] = price_range.get("min") or price_range.get("minPrice")
        result["price_max"] = price_range.get("max") or price_range.get("maxPrice")

    listings_data = loader.get("listings") or loader.get("activeListings")
    if isinstance(listings_data, dict):
        result["active_listings"] = listings_data.get("total") or listings_data.get("count")
    elif isinstance(listings_data, list):
        result["active_listings"] = len(listings_data)

    return result if result else None


def _extract_house_field(item: dict, result: dict) -> None:
    title = item.get("title") or item.get("name") or item.get("label", "")
    value = item.get("value") or item.get("description") or item.get("text", "")
    if not title or not value:
        return
    db_field = HOUSE_FIELD_MAP.get(title)
    if db_field:
        result[db_field] = str(value)


def _deep_search_house_fields(data: dict, depth: int = 0) -> dict:
    if depth > 5:
        return {}
    result = {}
    for key, val in data.items():
        if isinstance(val, dict):
            title = val.get("title") or val.get("name", "")
            value = val.get("value") or val.get("description", "")
            if title in HOUSE_FIELD_MAP and value:
                result[HOUSE_FIELD_MAP[title]] = str(value)
            else:
                result.update(_deep_search_house_fields(val, depth + 1))
        elif isinstance(val, list):
            for item in val:
                if isinstance(item, dict):
                    title = item.get("title") or item.get("name", "")
                    value = item.get("value") or item.get("description", "")
                    if title in HOUSE_FIELD_MAP and value:
                        result[HOUSE_FIELD_MAP[title]] = str(value)
                    else:
                        result.update(_deep_search_house_fields(item, depth + 1))
    return result


# --- Browser fetch functions ---

async def fetch_page(url: str) -> str | None:
    """Fetch a page using Playwright browser, return HTML content."""
    try:
        ctx = await get_browser_context()
        page = await ctx.new_page()
        try:
            resp = await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            if resp and resp.status == 200:
                html = await page.content()
                return html
            status = resp.status if resp else "no response"
            logger.warning("Page %s returned %s", url, status)
            return None
        finally:
            await page.close()
    except Exception as e:
        logger.error("Failed to fetch %s: %s", url, e)
        return None


async def fetch_search_page(category_slug: str, page_num: int) -> str | None:
    """Fetch a search results page."""
    url = f"{BASE_URL}/{CITY}/kvartiry/{category_slug}?p={page_num}"
    return await fetch_page(url)


async def fetch_listing_page(url: str) -> str | None:
    """Fetch a single listing page."""
    full_url = url if url.startswith("http") else f"{BASE_URL}{url}"
    return await fetch_page(full_url)


async def fetch_house_page(slug: str, address_id: int) -> str | None:
    """Fetch a house catalog page."""
    url = f"{BASE_URL}/catalog/houses/{CITY}/{slug}/{address_id}"
    return await fetch_page(url)
