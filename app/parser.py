"""HTTP client and data extraction from Avito pages.

Extracts structured JSON from window.__staticRouterHydrationData
embedded in Avito HTML pages.
"""

import re
import json
import logging
from typing import Optional

import httpx

from app.config import BASE_URL, CITY, USER_AGENT

logger = logging.getLogger(__name__)

# Regex to extract hydration data from HTML
HYDRATION_RE = re.compile(
    r'window\.__staticRouterHydrationData\s*=\s*({.+?})\s*;\s*</script>',
    re.DOTALL,
)


def _get_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
        },
        follow_redirects=True,
        timeout=30.0,
    )


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
    # Try common keys
    for key in ["catalog-or-main-or-item", "root"]:
        if key in loader:
            return loader[key]
    # Return first available
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

    # Try to find items in various possible structures
    search_result = loader.get("searchResult", {})
    raw_items = search_result.get("items", [])

    if not raw_items:
        # Alternative path
        raw_items = loader.get("items", [])

    for item in raw_items:
        try:
            parsed = _parse_search_item(item)
            if parsed:
                items.append(parsed)
        except Exception as e:
            logger.debug("Failed to parse search item: %s", e)
            continue

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

    # Extract URL path
    url_path = item.get("urlPath", "")

    # Extract title parts
    title = item.get("title", "")
    rooms, area, floor_val, total_floors = None, None, None, None
    # Try parsing "1-к. квартира, 37,5 м², 8/10 эт."
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

    # Extract addressId from houseCatalogPageUrl
    address_id = None
    slug = None
    house_url = item.get("houseCatalogPageUrl", "")
    if house_url:
        # /catalog/houses/yoshkar-ola/festivalnaya_ul_56/307170
        parts = house_url.rstrip("/").split("/")
        if len(parts) >= 2:
            try:
                address_id = int(parts[-1])
                slug = parts[-2]
            except ValueError:
                pass

    # Extract coordinates
    lat, lng = None, None
    geo = item.get("geo", {})
    if isinstance(geo, dict):
        coords = geo.get("coords", {})
        if isinstance(coords, dict):
            lat = coords.get("lat")
            lng = coords.get("lng")

    # House params from listing
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

        # Rating
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

# Map Russian field names to DB columns
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

    # Try to find house info in the loader data
    # The structure varies - look for common patterns
    house_info = loader.get("houseInfo") or loader.get("house") or loader.get("aboutHouse")

    if isinstance(house_info, dict):
        # Try structured data
        items = house_info.get("items", [])
        if isinstance(items, list):
            for item in items:
                _extract_house_field(item, result)

        # Try sections
        sections = house_info.get("sections", [])
        if isinstance(sections, list):
            for section in sections:
                sec_items = section.get("items", [])
                if isinstance(sec_items, list):
                    for item in sec_items:
                        _extract_house_field(item, result)

    # Try alternative: aboutHouseBlock
    about_block = loader.get("aboutHouseBlock") or loader.get("aboutHouse")
    if isinstance(about_block, dict) and not result:
        sections = about_block.get("sections", [])
        if isinstance(sections, list):
            for section in sections:
                sec_items = section.get("items", [])
                if isinstance(sec_items, list):
                    for item in sec_items:
                        _extract_house_field(item, result)

    # Deep search: look for any key containing house characteristics
    if not result:
        result = _deep_search_house_fields(loader)

    # Rating
    rating_data = loader.get("rating") or loader.get("houseRating")
    if isinstance(rating_data, dict):
        result["rating"] = rating_data.get("value") or rating_data.get("score")
        result["review_count"] = rating_data.get("count") or rating_data.get("reviewCount")

    # Price range
    price_range = loader.get("priceRange") or loader.get("priceSummary")
    if isinstance(price_range, dict):
        result["price_min"] = price_range.get("min") or price_range.get("minPrice")
        result["price_max"] = price_range.get("max") or price_range.get("maxPrice")

    # Active listings count
    listings_data = loader.get("listings") or loader.get("activeListings")
    if isinstance(listings_data, dict):
        result["active_listings"] = listings_data.get("total") or listings_data.get("count")
    elif isinstance(listings_data, list):
        result["active_listings"] = len(listings_data)

    return result if result else None


def _extract_house_field(item: dict, result: dict) -> None:
    """Extract a single house field from structured data."""
    title = item.get("title") or item.get("name") or item.get("label", "")
    value = item.get("value") or item.get("description") or item.get("text", "")

    if not title or not value:
        return

    db_field = HOUSE_FIELD_MAP.get(title)
    if db_field:
        result[db_field] = str(value)


def _deep_search_house_fields(data: dict, depth: int = 0) -> dict:
    """Recursively search for house fields in nested dict."""
    if depth > 5:
        return {}

    result = {}

    for key, val in data.items():
        if isinstance(val, dict):
            # Check if this dict has title/value structure
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


# --- HTTP fetch functions ---

async def fetch_search_page(client: httpx.AsyncClient, category_slug: str, page: int) -> str | None:
    """Fetch a search results page."""
    url = f"{BASE_URL}/{CITY}/kvartiry/{category_slug}?p={page}"
    try:
        resp = await client.get(url)
        if resp.status_code == 200:
            return resp.text
        logger.warning("Search page %d returned %d", page, resp.status_code)
        return None
    except Exception as e:
        logger.error("Failed to fetch search page %d: %s", page, e)
        return None


async def fetch_listing_page(client: httpx.AsyncClient, url: str) -> str | None:
    """Fetch a single listing page."""
    full_url = url if url.startswith("http") else f"{BASE_URL}{url}"
    try:
        resp = await client.get(full_url)
        if resp.status_code == 200:
            return resp.text
        return None
    except Exception as e:
        logger.error("Failed to fetch listing %s: %s", url, e)
        return None


async def fetch_house_page(client: httpx.AsyncClient, slug: str, address_id: int) -> str | None:
    """Fetch a house catalog page."""
    url = f"{BASE_URL}/catalog/houses/{CITY}/{slug}/{address_id}"
    try:
        resp = await client.get(url)
        if resp.status_code == 200:
            return resp.text
        logger.warning("House page %d returned %d", address_id, resp.status_code)
        return None
    except Exception as e:
        logger.error("Failed to fetch house %d: %s", address_id, e)
        return None
