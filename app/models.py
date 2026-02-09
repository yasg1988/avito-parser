from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class House(BaseModel):
    address_id: int
    slug: Optional[str] = None
    address: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None

    # Основное
    build_year: Optional[str] = None
    floors: Optional[str] = None

    # Коммуникации
    heating: Optional[str] = None
    hot_water: Optional[str] = None
    cold_water: Optional[str] = None
    electricity: Optional[str] = None
    gas: Optional[str] = None
    sewerage: Optional[str] = None
    ventilation: Optional[str] = None

    # Удобства
    passenger_lift: Optional[str] = None
    freight_lift: Optional[str] = None

    # Материалы
    house_type: Optional[str] = None
    floor_type: Optional[str] = None
    foundation: Optional[str] = None
    energy_class: Optional[str] = None

    # Инфраструктура
    playground: Optional[str] = None
    sports_ground: Optional[str] = None
    parking: Optional[str] = None

    # Рейтинг
    rating: Optional[float] = None
    review_count: Optional[int] = None

    # Цены
    price_min: Optional[int] = None
    price_max: Optional[int] = None
    active_listings: Optional[int] = None

    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class Listing(BaseModel):
    item_id: int
    address_id: Optional[int] = None
    title: Optional[str] = None
    price: Optional[int] = None
    listing_type: Optional[str] = None
    address: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    rooms: Optional[int] = None
    area: Optional[float] = None
    floor: Optional[int] = None
    total_floors: Optional[int] = None
    url: Optional[str] = None
    first_seen_at: Optional[datetime] = None
    last_seen_at: Optional[datetime] = None


class ScanStatus(BaseModel):
    status: str = "idle"
    phase: Optional[str] = None
    category: Optional[str] = None
    total_pages: int = 0
    done_pages: int = 0
    total_houses: int = 0
    done_houses: int = 0
    new_houses: int = 0
    listings_found: int = 0
    errors: int = 0
    started_at: Optional[datetime] = None
    message: Optional[str] = None


class StatsResponse(BaseModel):
    total_houses: int = 0
    total_listings: int = 0
    listings_sale: int = 0
    listings_rent_long: int = 0
    listings_rent_short: int = 0
    houses_with_details: int = 0
    last_scan: Optional[datetime] = None


class MonitoringResponse(BaseModel):
    status: str
    service: str = "avito-parser"
    total_houses: int = 0
    total_listings: int = 0
    last_scan: Optional[datetime] = None
    alerts: list[str] = []
