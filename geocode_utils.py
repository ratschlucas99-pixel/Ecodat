
from __future__ import annotations

import json
import os
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
try:
    from geopy.geocoders import Nominatim, ArcGIS
    from geopy.extra.rate_limiter import RateLimiter
    from geopy.location import Location
    _HAS_GEOPY = True
except Exception:
    _HAS_GEOPY = False

from tqdm import tqdm


def reverse_geocode(
    df: pd.DataFrame,
    lat_col: str = "Breedtegraad",
    lon_col: str = "Lengtegraad",
    behaviours: Optional[Iterable[str]] = None,
    behaviour_col: str = "Gedrag",
    cache_file: Optional[str] = None,
    user_agent: str = "observations_geocoder",
    min_delay_seconds: float = 1.1,
    geocoder: str = "nominatim",
    **geocoder_kwargs: Any,
) -> pd.DataFrame:

    df = df.copy()

    if not _HAS_GEOPY:
        df["address"] = None
        return df

    # Load cache
    cache: Dict[str, str] = {}
    if cache_file and os.path.isfile(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                cache = json.load(f)
        except Exception:
            cache = {}

    # Determine which rows to geocode
    mask = df[lat_col].notna() & df[lon_col].notna()
    if behaviours is not None:
        mask &= df[behaviour_col].isin(list(behaviours))

    # Instantiate geocoder
    if geocoder.lower() == "arcgis":
        geolocator = ArcGIS(user_agent=user_agent, **geocoder_kwargs)
    else:
        geolocator = Nominatim(user_agent=user_agent, **geocoder_kwargs)
    rate_limited = RateLimiter(
        geolocator.reverse,
        min_delay_seconds=min_delay_seconds,
        max_retries=3,
        error_wait_seconds=3.0,
    )

    addresses: List[Optional[str]] = [None] * len(df)
    for idx in tqdm(df.index, desc="Reverse geocoding", disable=(~mask).all()):
        if not mask.loc[idx]:
            continue
        lat = df.at[idx, lat_col]
        lon = df.at[idx, lon_col]
        key = f"{lat},{lon}"
        addr = cache.get(key)
        if addr is None:
            try:
                location: Location | None = rate_limited((lat, lon), exactly_one=True)  # type: ignore[name-defined]
                addr = location.address if location else None
            except Exception:
                addr = None
            if addr is not None:
                cache[key] = addr
        addresses[idx] = addr


    if cache_file:
        try:
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(cache, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    df["address"] = addresses
    return df


def parse_address(addr: Optional[str]) -> tuple[Optional[str], Optional[str]]:

    if addr is None or not isinstance(addr, str) or not addr.strip():
        return (None, None)
    parts = [p.strip() for p in addr.split(",") if p.strip()]
    if not parts:
        return (None, None)
    nr = parts[0] if len(parts) >= 1 else None
    straat = parts[1] if len(parts) >= 2 else None
    if len(parts) >= 5:
        plaats = parts[-5]
    else:
        plaats = parts[-1] if parts else None
    adres = None
    if straat and nr:
        adres = f"{straat} {nr}".strip()
    return (adres or None, plaats or None)
