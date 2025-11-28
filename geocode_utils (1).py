"""
Reverse geocoding utilities using geopy.

This module provides a convenient wrapper around geopy's geocoders to
perform reverse geocoding on rows of a pandas DataFrame.  It offers
flexibility to choose between different geocoding services (e.g.,
Nominatim or ArcGIS) and includes caching and rate limiting to respect
service usage policies.  Moving this logic into its own file allows
observation processing code to remain focused on data transformations
and simplifies testing of the geocoder itself.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
try:
    # Attempt to import geopy for reverse geocoding.  If the import
    # fails (e.g. because the package is not installed), we set a flag
    # indicating that geocoding is unavailable.  The functions below
    # will gracefully degrade by returning ``None`` for all addresses.
    from geopy.geocoders import Nominatim, ArcGIS  # type: ignore
    from geopy.extra.rate_limiter import RateLimiter  # type: ignore
    from geopy.location import Location  # type: ignore
    _HAS_GEOPY = True
except Exception:
    _HAS_GEOPY = False

from tqdm import tqdm  # type: ignore


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
    """Perform reverse geocoding on selected observations.

    Each row of ``df`` where the latitude and longitude columns are
    present (and optionally the behaviour column matches one of
    ``behaviours``) will be reverse geocoded to an address string.
    Results are cached on disk if ``cache_file`` is provided to
    minimise repeated requests.  You can choose between different
    geocoding backends by setting the ``geocoder`` argument.  At
    present ``"nominatim"`` (default) and ``"arcgis"`` are supported.

    Parameters
    ----------
    df : pandas.DataFrame
        Input DataFrame containing latitude and longitude columns.
    lat_col : str, optional
        Name of the latitude column.  Defaults to ``"Breedtegraad"``.
    lon_col : str, optional
        Name of the longitude column.  Defaults to ``"Lengtegraad"``.
    behaviours : Optional[Iterable[str]], optional
        If provided, only rows whose behaviour column value is in this
        iterable will be geocoded.  Otherwise all rows with valid
        coordinates are processed.
    behaviour_col : str, optional
        Name of the behaviour column used when ``behaviours`` is not
        ``None``.  Defaults to ``"Gedrag"``.
    cache_file : Optional[str], optional
        Path to a JSON file where geocoding results will be stored
        between runs.  If the file exists its contents will be read
        into memory; updates are written out after processing.
    user_agent : str, optional
        User agent string passed to the geocoder.  Some services
        require this to identify the client.
    min_delay_seconds : float, optional
        Minimum delay in seconds between requests enforced by the
        rate limiter.  Adjust this to comply with your chosen
        provider's terms of service.
    geocoder : str, optional
        Which geocoding service to use.  Supported values are
        ``"nominatim"`` (OpenStreetMap Nominatim) and ``"arcgis"``.
    **geocoder_kwargs : Any
        Additional keyword arguments passed through to the geocoder
        constructor.  For example, when using ``arcgis`` you may
        supply a ``referer``.

    Returns
    -------
    pandas.DataFrame
        A copy of ``df`` with a new column ``address`` containing
        geocoded address strings or ``None`` where no address could be
        resolved.
    """
    df = df.copy()
    # If geopy is unavailable, skip geocoding entirely.  Set the
    # ``address`` column to None for all eligible rows and return early.
    if not _HAS_GEOPY:
        df["address"] = None
        return df

    # Load or initialise cache
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
        geolocator = ArcGIS(user_agent=user_agent, **geocoder_kwargs)  # type: ignore[name-defined]
    else:
        geolocator = Nominatim(user_agent=user_agent, **geocoder_kwargs)  # type: ignore[name-defined]
    rate_limited = RateLimiter(
        geolocator.reverse,  # type: ignore[attr-defined]
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

    # Persist cache
    if cache_file:
        try:
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(cache, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    df["address"] = addresses
    return df


def parse_address(addr: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """Parse a geocoded address into a street and city.

    This helper splits a raw address string into two components: a street
    (including house number) and a place/city.  The parsing rules are
    intentionally simple and may need to be adjusted for specific
    locales.  If either component cannot be determined, ``None`` is
    returned for that part.

    Parameters
    ----------
    addr : Optional[str]
        A geocoded address string, usually produced by
        :func:`reverse_geocode`.

    Returns
    -------
    Tuple[Optional[str], Optional[str]]
        The ``(street, city)`` extracted from the input.
    """
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