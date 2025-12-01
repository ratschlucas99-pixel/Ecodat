
from __future__ import annotations

from datetime import datetime
from typing import Any, List, Optional, Iterable, Tuple

import numpy as np
import pandas as pd
import pytz
try:

    from astral import sun
    from astral import LocationInfo
    _HAS_ASTRAL = True
except Exception:
    _HAS_ASTRAL = False

from math import acos, asin, atan, ceil, cos, degrees, fmod, radians, sin, sqrt
def _julian_date(dt: datetime) -> float:
    ts = dt.timestamp()
    return ts / 86400.0 + 2440587.5

def _ts_from_julian(j: float) -> float:

    return (j - 2440587.5) * 86400.0

def _fallback_sunrise_sunset(
    date_obj: date,
    latitude: float,
    longitude: float,
    tz: pytz.BaseTzInfo,
    elevation: float = 0.0,
) -> Tuple[Optional[datetime], Optional[datetime]]:

    midday = datetime(date_obj.year, date_obj.month, date_obj.day, 12, 0, 0, tzinfo=pytz.UTC)
    J_date = _julian_date(midday)

    l_w = -longitude
    n = ceil(J_date - (2451545.0 + 0.0009) + 69.184 / 86400.0)
    J_star = n + 0.0009 - l_w / 360.0
    M_deg = fmod(357.5291 + 0.98560028 * J_star, 360.0)
    M_rad = radians(M_deg)
    # Equation of the center (degrees)
    C_deg = 1.9148 * sin(M_rad) + 0.02 * sin(2.0 * M_rad) + 0.0003 * sin(3.0 * M_rad)
    # Ecliptic longitude (degrees)
    lambda_deg = fmod(M_deg + C_deg + 180.0 + 102.9372, 360.0)
    lambda_rad = radians(lambda_deg)
    # Solar transit (Julian date)
    J_transit = 2451545.0 + J_star + 0.0053 * sin(M_rad) - 0.0069 * sin(2.0 * lambda_rad)
    # Sun declination
    sin_delta = sin(lambda_rad) * sin(radians(23.4397))
    cos_delta = cos(asin(sin_delta))
    # Hour angle calculation
    dip = -0.833 - 2.076 * sqrt(max(elevation, 0.0)) / 60.0
    # Compute cosine of hour angle
    some_cos = (
        sin(radians(dip)) - sin(radians(latitude)) * sin_delta
    ) / (cos(radians(latitude)) * cos_delta)
    # If |some_cos| > 1 then the sun never rises/sets
    if some_cos <= -1.0:
        sunrise_j = J_transit - 0.5
        sunset_j = None
    elif some_cos >= 1.0:
        sunrise_j = None
        sunset_j = None
    else:
        w0_rad = acos(some_cos)
        w0_deg = degrees(w0_rad)
        sunrise_j = J_transit - w0_deg / 360.0
        sunset_j = J_transit + w0_deg / 360.0
    # Convert Julian dates to UTC datetimes and then to the requested timezone
    def julian_to_dt(j: Optional[float]) -> Optional[datetime]:
        if j is None:
            return None
        ts = _ts_from_julian(j)
        dt_utc = datetime.utcfromtimestamp(ts).replace(tzinfo=pytz.UTC)
        return dt_utc.astimezone(tz)

    sunrise_dt = julian_to_dt(sunrise_j)
    sunset_dt = julian_to_dt(sunset_j)
    return sunrise_dt, sunset_dt

TIMEZONE = pytz.timezone("Europe/Amsterdam")

def parse_local(
    dt: Any,
    tz: pytz.BaseTzInfo = TIMEZONE,
) -> Optional[datetime]:
    if isinstance(dt, pd.Timestamp):
        if dt.tzinfo is None:
            return tz.localize(dt.to_pydatetime())
        return dt.tz_convert(tz)
    elif isinstance(dt, datetime):
        if dt.tzinfo is None:
            return tz.localize(dt)
        return dt.astimezone(tz)
    elif isinstance(dt, str) and dt:
        try:
            ts = pd.to_datetime(dt, errors="coerce")
            if pd.isna(ts):
                return None
            return parse_local(ts.to_pydatetime(), tz)
        except Exception:
            return None
    return None


def to_local(
    dt: Any,
    tz: pytz.BaseTzInfo = TIMEZONE,
) -> Optional[datetime]:

    if isinstance(dt, pd.Timestamp):
        dt = dt.to_pydatetime()
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = pytz.UTC.localize(dt)
        try:
            return dt.astimezone(tz)
        except Exception:
            return None
    if isinstance(dt, str) and dt:
        try:
            ts = pd.to_datetime(dt, errors="coerce", utc=True)
            if pd.isna(ts):
                return None
            return to_local(ts.to_pydatetime(), tz)
        except Exception:
            return None
    return None

def get_fieldvisit_suntimes(
    observations: pd.DataFrame,
    fieldvisits: pd.DataFrame,
    projects: pd.DataFrame,
    coord_col: str = "Coördinaten",
    fieldvisit_id_col_obs: str = "Veldbezoek ID...23",
    project_id_col_obs: str = "Project ID...24",
    fieldvisit_id_col: str = "ID",
    project_id_col: str = "project_id",
    outfile: Optional[str] = None,
) -> pd.DataFrame:

    # Project city names and IDs
    projects_df = projects[["ID", "Stad", "Naam"]].rename(
        columns={"ID": "Project_ID"}
    )

    #Take first coordinate per field visit
    unique_gps = (
        observations.dropna(subset=[coord_col])
        .groupby(fieldvisit_id_col_obs)
        .first()
        .reset_index()
        [[fieldvisit_id_col_obs, project_id_col_obs, coord_col]]
        .rename(columns={
            fieldvisit_id_col_obs: "Veldbezoek_ID",
            project_id_col_obs: "Project_ID",
        })
    )

    project_cities_gps = pd.merge(
        projects_df,
        unique_gps,
        on="Project_ID",
        how="inner",
    )

    fieldvisit_dates = (
        fieldvisits[[fieldvisit_id_col, project_id_col, "Startdatum"]]
        .rename(columns={
            fieldvisit_id_col: "Veldbezoek_ID",
            project_id_col: "Project_ID",
        })
    )

    fieldvisit_dates["Datum"] = pd.to_datetime(
        fieldvisit_dates["Startdatum"], errors="coerce"
    ).dt.date

    loc_date = pd.merge(
        project_cities_gps,
        fieldvisit_dates[["Veldbezoek_ID", "Datum"]],
        on="Veldbezoek_ID",
        how="inner",
    )

    # Split coordinates into lat/lon
    loc_date[["lat", "lon"]] = (
        loc_date[coord_col].str.split(",", expand=True).iloc[:, :2]
    )
    loc_date["lat"] = pd.to_numeric(loc_date["lat"], errors="coerce")
    loc_date["lon"] = pd.to_numeric(loc_date["lon"], errors="coerce")

    # Compute sunrise and sunset
    sunrise_list: List[Optional[datetime]] = []
    sunset_list: List[Optional[datetime]] = []
    for _, row in loc_date.iterrows():
        lat, lon, dt = row["lat"], row["lon"], row["Datum"]
        if pd.isna(lat) or pd.isna(lon) or pd.isna(dt):
            sunrise_list.append(None)
            sunset_list.append(None)
            continue
        try:
            if _HAS_ASTRAL:
                #Use Astral when available
                location = LocationInfo(latitude=lat, longitude=lon)
                sun_times = sun.sun(location.observer, date=dt, tzinfo=pytz.UTC)
                sr = to_local(sun_times["sunrise"], TIMEZONE)
                ss = to_local(sun_times["sunset"], TIMEZONE)
            else:
                #Fallback to the NOAA approximation
                sr, ss = _fallback_sunrise_sunset(dt, lat, lon, TIMEZONE)
            sunrise_list.append(sr)
            sunset_list.append(ss)
        except Exception:
            sunrise_list.append(None)
            sunset_list.append(None)

    loc_date["sunrise"] = sunrise_list
    loc_date["sunset"] = sunset_list

    result = loc_date[["Project_ID", "Naam", "Veldbezoek_ID", "Stad", coord_col, "sunrise", "sunset"]]

    if outfile:
        import os
        os.makedirs(os.path.dirname(outfile), exist_ok=True)
        result.to_csv(outfile, index=False, sep=";")
    return result
