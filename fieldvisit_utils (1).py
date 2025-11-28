
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

    # Unix timestamp in seconds
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
    # Atmospheric refraction and solar disc height (-0.833 degrees) plus dip due to elevation.
    # Convert elevation in metres to dip in degrees (approx −2.076° * sqrt(h) / 60).
    dip = -0.833 - 2.076 * sqrt(max(elevation, 0.0)) / 60.0
    # Compute cosine of hour angle
    some_cos = (
        sin(radians(dip)) - sin(radians(latitude)) * sin_delta
    ) / (cos(radians(latitude)) * cos_delta)
    # If |some_cos| > 1 then the sun never rises/sets on this date.
    if some_cos <= -1.0:
        # Sun is above horizon all day (polar day) -> no sunset
        sunrise_j = J_transit - 0.5  # placeholder: 12 hours earlier
        sunset_j = None
    elif some_cos >= 1.0:
        # Sun is below horizon all day (polar night) -> no sunrise
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

# Default timezone used throughout the project.  Feel free to
# override this when calling the functions below if you work in a
# different region.
TIMEZONE = pytz.timezone("Europe/Amsterdam")


def parse_local(
    dt: Any,
    tz: pytz.BaseTzInfo = TIMEZONE,
) -> Optional[datetime]:
    """Convert a variety of timestamp inputs into a timezone‑aware datetime.

    Parameters
    ----------
    dt : Any
        A pandas.Timestamp, datetime, or string representing a date/time.
        If a string is provided, it will be parsed with pandas.to_datetime.
    tz : pytz.BaseTzInfo, optional
        The timezone to localize naive datetimes to.  Defaults to
        Europe/Amsterdam.

    Returns
    -------
    Optional[datetime]
        A timezone‑aware datetime or ``None`` if the input could not be
        parsed.
    """
    if isinstance(dt, pd.Timestamp):
        if dt.tzinfo is None:
            # Treat stored clock time as local
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
    """Convert a timestamp to the given timezone.

    This function accepts a pandas.Timestamp, python datetime, or string
    and returns a timezone aware datetime in the requested timezone.  If
    the input is naive (has no timezone information) it is assumed to
    represent UTC and will be localized accordingly before converting.

    Parameters
    ----------
    dt : Any
        Timestamp, datetime, or ISO 8601 string.
    tz : pytz.BaseTzInfo, optional
        The desired timezone for the returned datetime.  Defaults
        to Europe/Amsterdam.

    Returns
    -------
    Optional[datetime]
        A timezone aware datetime or ``None`` if conversion failed.
    """
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
    """Compute sunrise and sunset times for each field visit location.

    This function extracts a representative latitude/longitude per field
    visit (using the first non‑null coordinate from the observations
    table) and combines it with the visit date and project city to look
    up sunrise and sunset times via the Astral library.  All returned
    datetimes are timezone aware using the module's default TIMEZONE.

    Parameters
    ----------
    observations : pandas.DataFrame
        Raw observations containing a coordinate column and field visit
        identifiers.
    fieldvisits : pandas.DataFrame
        Table of field visits with start dates.
    projects : pandas.DataFrame
        Table of project metadata including the city name ("Stad") and
        project name ("Naam").
    coord_col : str, optional
        Column in ``observations`` containing "lat, lon" strings.
    fieldvisit_id_col_obs : str, optional
        Column in ``observations`` holding the field visit ID.
    project_id_col_obs : str, optional
        Column in ``observations`` holding the project ID.
    fieldvisit_id_col : str, optional
        Column in ``fieldvisits`` holding the field visit ID.
    project_id_col : str, optional
        Column in ``fieldvisits`` holding the project ID.
    outfile : Optional[str], optional
        If provided, write the resulting table to CSV at this path.

    Returns
    -------
    pandas.DataFrame
        A table with one row per field visit containing project and
        location metadata plus timezone aware sunrise and sunset times.
    """
    # Project city names and IDs
    projects_df = projects[["ID", "Stad", "Naam"]].rename(
        columns={"ID": "Project_ID"}
    )

    # Take first coordinate per field visit
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

    # Compute sunrise and sunset per row.  Prefer Astral when available;
    # otherwise fall back to our internal approximation.  The fallback
    # algorithm produces reasonable values but may differ by several
    # minutes from Astral.  When no sunrise or sunset occurs (e.g.
    # polar day/night) ``None`` is stored for that field.
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
                # Use Astral for high precision when available
                location = LocationInfo(latitude=lat, longitude=lon)  # type: ignore[name-defined]
                sun_times = sun.sun(location.observer, date=dt, tzinfo=pytz.UTC)  # type: ignore[name-defined]
                sr = to_local(sun_times["sunrise"], TIMEZONE)
                ss = to_local(sun_times["sunset"], TIMEZONE)
            else:
                # Fallback to the NOAA-based approximation
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