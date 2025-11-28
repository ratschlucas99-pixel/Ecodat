"""
Functions to generate suggested start and end times for field visits and
flag records requiring manual checks.

This module encapsulates the logic for deriving a suggested start and
end time for each field visit based on project codes, day parts and
astronomical sunrise/sunset times.  It also provides a helper to flag
records where additional review is necessary.  By splitting these
utilities into a separate file, scripts that wish to suggest times or
flag anomalies can import just what they need without pulling in
higher‑level field visit processing code.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

try:
    # Try relative import when part of a package
    from .fieldvisit_utils import parse_local, to_local  # type: ignore
except Exception:
    # Fallback to absolute import when running as a standalone script
    from fieldvisit_utils import parse_local, to_local  # type: ignore


def _extract_project_and_daypart(name: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """Extract a project code and day part (morning/evening) from a name.

    The original implementation used a separate function to clean and
    normalise field visit names.  Since that function has been removed,
    this helper replicates only the portions needed by the time
    suggestion logic.  Project codes consist of patterns like VM01,
    VM02, VM03, GZ, ZR, HM or Uitvliegtelling (case insensitive) and
    can include separators like hyphens or spaces.  Day parts are
    either ``ochtend`` (morning) or ``avond`` (evening) optionally
    followed by a number or Roman numeral (I, II, III).

    Parameters
    ----------
    name : Optional[str]
        The raw field visit name to parse.

    Returns
    -------
    Tuple[Optional[str], Optional[str]]
        The normalised project code and day part.  If nothing can be
        extracted, ``None`` is returned for that element.
    """
    if not isinstance(name, str) or not name:
        return (None, None)
    name_lower = name.lower()
    # Extract project code
    match = re.search(r"([vzwh]m[- ]?\d+|gz|zr|hm|uitvliegtelling)", name_lower, re.IGNORECASE)
    project = None
    if match:
        project = match.group(1).upper().replace(" ", "").replace("-", "")
        # normalise VM single digit codes to have a leading zero (e.g. VM1 -> VM01)
        m2 = re.match(r"VM(\d)$", project, re.IGNORECASE)
        if m2:
            project = f"VM0{m2.group(1)}"
        # Convert WM prefix to VM
        if project.startswith("WM"):
            project = project.replace("WM", "VM", 1)
    # Extract day part
    dagdeel_match = re.search(r"\b(avond|ochtend)\s*([0-9]+|I{1,3})?", name_lower, re.IGNORECASE)
    dagdeel = None
    if dagdeel_match:
        t = dagdeel_match.group(1).lower()
        n_raw = dagdeel_match.group(2)
        # normalise numerals (roman or integer)
        n_norm = None
        if n_raw:
            roman_map = {"i": "1", "ii": "2", "iii": "3"}
            lower = n_raw.lower()
            if lower in roman_map:
                n_norm = roman_map[lower]
            else:
                try:
                    n_norm = str(int(n_raw))
                except ValueError:
                    n_norm = None
        if n_norm:
            dagdeel = f"{t} {n_norm}"
        else:
            dagdeel = t
    return (project, dagdeel)


def get_fieldvisit_time_suggest(df: pd.DataFrame) -> pd.DataFrame:
    """Suggest start and end times for field visits based on project/daypart.

    This function computes suggested start and end times for each
    observation in ``df``.  It first parses the raw start and end
    datetimes into timezone aware values using :func:`parse_local`.
    The sunrise and sunset columns (if present) are converted to local
    time using :func:`to_local`.  The project code and day part are
    extracted from the ``Naam`` column on the fly, so no separate
    cleaning step is required.

    The adjustment rules mirror those from the original
    ``get_fieldvisit_timesuggest`` function but will gracefully skip
    adjustments if the project or day part cannot be determined.

    Parameters
    ----------
    df : pandas.DataFrame
        Input table containing at minimum the following columns:
        ``Startdatum``, ``Einddatum``, ``Naam`` and optionally
        ``sunrise`` and ``sunset``.  Extra columns are preserved.

    Returns
    -------
    pandas.DataFrame
        A new DataFrame with additional columns:
        ``Starttijd_Suggest``, ``Eindtijd_Suggest``, ``duur_suggest``,
        ``Project`` and ``Dagdeel``.  The original columns are
        untouched.
    """
    df = df.copy()
    # Parse local datetimes
    df["Starttijd_Suggest"] = df.get("Startdatum").apply(parse_local)
    df["Eindtijd_Suggest"] = df.get("Einddatum").apply(parse_local)
    df["sunrise_local"] = df.get("sunrise").apply(lambda x: to_local(x) if not pd.isna(x) else None)
    df["sunset_local"] = df.get("sunset").apply(lambda x: to_local(x) if not pd.isna(x) else None)

    projects: List[Optional[str]] = []
    dagdelen: List[Optional[str]] = []
    # Precompute project/daypart for each row from the name
    names = df.get("Naam", pd.Series([None] * len(df)))
    for name in names:
        project, dagdeel = _extract_project_and_daypart(name)
        projects.append(project)
        dagdelen.append(dagdeel)
    df["Project"] = projects
    df["Dagdeel"] = dagdelen

    # Containers for results
    results_start: List[Optional[datetime]] = []
    results_end: List[Optional[datetime]] = []
    results_dur: List[Optional[float]] = []

    # Iterate rows and apply adjustment logic
    for _, row in df.iterrows():
        proj: str = row.get("Project") or ""
        dagdeel: str = row.get("Dagdeel") or ""
        start: Optional[datetime] = row.get("Starttijd_Suggest")
        end: Optional[datetime] = row.get("Eindtijd_Suggest")
        sunrise_local: Optional[datetime] = row.get("sunrise_local")
        sunset_local: Optional[datetime] = row.get("sunset_local")

        # Helper to update a datetime's time components
        def update_time(dt: datetime, hour: int, minute: int, second: int = 0) -> datetime:
            return dt.replace(hour=hour, minute=minute, second=second)

        # Evening adjustments for VM01
        if re.fullmatch(r"VM01", proj, flags=re.IGNORECASE) and dagdeel.startswith("avond"):
            if start and sunset_local:
                if start > sunset_local or start < sunset_local - timedelta(hours=1):
                    start = sunset_local
            if end and sunset_local:
                three_h = sunset_local + timedelta(hours=3)
                four_h = sunset_local + timedelta(hours=4)
                if end < three_h or end > four_h:
                    end = three_h
        # Morning adjustments for VM01
        if re.fullmatch(r"VM01", proj, flags=re.IGNORECASE) and dagdeel.startswith("ochtend"):
            if end and sunrise_local:
                start_of_window = sunrise_local
                end_of_window = sunrise_local + timedelta(hours=4)
                if end < start_of_window or end > end_of_window:
                    end = start_of_window
            if start and sunrise_local:
                start_window = sunrise_local - timedelta(hours=4)
                end_window = sunrise_local - timedelta(hours=3)
                if start < start_window or start > end_window:
                    start = sunrise_local - timedelta(hours=3)
        # VM02 evening rules
        if re.fullmatch(r"VM02", proj, flags=re.IGNORECASE) and dagdeel.startswith("avond"):
            if start:
                start_min = start.hour * 60 + start.minute
                lower = 22 * 60 + 59
                upper = 23 * 60 + 59
                if start_min < lower or start_min > upper:
                    start = update_time(start, 23, 59, 0)
            if start and end and end <= start:
                end = end + timedelta(days=1)
            if end:
                end_min = end.hour * 60 + end.minute
                if end_min < 2 * 60:
                    end = update_time(end, 2, 0, 0)
                elif end_min > 3 * 60:
                    end = update_time(end, 3, 0, 0)
        # GZ projects: evening relative to sunset
        if re.search(r"GZ", proj, flags=re.IGNORECASE):
            if start and sunset_local:
                lower = sunset_local - timedelta(minutes=150)
                upper = sunset_local - timedelta(minutes=90)
                if start > upper or start < lower:
                    start = sunset_local - timedelta(minutes=90)
            if end and sunset_local:
                lower = sunset_local + timedelta(minutes=30)
                upper = sunset_local + timedelta(minutes=90)
                if end < lower or end > upper:
                    end = sunset_local + timedelta(minutes=30)
        # ZR projects: morning relative to sunrise
        if re.search(r"ZR", proj, flags=re.IGNORECASE):
            if start and sunrise_local:
                lower = sunrise_local - timedelta(minutes=150)
                upper = sunrise_local - timedelta(minutes=90)
                if start > upper or start < lower:
                    start = sunrise_local - timedelta(minutes=90)
            if end and sunrise_local:
                lower = sunrise_local + timedelta(minutes=30)
                upper = sunrise_local + timedelta(minutes=90)
                if end < lower or end > upper:
                    end = sunrise_local + timedelta(minutes=30)
        # Compute duration in hours if both times present
        duration: Optional[float]
        if start and end:
            duration = (end - start).total_seconds() / 3600.0
        else:
            duration = np.nan
        results_start.append(start)
        results_end.append(end)
        results_dur.append(duration)

    df["Starttijd_Suggest"] = results_start
    df["Eindtijd_Suggest"] = results_end
    df["duur_suggest"] = results_dur

    return df


def flag_fieldtime_changes(df: pd.DataFrame) -> pd.DataFrame:
    """Flag field visits that may require manual data checks.

    A record will be marked for checking if:

      * The project code starts with "VM03" (case insensitive).
      * Either sunrise or sunset is missing (NaT/NaN).
      * The cleaned name (if available) is missing or empty.  If the
        ``Naam_schoon`` column is absent, the raw ``Naam`` is used instead.

    Parameters
    ----------
    df : pandas.DataFrame
        A DataFrame produced by :func:`get_fieldvisit_time_suggest` or
        another source containing at least the columns ``Project``,
        ``sunrise`` and ``sunset``.  Optional columns ``Naam`` and
        ``Naam_schoon`` will be inspected for missing values.

    Returns
    -------
    pandas.DataFrame
        The input DataFrame with an additional column ``check_data``
        containing "yes" if manual review is suggested and "no"
        otherwise.
    """
    df = df.copy()
    # Determine which name column to inspect for emptiness
    if "Naam_schoon" in df.columns:
        name_col_to_check = "Naam_schoon"
    elif "Naam" in df.columns:
        name_col_to_check = "Naam"
    else:
        # If neither column exists, create an empty string series to avoid errors
        df["_tmp_name"] = ""
        name_col_to_check = "_tmp_name"

    name_series = df[name_col_to_check].fillna("")
    naam_missing = name_series.str.strip() == ""

    # Check project codes if present
    proj_series = df.get("Project", pd.Series([""] * len(df)))
    vm03_mask = proj_series.fillna("").str.contains(r"^VM03", case=False, regex=True)
    sunrise_missing = df.get("sunrise").isna()
    sunset_missing = df.get("sunset").isna()

    check_mask = vm03_mask | sunrise_missing | sunset_missing | naam_missing
    df["check_data"] = np.where(check_mask, "yes", "no")

    # Clean up temporary column if created
    if "_tmp_name" in df.columns:
        df = df.drop(columns=["_tmp_name"])
    return df