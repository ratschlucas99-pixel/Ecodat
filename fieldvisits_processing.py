from __future__ import annotations
import os
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time, date
import pytz

try:
    from fieldvisit_utils import TIMEZONE
except Exception:
    # Fallback timezone
    TIMEZONE = pytz.timezone("Europe/Amsterdam")

from fieldvisit_utils import get_fieldvisit_suntimes as compute_suntimes
from timesuggest_utils import get_fieldvisit_time_suggest as compute_time_suggest
from timesuggest_utils import flag_fieldtime_changes as compute_flag_fieldtime_changes

def parse_local(dt: Any, tz: pytz.BaseTzInfo = TIMEZONE) -> Optional[datetime]:

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


def to_local(dt: Any, tz: pytz.BaseTzInfo = TIMEZONE) -> Optional[datetime]:
    ##Convert a timestamp to the given timezone
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
    #Sunrise and sunset times for each field visit.
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
    #Split coordinates into lat/lon
    loc_date[["lat", "lon"]] = loc_date[coord_col].str.split(",", expand=True).iloc[:, :2]
    loc_date["lat"] = pd.to_numeric(loc_date["lat"], errors="coerce")
    loc_date["lon"] = pd.to_numeric(loc_date["lon"], errors="coerce")

    #Compute sunrise and sunset per row
    sunrise_list: List[Optional[datetime]] = []
    sunset_list: List[Optional[datetime]] = []
    for _, row in loc_date.iterrows():
        lat, lon, dt = row["lat"], row["lon"], row["Datum"]
        if pd.isna(lat) or pd.isna(lon) or pd.isna(dt):
            sunrise_list.append(None)
            sunset_list.append(None)
            continue
        try:
            location = LocationInfo(latitude=lat, longitude=lon)
            sun_times = sun.sun(location.observer, date=dt, tzinfo=pytz.UTC)

            sunrise_list.append(to_local(sun_times["sunrise"], TIMEZONE))
            sunset_list.append(to_local(sun_times["sunset"], TIMEZONE))
        except Exception:
            sunrise_list.append(None)
            sunset_list.append(None)

    loc_date["sunrise"] = sunrise_list
    loc_date["sunset"] = sunset_list

    result = loc_date[["Project_ID", "Naam", "Veldbezoek_ID", "Stad", coord_col, "sunrise", "sunset"]]

    if outfile:
        os.makedirs(os.path.dirname(outfile), exist_ok=True)
        result.to_csv(outfile, index=False, sep=";")
    return result


def naam_veldbezoeken_schoon(
    df: pd.DataFrame,
    remove_patterns: Optional[Iterable[str]] = None,
    naam_col: str = "Naam",
) -> pd.DataFrame:
    ## Normalize and clean field visit names

    df = df.copy()
    names = df[naam_col].fillna("")

    # Extract project codes
    project_match = names.str.extract(r"(?i)([VW]M[- ]?\d+|GZ|ZR|HM|Uitvliegtelling)")
    project = project_match[0].str.upper().str.replace("[- ]", "", regex=True)
    project = project.str.replace(r"^WM", "VM", regex=True)
    project = project.str.replace(r"^VM(\d)$", lambda m: f"VM0{m.group(1)}", regex=True)

    # Extract day part
    dagdeel_match = names.str.extract(r"(?i)(avond|ochtend)\s*([0-9]+|I{1,3})?")
    dagdeel_type = dagdeel_match[0].str.lower()
    dagdeel_num_raw = dagdeel_match[1]

    # Normalize numerals
    def normalize_dagdeel_num(x: str) -> Optional[str]:
        if pd.isna(x) or x == "":
            return None
        roman_map = {"i": "1", "ii": "2", "iii": "3"}
        x_lower = x.lower()
        if x_lower in roman_map:
            return roman_map[x_lower]
        try:
            return str(int(x))
        except ValueError:
            return None

    dagdeel_num = dagdeel_num_raw.apply(normalize_dagdeel_num)

    dagdeel = []
    for t, n in zip(dagdeel_type, dagdeel_num):
        if pd.isna(t) or t == "":
            dagdeel.append(None)
        elif pd.isna(n) or n is None:
            dagdeel.append(t)
        else:
            dagdeel.append(f"{t} {n}")

    df["Project"] = project
    df["Dagdeel"] = dagdeel

    naam_schoon: List[Optional[str]] = []
    for prj, dd in zip(project, dagdeel):
        if prj is None or prj == "":
            naam_schoon.append(None)
        elif dd is None or dd == "":
            naam_schoon.append(prj)
        else:
            naam_schoon.append(f"{prj} {dd}")
    df["Naam_schoon"] = naam_schoon

    #Determine rows to remove
    if remove_patterns:
        pattern = re.compile("|".join(remove_patterns), re.IGNORECASE)
        df["Rows_Removed"] = np.where(
            df[naam_col].fillna("").str.contains(pattern), "remove", "keep"
        )
    else:
        df["Rows_Removed"] = "keep"

    return df


def get_fieldvisit_timesuggest(df: pd.DataFrame) -> pd.DataFrame:
    ##Suggest start and end times for field visits

    df = df.copy()

    # Convert to local times
    df["Starttijd_Suggest"] = df["Startdatum"].apply(parse_local)
    df["Eindtijd_Suggest"] = df["Einddatum"].apply(parse_local)
    df["sunset_local"] = df["sunset"].apply(lambda x: to_local(x) if not pd.isna(x) else None)
    df["sunrise_local"] = df["sunrise"].apply(lambda x: to_local(x) if not pd.isna(x) else None)
    df["duur_suggest"] = np.nan

    def update_time(dt: datetime, hour: int, minute: int, second: int = 0) -> datetime:
        return dt.replace(hour=hour, minute=minute, second=second)

    df["start_min"] = df["Starttijd_Suggest"].apply(
        lambda x: x.hour * 60 + x.minute if isinstance(x, datetime) else np.nan
    )
    df["end_min"] = df["Eindtijd_Suggest"].apply(
        lambda x: x.hour * 60 + x.minute if isinstance(x, datetime) else np.nan
    )

    results_start = []
    results_end = []
    results_dur = []
    for idx, row in df.iterrows():
        # Extract project and day part
        proj_val = row.get("Project")
        if isinstance(proj_val, str):
            proj = proj_val
        elif pd.isna(proj_val):
            proj = ""
        else:
            proj = str(proj_val)
        dagdeel_val = row.get("Dagdeel")
        if isinstance(dagdeel_val, str):
            dagdeel = dagdeel_val
        elif pd.isna(dagdeel_val):
            dagdeel = ""
        else:
            dagdeel = str(dagdeel_val)
        start = row.get("Starttijd_Suggest")
        end = row.get("Eindtijd_Suggest")
        sunset_local = row.get("sunset_local")
        sunrise_local = row.get("sunrise_local")

        # Evening adjustments
        if re.match(r"VM01", proj, flags=re.IGNORECASE) and dagdeel.startswith("avond"):
            if start and sunset_local:
                if start > sunset_local or start < sunset_local - timedelta(hours=1):
                    start = sunset_local
            if end and sunset_local:
                three_h = sunset_local + timedelta(hours=3)
                four_h = sunset_local + timedelta(hours=4)
                if end < three_h or end > four_h:
                    end = three_h

        # Morning adjustments
        if re.match(r"VM01", proj, flags=re.IGNORECASE) and dagdeel.startswith("ochtend"):
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

        # VM02 evening start time must be 22:59–23:59
        if re.fullmatch(r"VM02", proj, flags=re.IGNORECASE) and dagdeel.startswith("avond"):
            if start:
                start_min = start.hour * 60 + start.minute
                lower = 22 * 60 + 59
                upper = 23 * 60 + 59
                if start_min < lower or start_min > upper:
                    start = update_time(start, 23, 59, 0)
            # If end is not after start, move to next day
            if start and end and end <= start:
                end = end + timedelta(days=1)
            # End must be within 02:00–03:00 on following day
            if end:
                end_min = end.hour * 60 + end.minute
                if end_min < 2 * 60:
                    end = update_time(end, 2, 0, 0)
                elif end_min > 3 * 60:
                    end = update_time(end, 3, 0, 0)

        # GZ: evening visits relative to sunset
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

        # ZR: morning visits relative to sunrise (1.5h before to 0.5h after)
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

        # Compute duration if both start and end present
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
    ##Flag field visits that require manual data checks
    df = df.copy()
    naam_missing = df["Naam_schoon"].isna() | (df["Naam_schoon"].str.strip() == "")
    check_mask = (
        df["Project"].fillna("").str.contains(r"^VM03", case=False) |
        df["sunrise"].isna() |
        df["sunset"].isna() |
        naam_missing
    )
    df["check_data"] = np.where(check_mask, "yes", "no")
    return df


def transform_fieldvisits(
    observations_csv: str,
    fieldvisits_csv: str,
    projects_csv: str,
    remove_patterns: Optional[Iterable[str]] = None,
    out_dir: str = "Data_Output",
    outfile_meta: str = "veldbezoeken_export_aanpassingenMETA.csv",
) -> pd.DataFrame:
    ##Final summary each field visit
    # Load CSVs
    obs = pd.read_csv(observations_csv, sep=";", dtype=str)
    fv = pd.read_csv(fieldvisits_csv, sep=";", dtype=str)
    pr = pd.read_csv(projects_csv, sep=";", dtype=str)

    # Ensure datetime columns are parsed
    fv["Startdatum"] = pd.to_datetime(fv["Startdatum"], errors="coerce")
    fv["Einddatum"] = pd.to_datetime(fv["Einddatum"], errors="coerce")

    # Compte sunrise/sunset times
    obs_columns = obs.columns.tolist()
    fld_id_col_obs = "Veldbezoek ID...23" if "Veldbezoek ID...23" in obs_columns else (
        "Veldbezoek ID" if "Veldbezoek ID" in obs_columns else None
    )
    proj_id_col_obs = "Project ID...24" if "Project ID...24" in obs_columns else (
        "Project ID" if "Project ID" in obs_columns else None
    )
    if fld_id_col_obs is None or proj_id_col_obs is None:
        raise KeyError(
            "Could not determine field visit or project ID columns in observations CSV;"
            " please check the column names."
        )

    sun_df = compute_suntimes(
        observations=obs,
        fieldvisits=fv,
        projects=pr,
        fieldvisit_id_col_obs=fld_id_col_obs,
        project_id_col_obs=proj_id_col_obs,
        fieldvisit_id_col="ID",
        project_id_col="project_id",
        outfile=None
    )

    # Merge with field visits
    fv = fv.rename(columns={fv.columns[0]: "Veldbezoek_ID"})
    sun_df = sun_df.rename(columns={sun_df.columns[2]: "Veldbezoek_ID"})
    meta1 = pd.merge(fv, sun_df[["Veldbezoek_ID", "Stad", "Coördinaten", "sunrise", "sunset"]], on="Veldbezoek_ID", how="left")

    # Fill missing coordinates with first non‑NA value
    meta1["Coördinaten"] = meta1["Coördinaten"].str.strip().replace("", np.nan)
    meta1["coord_project"] = meta1.groupby("project_id")["Coördinaten"].transform(lambda x: x.dropna().iloc[0] if not x.dropna().empty else np.nan)
    meta1["Coördinaten"] = meta1["Coördinaten"].combine_first(meta1["coord_project"])
    meta1 = meta1.drop(columns=["coord_project"])

    # Apply simple removal pattern check directly on the merged data
    meta2 = meta1.copy()
    if remove_patterns:
        pattern = re.compile("|".join(remove_patterns), re.IGNORECASE)
        meta2["Rows_Removed"] = np.where(
            meta2["Naam"].fillna("").str.contains(pattern), "remove", "keep"
        )
    else:
        meta2["Rows_Removed"] = "keep"

    # Compute suggested start/end times
    meta3 = compute_time_suggest(meta2)
    # Flag records requiring manual checks
    meta4 = compute_flag_fieldtime_changes(meta3)

    # Build the summary DataFrame.
    adjustments = pd.DataFrame({
        "project_id": meta4.get("project_id"),
        "veldbezoek_ID": meta4.get("Veldbezoek_ID"),
        "check_data": meta4.get("check_data"),
        "verwijderd": meta4.get("Rows_Removed"),
        "project_naam": meta4.get("Project Naam"),
        "veldbezoeknaam_oud": meta4.get("Naam"),
        "veldbezoeknaam_nieuw": meta4.get("Naam"),
        "starttijd_oud": meta4.get("Startdatum"),
        "starttijd_nieuw": meta4.get("Starttijd_Suggest"),
        "eindtijd_oud": meta4.get("Einddatum"),
        "eindtijd_nieuw": meta4.get("Eindtijd_Suggest"),
        "duur_oud": meta4.get("Duur (uren)"),
        "duur_nieuw": meta4.get("duur_suggest"),
        "zonsopkomst": meta4.get("sunrise"),
        "zonsondergang": meta4.get("sunset"),
    })

    # Write summary CSV
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, outfile_meta)
    adjustments.to_csv(out_path, index=False, sep=";", na_rep="")
    return adjustments


if __name__ == "__main__":
    import argparse
    from pathlib import Path

    # Determine defaults relative to this file
    this_dir = Path(__file__).resolve().parent
    data_dir = (this_dir / ".." / "data").resolve()
    default_obs = str(data_dir / "waarnemingen_export_2025-09-23_17-17.csv")
    default_fv = str(data_dir / "veldbezoeken_export_2025-09-23-17-16.csv") if (data_dir / "veldbezoeken_export_2025-09-23-17-16.csv").exists() else str(data_dir / "veldbezoeken_export_2025-09-23_17-16.csv")
    default_pr = str(data_dir / "projecten_export_2025-09-23_17-15.csv")
    default_out = str((this_dir / ".." / "output" / "field_visits").resolve())

    parser = argparse.ArgumentParser(
        description="Process field visits and produce suggested adjustments."
    )
    parser.add_argument(
        "observations_csv", nargs="?", default=default_obs,
        help=f"Path to observations CSV; defaults to {default_obs}"
    )
    parser.add_argument(
        "fieldvisits_csv", nargs="?", default=default_fv,
        help=f"Path to field visits CSV; defaults to {default_fv}"
    )
    parser.add_argument(
        "projects_csv", nargs="?", default=default_pr,
        help=f"Path to projects CSV; defaults to {default_pr}"
    )
    parser.add_argument(
        "--out_dir", default=default_out,
        help=f"Directory to save outputs; defaults to {default_out}"
    )
    parser.add_argument(
        "--remove_patterns", nargs="*", default=["test", "ongeldig", "tim"],
        help="Name patterns to flag for removal"
    )
    args = parser.parse_args()

    transform_fieldvisits(
        observations_csv=args.observations_csv,
        fieldvisits_csv=args.fieldvisits_csv,
        projects_csv=args.projects_csv,
        remove_patterns=args.remove_patterns,
        out_dir=args.out_dir
    )
