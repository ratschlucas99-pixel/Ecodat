from __future__ import annotations
import os
import json
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple
import pandas as pd
import numpy as np
from geocode_utils import reverse_geocode as geocode_reverse_geocode, parse_address as geocode_parse_address
from tqdm import tqdm


def safe_name(s: str) -> str:
    ##Return a file‑name safe version of ``s``.
    if not isinstance(s, str) or not s:
        return "unknown"
    s = s.strip()
    s = re.sub(r"[^A-Za-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.lower()[:40]


def assign_groups(df: pd.DataFrame, species_col: str = "Soort") -> pd.DataFrame:
    ##Assign a high‑level group to each observation based on the species
    df = df.copy()
    species_lower = df[species_col].fillna("").str.lower()
    conditions = [
        species_lower.str.contains(r"vleermuis|vlieger", regex=True),
        species_lower.str.contains(r"\bmuis\b|vos|pad|salamander", regex=True),
        species_lower == ""
    ]
    choices = ["Vleermuizen", "Overig", "onbekend"]
    df["Groep"] = np.select(conditions, choices, default="Vogels")
    return df


def fill_missing_counts(df: pd.DataFrame, count_col: str = "Aantal") -> pd.DataFrame:
    #Fill missing observation counts with na
    df = df.copy()
    df[count_col] = df[count_col].fillna(1)
    return df


def reverse_geocode(*args, **kwargs):  # type: ignore[override]

    return geocode_reverse_geocode(*args, **kwargs)  # type: ignore


def assign_functions(df: pd.DataFrame,
                     group_col: str = "Groep",
                     behaviour_col: str = "Gedrag",
                     count_col: str = "Aantal",
                     function_col: str = "Functie") -> pd.DataFrame:
    ##Functional categoies to each observation
    df = df.copy()

    # Define behaviour categories
    locatie_verblijf_vleer_VM01 = {
        "Invliegend (algemeen)", "uitvliegend (algemeen)",
        "territoriumindicerend", "ter plaatse", "bezoek aan nestplaats"
    }
    locatie_verblijf_vleer_VM023 = {
        "baltsend", "zwermend (algemeen)", "baltsend/zingend",
        "parend / copula"
    }
    vliegroute = {
        "overvliegend", "passerend (niet nader omschreven)",
        "overvliegend naar noord", "overvliegend naar zuid",
        "overvliegend naar oost", "overvliegend naar west"
    }
    locatie_verblijf_vogels = {
        "Invliegend (algemeen)", "uitvliegend (algemeen)",
        "territoriumindicerend", "ter plaatse",
        "bezoek aan nestplaats", "parend / copula",
        "baltsend/zingend", "baltsend", "slaapplaats",
        "nest-indicerend gedrag", "roepend", "nestbouw", "rustend"
    }
    foerageergebied = {"foeragerend"}

    counts = pd.to_numeric(df[count_col], errors="coerce")
    fun = df.get(function_col, pd.Series([None] * len(df), index=df.index))
    vleermuizen_mask = df[group_col] == "Vleermuizen"
    vogels_mask = df[group_col] == "Vogels"

    # Summer and breeding sites for bats (VM01)
    mask = (
        vleermuizen_mask &
        df[behaviour_col].isin(locatie_verblijf_vleer_VM01) &
        counts.notna() & (counts < 10)
    )
    fun.loc[mask] = "zomerverblijfplaats"

    # Maternity roosts for bats
    mask = (
        vleermuizen_mask &
        df[behaviour_col].isin(locatie_verblijf_vleer_VM01) &
        counts.notna() & (counts > 9)
    )
    fun.loc[mask] = "kraamverblijfplaats"

    # Courtship roosts for bats
    mask = (
        vleermuizen_mask &
        df[behaviour_col].isin(locatie_verblijf_vleer_VM023) &
        counts.notna()
    )
    fun.loc[mask] = "paarverblijfplaats"

    # Nesting sites for birds
    mask = (
        vogels_mask &
        df[behaviour_col].isin(locatie_verblijf_vogels) &
        counts.notna()
    )
    fun.loc[mask] = "nestlocatie"

    # Flight paths
    mask = df[behaviour_col].isin(vliegroute) & counts.notna()
    fun.loc[mask] = "vliegroute"

    # Foraging areas
    mask = df[behaviour_col].isin(foerageergebied) & counts.notna()
    fun.loc[mask] = "foerageergebied"

    df[function_col] = fun
    return df


def parse_address(*args, **kwargs):  # type: ignore[override]

    return geocode_parse_address(*args, **kwargs)  # type: ignore


def transform_data(raw_df: pd.DataFrame,
                   behaviours_for_geocoding: Optional[Iterable[str]] = None,
                   geocode_cache: Optional[str] = None) -> pd.DataFrame:
    ##Transform the raw observations into a cleaned table
    df = raw_df.copy()

    if "Gezien op" in df.columns:
        df["Datum"] = pd.to_datetime(df["Gezien op"], errors="coerce").dt.date
        df["Tijd"] = pd.to_datetime(df["Gezien op"], errors="coerce").dt.time
    else:
        df["Datum"] = pd.NaT
        df["Tijd"] = pd.NaT

    df["Verblijfnummer"] = np.nan
    df["Functie"] = None
    df["Adres"] = None
    df["Plaats"] = None

    if "Opmerking" in df.columns:
        df["Locatie_adres"] = df["Opmerking"]
    else:
        df["Locatie_adres"] = None

    df = assign_groups(df)

    df = fill_missing_counts(df)

    # Reverse geocode
    if behaviours_for_geocoding is not None:
        df = reverse_geocode(df,
                             lat_col="Breedtegraad",
                             lon_col="Lengtegraad",
                             behaviours=behaviours_for_geocoding,
                             behaviour_col="Gedrag",
                             cache_file=geocode_cache)
    else:
        df["address"] = None

    df = assign_functions(df)

    # Assign Verblijfnummer (unique ID per address)
    codes, uniques = pd.factorize(df["address"], sort=True)
    df["Verblijfnummer"] = codes.astype(float) + 1
    df.loc[df["address"].isna(), "Verblijfnummer"] = np.nan

    parsed = df["address"].apply(lambda s: parse_address(s))
    df["Adres"] = parsed.apply(lambda t: t[0])
    df["Plaats"] = parsed.apply(lambda t: t[1])

    return df


def clean_data(df: pd.DataFrame,
               out_dir: str = "Data_Output",
               project_col: str = "Projectnaam") -> None:
    ##Write a cleaned summary CSV per project
    os.makedirs(out_dir, exist_ok=True)

    cols = [
        "Verblijfnummer", "Groep", "Soort", "Datum", "Tijd",
        "Aantal", "Gedrag", "Verblijfplaats", "Sekse",
        "Adres", "Plaats", "Locatie_adres", "Functie", project_col
    ]
    existing_cols = [c for c in cols if c in df.columns]

    # Group by project and write each to CSV
    for project, group in df.dropna(subset=[project_col]).groupby(project_col):
        fname = f"waarnemingen_export_{safe_name(str(project))}.csv"
        out_path = os.path.join(out_dir, fname)
        group[existing_cols].to_csv(out_path, sep=";", index=False, na_rep="")


if __name__ == "__main__":
    import argparse
    import sys
    from pathlib import Path

    this_dir = Path(__file__).resolve().parent
    default_data_dir = (this_dir / ".." / "data").resolve()
    default_csv_path = str(default_data_dir / "waarnemingen_export_2025-09-23_17-17.csv")
    default_project_csv = str(default_data_dir / "veldbezoeken_export_2025-09-23_17-16.csv")
    default_out_dir = str((this_dir / ".." / "output" / "observations").resolve())

    parser = argparse.ArgumentParser(
        description="Process observations CSV into per‑project cleaned outputs."
    )
    # csv_path
    parser.add_argument(
        "csv_path", nargs="?", default=default_csv_path,
        help=f"Path to the raw observations CSV; defaults to {default_csv_path}"
    )
    parser.add_argument(
        "--project_csv", default=default_project_csv,
        help=f"Path to veldbezoeken export CSV for project names; defaults to {default_project_csv}"
    )
    parser.add_argument(
        "--out_dir", default=default_out_dir,
        help=f"Output directory for cleaned CSVs; defaults to {default_out_dir}"
    )
    parser.add_argument(
        "--geocode", action="store_true",
        help="Perform reverse geocoding (may be slow)"
    )
    parser.add_argument(
        "--project_col", default="Projectnaam",
        help="Column to group by when writing per‑project CSVs (e.g. 'Projectnaam' or 'project_id')."
    )

    args = parser.parse_args()

    # Load raw observations
    raw = pd.read_csv(args.csv_path, sep=";", parse_dates=["Gezien op"], dayfirst=True)

    # Optionally merge in project names if provided
    if args.project_csv:
        try:
            veld = pd.read_csv(args.project_csv, sep=";", parse_dates=["Aangemaakt op"], dayfirst=True)

            if "Project ID" in raw.columns and "project_id" in veld.columns and "Project Naam" in veld.columns:
                id_map = veld.set_index("project_id")["Project Naam"].to_dict()
                raw["Projectnaam"] = raw["Project ID"].map(id_map)
        except FileNotFoundError:
            pass

    locatie_verblijf = [
        "Invliegend (algemeen)", "baltsend", "zwermend (algemeen)",
        "uitvliegend (algemeen)", "nest-indicerend gedrag",
        "territoriumindicerend", "ter plaatse", "slaapplaats",
        "bezoek aan nestplaats", "baltsend/zingend", "rustend",
        "nestbouw", "parend / copula"
    ]
    behaviours_for_geocoding = locatie_verblijf if args.geocode else None

    # Transform data
    transformed = transform_data(
        raw,
        behaviours_for_geocoding=behaviours_for_geocoding,
        geocode_cache="geocode_cache.json" if args.geocode else None
    )

    # Write per‑project cleaned data using the specified grouping column
    clean_data(transformed, out_dir=args.out_dir, project_col=args.project_col)