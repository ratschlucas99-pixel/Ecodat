from __future__ import annotations
import os
import re
from typing import Optional
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import folium
from observations_processing import safe_name


def create_gis_outputs(csv_path: str,
                       out_root: str = "gis_output",
                       lat_col: str = "Breedtegraad",
                       lon_col: str = "Lengtegraad",
                       project_col: str = "Projectnaam",
                       coord_col: str = "Coördinaten",
                       popup_cols: Optional[list[str]] = None) -> None:
    ##Generate per‑project shapefiles, GeoPackages and maps
    import pathlib
    path = pathlib.Path(csv_path)

    if path.is_dir():
        for file_path in sorted(path.rglob("*.csv")):
            create_gis_outputs(
                csv_path=str(file_path),
                out_root=out_root,
                lat_col=lat_col,
                lon_col=lon_col,
                project_col=project_col,
                coord_col=coord_col,
                popup_cols=popup_cols,
            )
        return

    # Load the data
    df = pd.read_csv(csv_path, sep=";", dtype=str)
    if project_col not in df.columns:
        candidates = [
            c for c in df.columns
            if re.search(r"project", c, flags=re.IGNORECASE)
            and (re.search(r"naam", c, flags=re.IGNORECASE) or re.search(r"name", c, flags=re.IGNORECASE))
        ]
        if not candidates:
            candidates = [c for c in df.columns if re.search(r"project", c, flags=re.IGNORECASE)]
        if candidates:
            project_col = candidates[0]
        else:
            project_col = "_no_project_"
            df[project_col] = "project"

    # Detect latitude/longitude columns if the specified ones are missing.
    for var_name, default_candidates in [
        ("lat_col", [lat_col, "Breedtegraad", "Lat", "Latitude", "latitude", "lat"]),
        ("lon_col", [lon_col, "Lengtegraad", "Lon", "Longitude", "longitude", "lon"])
    ]:
        name = locals()[var_name]
        if name not in df.columns:
            found = None
            for cand in default_candidates:
                if cand in df.columns:
                    found = cand
                    break
            if found:
                locals()[var_name] = found
            else:
                # If lat/lon columns are not found but a coordinate column exists, split it
                if coord_col in df.columns and var_name in ["lat_col", "lon_col"]:
                    locals()[var_name] = None
                else:
                    raise KeyError(
                        f"Could not find a valid coordinate column for {var_name} in {csv_path}. "
                        f"Looked for {default_candidates} and '{coord_col}'. Specify --{var_name} explicitly."
                    )
    # Parse 'Coördinaten' column if lat/lon not set
    if (lat_col is None or lon_col is None) and coord_col in df.columns:
        coords = df[coord_col].str.split("[ ,]+", n=2, expand=True)
        if lat_col is None:
            lat_col = "lat"
            df[lat_col] = coords[0]
        if lon_col is None:
            lon_col = "lon"
            df[lon_col] = coords[1]

    # Ensure numeric coordinate columns
    df[lat_col] = pd.to_numeric(df[lat_col], errors="coerce")
    df[lon_col] = pd.to_numeric(df[lon_col], errors="coerce")
    df = df.dropna(subset=[project_col, lat_col, lon_col])

    # Create output directory
    os.makedirs(out_root, exist_ok=True)

    # Iterate per project
    for project, group in df.groupby(project_col):
        proj_name = safe_name(str(project))
        proj_dir = os.path.join(out_root, f"waarn_{proj_name}")
        os.makedirs(proj_dir, exist_ok=True)


        gdf_wgs = gpd.GeoDataFrame(
            group,
            geometry=[Point(xy) for xy in zip(group[lon_col], group[lat_col])],
            crs="EPSG:4326"
        )

        gdf_rd = gdf_wgs.to_crs("EPSG:28992")

        # Prepare shapefile safe copy: convert datetime/timedelta
        shp_ready = gdf_rd.copy()
        for col in shp_ready.columns:
            if pd.api.types.is_datetime64_any_dtype(shp_ready[col]):
                shp_ready[col] = shp_ready[col].dt.strftime("%Y-%m-%d %H:%M:%S")
            elif pd.api.types.is_timedelta64_dtype(shp_ready[col]):
                shp_ready[col] = shp_ready[col].astype(str)

        #GeoPackage
        gpkg_path = os.path.join(proj_dir, f"waarn_{proj_name}.gpkg")
        gdf_rd.to_file(gpkg_path, layer=proj_name, driver="GPKG")

        #Shapefile
        shp_path = os.path.join(proj_dir, f"waarn_{proj_name}.shp")
        shp_ready.to_file(shp_path, driver="ESRI Shapefile")

        #Interactive map using folium
        if popup_cols is None:
            popup_cols_to_use = [
                c for c in ["Soort", "Adres", "Plaats", "Datum"] if c in group.columns
            ]
        else:
            popup_cols_to_use = popup_cols

        #Folium map centred on the mean coordinate
        mean_lat = group[lat_col].mean()
        mean_lon = group[lon_col].mean()
        m = folium.Map(location=[mean_lat, mean_lon], zoom_start=13)

        for _, row in group.iterrows():
            popup_html = "<br>".join([
                f"<b>{col}:</b> {row[col]}" for col in popup_cols_to_use if pd.notna(row[col])
            ])
            folium.CircleMarker(
                location=(row[lat_col], row[lon_col]),
                radius=5,
                weight=1,
                fill=True,
                fill_opacity=0.8,
                popup=popup_html
            ).add_to(m)

        if len(group) > 1:
            bounds = [[group[lat_col].min(), group[lon_col].min()],
                      [group[lat_col].max(), group[lon_col].max()]]
            m.fit_bounds(bounds)

        # Save as HTML
        html_path = os.path.join(proj_dir, f"map_{proj_name}.html")
        m.save(html_path)


if __name__ == "__main__":
    import argparse
    from pathlib import Path

    this_dir = Path(__file__).resolve().parent
    data_dir = (this_dir / ".." / "data").resolve()

    default_csv = str(data_dir / "waarnemingen_export_2025-09-23_17-17.csv")
    default_out = str((this_dir / ".." / "output" / "gis_output").resolve())

    parser = argparse.ArgumentParser(
        description="Create GIS outputs per project from a observations CSV."
    )
    parser.add_argument(
        "csv_path", nargs="?", default=default_csv,
        help=f"Path to cleaned observations CSV with lat/lon and project name; defaults to {default_csv}"
    )
    parser.add_argument(
        "--out_root", default=default_out,
        help=f"Directory to write per‑project outputs; defaults to {default_out}"
    )
    args = parser.parse_args()
    create_gis_outputs(args.csv_path, out_root=args.out_root)
