# ---- packages ----
library(dplyr)
library(stringr)
library(sf)
library(leaflet)
library(htmlwidgets)
library(fs)
library(hms)      # only used if you have a 'Tijd' hms column

# ---- config: choose a short, writable folder on Windows ----
out_root <- "C:/tmp/waarn_out"
dir_create(out_root)

# ---- helpers ----
safe_name <- function(x) {
  x |>
    str_trim() |>
    str_replace_all("[^[:alnum:]]+", "_") |>
    str_replace_all("_+", "_") |>
    str_to_lower() |>
    substring(1, 40)  # keep filenames short
}

# save leaflet robustly (selfcontained first; fallback to libdir)
save_leaflet_safe <- function(widget, out_dir, base) {
  dir_create(out_dir)
  html_path <- file.path(out_dir, paste0(base, ".html"))
  
  ok <- try({
    saveWidget(widget, html_path, selfcontained = TRUE)
  }, silent = TRUE)
  
  if (inherits(ok, "try-error")) {
    libdir <- file.path(out_dir, "libs")
    dir_create(libdir)
    saveWidget(widget, html_path, selfcontained = FALSE, libdir = libdir)
  }
  invisible(html_path)
}

# write per-project vector data (SHP + GPKG) with SHP-safe fields
write_per_project <- function(sf_pts_rd, project, out_root) {
  base    <- paste0("waarn_", safe_name(project))
  out_dir <- fs::path(out_root, base)
  fs::dir_create(out_dir)
  
  # Make a SHP-friendly copy: stringify POSIXt / hms / difftime
  shp_ready <- sf_pts_rd %>%
    dplyr::mutate(
      dplyr::across(dplyr::where(~ inherits(.x, "POSIXt")),
                    ~ format(as.POSIXct(.x), "%Y-%m-%d %H:%M:%S")),
      dplyr::across(dplyr::where(~ inherits(.x, "hms")),
                    ~ format(.x)),
      dplyr::across(dplyr::where(~ inherits(.x, "difftime")),
                    ~ as.character(.x))
    )
  
  # Full-fidelity GeoPackage
  sf::st_write(sf_pts_rd, fs::path(out_dir, paste0(base, ".gpkg")),
               delete_layer = TRUE, quiet = TRUE)
  
  # Legacy Shapefile
  sf::st_write(shp_ready, fs::path(out_dir, paste0(base, ".shp")),
               delete_layer = TRUE, quiet = TRUE,
               layer_options = "ENCODING=UTF-8")
  
  invisible(out_dir)
}



# ---- build sf from your table (expects Breedtegraad/Lengtegraad + Projectnaam) ----
# Keep WGS84 for web maps
pts_wgs <- waarnemingen_export3 %>%
  filter(!is.na(Projectnaam), !is.na(Breedtegraad), !is.na(Lengtegraad)) %>%
  mutate(
    Breedtegraad = as.numeric(Breedtegraad),
    Lengtegraad  = as.numeric(Lengtegraad)
  ) %>%
  st_as_sf(coords = c("Lengtegraad", "Breedtegraad"), crs = 4326, remove = FALSE)

# ---- per project: write files + leaflet map with OSM base ----
pts_wgs %>%
  group_by(Projectnaam) %>%
  group_walk(~ {
    proj  <- .y$Projectnaam[[1]]
    x_wgs <- .x
    x_rd  <- st_transform(x_wgs, 28992)   # RD New for SHP/GPKG
    
    # 1) Write vector files (SHP + GPKG)
    proj_dir <- write_per_project(sf_pts_rd = x_rd, project = proj, out_root = out_root)
    
    # 2) Leaflet map with OSM tiles
    pop <- paste0(
      "<b>Soort:</b> ", x_wgs$Soort, "<br>",
      "<b>Adres:</b> ", x_wgs$Adres, "<br>",
      "<b>Plaats:</b> ", x_wgs$Plaats, "<br>",
      "<b>Datum:</b> ", x_wgs$Datum
    )
    
    m <- leaflet(x_wgs) |>
      addTiles() |>
      addCircleMarkers(
        lng = ~Lengtegraad, lat = ~Breedtegraad,
        radius = 5, stroke = TRUE, weight = 1, fillOpacity = 0.8,
        popup = pop
      )
    
    bb <- st_bbox(x_wgs)
    if (is.finite(bb$xmin) && bb$xmin != bb$xmax && bb$ymin != bb$ymax) {
      m <- m |> fitBounds(bb$xmin, bb$ymin, bb$xmax, bb$ymax)
    } else {
      m <- m |> setView(lng = mean(c(bb$xmin, bb$xmax)), lat = mean(c(bb$ymin, bb$ymax)), zoom = 16)
    }
    
    base <- paste0("map_", safe_name(proj))
    save_leaflet_safe(m, proj_dir, base)
  })
















############## Fix empty coordinates #############333

library(dplyr)
library(sf)
library(stringr)

# robust numeric parser: handles "51,5665" and "51.5665"
to_num <- function(x) {
  if (is.numeric(x)) return(x)
  suppressWarnings(as.numeric(gsub(",", ".", as.character(x))))
}

df0 <- waarnemingen_export3 %>%
  mutate(
    lat = to_num(Breedtegraad),
    lon = to_num(Lengtegraad)
  )

n <- nrow(df0)
wgs_like      <- sum(dplyr::between(df0$lat, -90, 90)   & dplyr::between(df0$lon, -180, 180), na.rm = TRUE)
wgs_swapped   <- sum(dplyr::between(df0$lon, -90, 90)   & dplyr::between(df0$lat, -180, 180), na.rm = TRUE)
rd_like       <- sum(dplyr::between(df0$lon,    0, 300000) & dplyr::between(df0$lat, 300000, 620000), na.rm = TRUE)

cat("Rows:", n,
    "\nWGS-like:", wgs_like,
    "\nWGS-swapped-like:", wgs_swapped,
    "\nRD-like:", rd_like, "\n")
print(summary(df0[,c("lat","lon")]))

# choose a path based on what's most common
if (wgs_like >= 0.6 * n) {
  message("Interpreting as WGS84 (lon = Lengtegraad, lat = Breedtegraad).")
  pts_wgs <- df0 %>%
    filter(is.finite(lat), is.finite(lon),
           dplyr::between(lat, -90, 90), dplyr::between(lon, -180, 180)) %>%
    st_as_sf(coords = c("lon", "lat"), crs = 4326, remove = FALSE)
  pts_rd <- st_transform(pts_wgs, 28992)
  
} else if (wgs_swapped >= 0.6 * n) {
  message("Interpreting as WGS84 but columns swapped (lon=Breedtegraad, lat=Lengtegraad).")
  pts_wgs <- df0 %>%
    filter(is.finite(lat), is.finite(lon),
           dplyr::between(lon, -90, 90), dplyr::between(lat, -180, 180)) %>%
    st_as_sf(coords = c("lat", "lon"), crs = 4326, remove = FALSE)
  pts_rd <- st_transform(pts_wgs, 28992)
  
} else if (rd_like >= 0.6 * n) {
  message("Interpreting as RD New already (X = Lengtegraad, Y = Breedtegraad).")
  pts_rd <- df0 %>%
    filter(is.finite(lat), is.finite(lon),
           dplyr::between(lon, 0, 300000), dplyr::between(lat, 300000, 620000)) %>%
    st_as_sf(coords = c("lon", "lat"), crs = 28992, remove = FALSE)
  pts_wgs <- st_transform(pts_rd, 4326)
  
} else {
  stop("Could not confidently detect CRS from the value ranges.\n",
       "Show a few raw values:\n Breedtegraad: ", paste(head(na.omit(waarnemingen_export3$Breedtegraad),3), collapse=", "),
       "\n Lengtegraad: ", paste(head(na.omit(waarnemingen_export3$Lengtegraad),3), collapse=", "),
       "\nCheck if they use commas, or if they’re RD (meters) vs WGS84 (degrees).")
}

# sanity check: bbox should be finite and non-zero
print(st_bbox(pts_wgs))
print(st_crs(pts_rd))





################################### Last gives error, try this##################3


# --- packages ---
library(dplyr)
library(stringr)
library(sf)
library(leaflet)
library(htmlwidgets)
library(fs)
library(hms)

# Quiet jsonlite deprecation spam from htmlwidgets
options(htmlwidgets.TOJSON_ARGS = list(keep_vec_names = FALSE))

# --- output root (short path) ---
out_root <- "C:/tmp/waarn_out1"
fs::dir_create(out_root)

# --- helpers ---
safe_name <- function(x) {
  x |>
    str_trim() |>
    str_replace_all("[^[:alnum:]]+", "_") |>
    str_replace_all("_+", "_") |>
    str_to_lower() |>
    substr(1, 40)
}

save_leaflet_safe <- function(widget, out_dir, base) {
  fs::dir_create(out_dir)
  html_path <- file.path(out_dir, paste0(base, ".html"))
  ok <- try(saveWidget(widget, html_path, selfcontained = TRUE), silent = TRUE)
  if (inherits(ok, "try-error")) {
    libdir <- file.path(out_dir, "libs")
    fs::dir_create(libdir)
    saveWidget(widget, html_path, selfcontained = FALSE, libdir = libdir)
  }
  invisible(html_path)
}

# Convert POSIXt/hms/difftime columns to SHP-friendly strings
make_shp_friendly <- function(x){
  x %>%
    mutate(
      across(where(~ inherits(.x, "POSIXt")),  ~ format(as.POSIXct(.x), "%Y-%m-%d %H:%M:%S")),
      across(where(~ inherits(.x, "hms")),      ~ format(.x)),
      across(where(~ inherits(.x, "difftime")), ~ as.character(.x))
    )
}

# Write per project: GPKG + SHP
write_per_project <- function(sf_pts_rd, project, out_root) {
  base    <- paste0("waarn_", safe_name(project))
  out_dir <- fs::path(out_root, base)
  fs::dir_create(out_dir)
  
  # Full-fidelity GeoPackage
  sf::st_write(sf_pts_rd, fs::path(out_dir, paste0(base, ".gpkg")),
               delete_layer = TRUE, quiet = TRUE)
  
  # Legacy Shapefile (stringify time-like fields)
  shp_ready <- make_shp_friendly(sf_pts_rd)
  sf::st_write(shp_ready, fs::path(out_dir, paste0(base, ".shp")),
               delete_layer = TRUE, quiet = TRUE, layer_options = "ENCODING=UTF-8")
  
  invisible(out_dir)
}

# --- FIX THE COORDINATES (key part) ---
# If value already numeric in range use it; else insert decimal after 2 (lat) or 1 (lon) digits
norm_num <- function(x) {
  if (is.numeric(x)) return(x)
  suppressWarnings(as.numeric(gsub(",", ".", as.character(x))))
}

fix_coord <- function(x, after_digits) {
  if (is.na(x)) return(NA_real_)
  s <- gsub("\\D", "", as.character(x))     # keep digits only
  if (s == "") return(NA_real_)
  d <- nchar(s)
  as.numeric(s) / (10^(d - after_digits))
}

smart_lat <- function(x) {
  y <- norm_num(x)
  if (is.finite(y) && abs(y) <= 90) return(y)  # already fine
  fix_coord(x, 2)                              # put decimal after two digits -> 51.x
}

smart_lon <- function(x) {
  y <- norm_num(x)
  if (is.finite(y) && abs(y) <= 180) return(y) # already fine
  fix_coord(x, 1)                              # put decimal after one digit -> 4.x / 5.x / 6.x
}

# Add clean lat/lon columns
waarnemingen_export3 <- waarnemingen_export3 %>%
  mutate(
    lat = vapply(Breedtegraad, smart_lat, numeric(1)),
    lon = vapply(Lengtegraad,  smart_lon, numeric(1))
  )

# (Optional) quick sanity check:
# summary(waarnemingen_export3[,c("lat","lon")])

# --- Build sf (WGS84) then transform to RD New for export ---
pts_wgs <- waarnemingen_export3 %>%
  filter(!is.na(Projectnaam), is.finite(lat), is.finite(lon),
         between(lat, 48, 54), between(lon, 2, 8)) %>%   # NL-ish bounds
  st_as_sf(coords = c("lon", "lat"), crs = 4326, remove = FALSE)

pts_wgs %>%
  group_by(Projectnaam) %>%
  group_walk(~ {
    proj  <- .y$Projectnaam[[1]]
    x_wgs <- .x
    x_rd  <- st_transform(x_wgs, 28992)   # RD New for GIS workflows
    
    # 1) Vector outputs
    proj_dir <- write_per_project(sf_pts_rd = x_rd, project = proj, out_root = out_root)
    
    # 2) Leaflet map with OSM base (from WGS84)
    pop <- paste0(
      "<b>Soort:</b> ", x_wgs$Soort, "<br>",
      "<b>Adres:</b> ", x_wgs$Adres, "<br>",
      "<b>Plaats:</b> ", x_wgs$Plaats, "<br>",
      "<b>Datum:</b> ", x_wgs$`Gezien op`
    )
    
    m <- leaflet(x_wgs) |>
      addTiles() |>
      addCircleMarkers(lng = ~lon, lat = ~lat,
                       radius = 5, stroke = TRUE, weight = 1, fillOpacity = 0.8,
                       popup = pop)
    
    bb <- st_bbox(x_wgs)
    if (is.finite(bb$xmin) && bb$xmin != bb$xmax && bb$ymin != bb$ymax) {
      m <- m |> fitBounds(bb$xmin, bb$ymin, bb$xmax, bb$ymax)
    } else {
      m <- m |> setView(lng = mean(c(bb$xmin, bb$xmax)),
                        lat = mean(c(bb$ymin, bb$ymax)), zoom = 15)
    }
    
    base <- paste0("map_", safe_name(proj))
    save_leaflet_safe(m, proj_dir, base)
  })

