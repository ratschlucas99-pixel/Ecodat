library(dplyr)
library(stringr)
library(tidyr)

# Helper to normalize dagdeel numbers (handles 03 -> 3 and I/II/III)
.normalize_dagdeel_num <- function(x) {
  ifelse(
    is.na(x), NA_character_,
    ifelse(str_detect(x, "^(?i)i{1,3}$"),
           recode(str_to_lower(x), "i"="1","ii"="2","iii"="3"),
           as.character(suppressWarnings(as.integer(x))) # drops leading zeros
    )
  )
}

naam_veldbezoeken_schoon <- function(df, remove_patterns = c("test")) {
  # Build case-insensitive pattern for removal flag
  pat <- if (length(remove_patterns)) str_c(remove_patterns, collapse = "|") else NULL
  
  df %>%
    mutate(
      # ---- Extract project code (VM with optional dash/space + digits), or GZ/ZR/HM/Uitvliegtelling
      Project = str_extract(Naam, "(?i)([VW]M[- ]?\\d+|GZ|ZR|HM|Uitvliegtelling)"),
      # ---- Extract dagdeel (avond/ochtend + optional number or roman I/II/III)
      Dagdeel_raw = str_extract(Naam, "(?i)(avond|ochtend)[[:space:]]*([0-9]+|I{1,3})?")
    ) %>%
    mutate(
      # --- Normalize Project ---
      Project = str_to_upper(Project),
      Project = str_replace_all(Project, "[- ]", ""),   # remove dashes/spaces
      Project = str_replace(Project, "^WM", "VM"),      # fix wm -> VM
      Project = str_replace(Project, "^VM(\\d)$", "VM0\\1"),  # pad single digit
      # keep VM10, VM12, etc. untouched
      
      # --- Split dagdeel into type + number, then standardize ---
      Dagdeel_type = str_to_lower(str_extract(Dagdeel_raw, "(?i)avond|ochtend")),
      Dagdeel_num_raw = str_extract(Dagdeel_raw, "(?i)\\d+|I{1,3}"),
      Dagdeel_num = .normalize_dagdeel_num(Dagdeel_num_raw),
      
      # Recompose a clean Dagdeel label (e.g., "avond 1", "ochtend 3", or just "avond")
      Dagdeel = ifelse(
        is.na(Dagdeel_type), NA_character_,
        ifelse(is.na(Dagdeel_num), Dagdeel_type, paste(Dagdeel_type, Dagdeel_num))
      ),
      
      Naam_schoon = ifelse(
        is.na(Project), NA_character_,
        ifelse(is.na(Dagdeel), Project, paste(Project, Dagdeel))
      ),
      
      # ---- Flag rows to remove based on name patterns (case-insensitive) ----
      Rows_Removed = if (is.null(pat)) "keep" else if_else(
        replace_na(str_detect(Naam, regex(pat, ignore_case = TRUE)), FALSE),
        "remove", "keep"
      )
    ) %>%
    select(-Dagdeel_raw, -Dagdeel_num_raw, -Dagdeel_num)
}
