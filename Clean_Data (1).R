


veldbezoeken_export_schoon1 <- veldbezoeken_export_meta4
veldbezoeken_export_schoon2 <- veldbezoeken_export_schoon1[veldbezoeken_export_schoon1$Rows_Removed == "keep",]
veldbezoeken_export_schoon <- veldbezoeken_export_schoon2[,names(veldbezoeken_export)]

extra_in_schoon2 <- setdiff(names(veldbezoeken_export_schoon2),
                            names(veldbezoeken_export))

# extra_in_schoon2

veldbezoeken_export_schoon$Naam <- veldbezoeken_export_schoon2$Naam_schoon
veldbezoeken_export_schoon$Startdatum <- veldbezoeken_export_schoon2$Starttijd_Suggest
veldbezoeken_export_schoon$Einddatum <- veldbezoeken_export_schoon2$Eindtijd_Suggest
veldbezoeken_export_schoon$`Duur (uren)` <- veldbezoeken_export_schoon2$duur_suggest

# are the column names identical
identical(names(veldbezoeken_export), names(veldbezoeken_export_schoon))


View(veldbezoeken_export_schoon)


##### Write CSVs

# choose/normalize the project-name column
df <- veldbezoeken_export_schoon
if (!"project_name" %in% names(df) && "Project Naam" %in% names(df)) {
  df <- dplyr::rename(df, project_name = `Project Naam`)
}

# folder to write to
out_dir <- "Data_Output"
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)


# helper to make safe filenames
safe_name <- function(x) {
  x %>%
    str_trim() %>%
    str_replace_all("[^[:alnum:]]+", "_") %>%  # non-letters/digits -> _
    str_replace_all("_+", "_") %>%
    str_to_lower()
}

# write one CSV per project
df %>%
  filter(!is.na(project_name)) %>%
  group_by(project_name) %>%
  group_walk(~ {
    fname <- paste0("veldbezoeken_export_", safe_name(.y$project_name), ".csv")
    # Dutch Excel-friendly; use write.csv() if you prefer commas
    write.csv2(.x, file = file.path(out_dir, fname), row.names = FALSE, na = "")
  })

