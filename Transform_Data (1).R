setwd("C:/Users/Gebruiker/Ed Michels/Bedrijf - 01- Projecten/ZijnR_DataProjecten_2025/Velddata_opschonen/VeldApp_R")



############## create dataframe with sunset and sunrise times per field visit #############

sun_times_fieldvisit <- get_fieldvisit_suntimes(waarnemingen_export,
                                                veldbezoeken_export,
                                                projecten_export,
                                                outfile = "sun_times_fieldvisit.csv"
                                                )


################## Merge sun_times with fieldvisits  ##################
colnames(sun_times_fieldvisit)[2] <- "Project_naam"
colnames(veldbezoeken_export)[1] <- "Veldbezoek_ID"
veldbezoeken_export_meta1 <- full_join(veldbezoeken_export, 
                                      sun_times_fieldvisit[,c("Veldbezoek_ID", 
                                                              "Stad", 
                                                              "Coördinaten", 
                                                              "sunrise", 
                                                              "sunset")], 
                                      by = "Veldbezoek_ID")

veldbezoeken_export_meta1[is.na(veldbezoeken_export_meta1$Coördinaten),]


veldbezoeken_export_meta1 <- veldbezoeken_export_meta1 %>%
                                mutate(Coördinaten = na_if(str_trim(Coördinaten), "")) %>%   # treat "" as NA
                                group_by(project_id ) %>%
                                mutate(
                                  coord_project = first(Coördinaten[!is.na(Coördinaten)])    # fallback per project
                                ) %>%
                                ungroup() %>%
                                mutate(
                                  Coördinaten = coalesce(Coördinaten, coord_project)         # fill NA with fallback
                                ) %>%
                                select(-coord_project)

################# Get clean names for field visits #####################
veldbezoeken_export_meta2 <- naam_veldbezoeken_schoon(veldbezoeken_export_meta1,
                                                      remove_patterns = c("test",
                                                                          "ongeldig",
                                                                          "tim"))
# View(veldbezoeken_export_meta2[,c("Naam", "Naam_schoon", "Project",
#                                  "Dagdeel", "Rows_Removed")])



################## get suggested start times and end times against sunset and sunrise #####


veldbezoeken_export_meta3 <- get_fieldvisit_timesuggest(veldbezoeken_export_meta2)
# names(veldbezoeken_export_meta3)
# View(veldbezoeken_export_meta3[,c("Naam", "Startdatum", "sunset", 
#                                  "Starttijd_Suggest", "Einddatum" ,"sunrise", 
#                                  "Eindtijd_Suggest", "duur_suggest")])

veldbezoeken_export_meta4 <- flag_Fieldtime_changes(veldbezoeken_export_meta3)

# View(veldbezoeken_export_meta4[,c("Naam","Startdatum","Starttijd_Suggest", 
#                                  "Einddatum", "Eindtijd_Suggest", 
#                                  "duur_suggest", "check_data")]
#     )

veldbezoeken_export_aanpassingen <-  veldbezoeken_export_meta4 %>%
                                        select(project_id = project_id,
                                               veldbezoek_ID = Veldbezoek_ID,
                                               check_data =  check_data,
                                               verwijderd = Rows_Removed,
                                               project_naam = `Project Naam`,
                                               veldbezoeknaam_oud = Naam,
                                               veldbezoeknaam_nieuw = Naam_schoon,
                                               starttijd_oud = Startdatum,
                                               starttijd_nieuw = Starttijd_Suggest,
                                               eindtijd_oud = Einddatum,
                                               eindtijd_nieuw = Eindtijd_Suggest,
                                               duur_oud = `Duur (uren)`,
                                               duur_nieuw = duur_suggest,
                                               zonsopkomst = sunrise,
                                               zonsondergang = sunset 
                                               )
# View(veldbezoeken_export_aanpassingen)

# Export an csv file for the meta data 
out_path <- "Data_Output/veldbezoeken_export_aanpassingenMETA.csv"
dir.create(dirname(out_path), recursive = TRUE, showWarnings = FALSE)
write.csv(
  veldbezoeken_export_aanpassingen,
  out_path,
  row.names = FALSE,
  na = ""
)

