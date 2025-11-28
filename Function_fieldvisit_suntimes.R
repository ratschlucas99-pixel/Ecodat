library(dplyr)
library(tidyr)
library(suncalc)
library(lubridate)

get_fieldvisit_suntimes <- function(waarnemingen_export, veldbezoeken_export, projecten_export,
                                    outfile = NULL) {
  
  # 1. Projects with their city
  Unique_Projects_Cities <- projecten_export %>%
    select(Project_ID = ID,
           Stad, Naam)
  
  # 2. For each field visit, take the first coordinates and keep project ID
  Unique_Projects_GPS <- waarnemingen_export %>%
    group_by(`Veldbezoek ID...23`) %>%
    slice(1) %>%
    ungroup() %>%
    select(Project_ID    = `Project ID...24`,
           Veldbezoek_ID = `Veldbezoek ID...23`,
           Coördinaten)
  
  # 3. Combine projects with their city + GPS
  Project_Cities_GPS <- Unique_Projects_Cities %>%
    inner_join(Unique_Projects_GPS, by = "Project_ID")
  
  # 4. Field visit dates
  Fieldvisit_Date <- veldbezoeken_export %>%
    select(Veldbezoek_ID = ID,
           Project_ID    = project_id,
           Startdatum) %>%
    mutate(Datum = as.Date(Startdatum))
  
  # 5. Add date to project+city+GPS
  Fieldvisit_Location_Date <- Project_Cities_GPS %>%
    inner_join(Fieldvisit_Date %>% select(Veldbezoek_ID, Datum),
               by = "Veldbezoek_ID") %>%
    separate(Coördinaten, into = c("lat", "lon"), sep = ",", remove = FALSE) %>%
    mutate(lat = as.numeric(lat),
           lon = as.numeric(lon))
  
  # 6. Apply getSunlightTimes row by row
  sun_times <- Fieldvisit_Location_Date %>%
    rowwise() %>%
    mutate(sun = list(getSunlightTimes(date = Datum,
                                       lat = lat,
                                       lon = lon,
                                       keep = c("sunrise", "sunset")))) %>%
    unnest_wider(sun, names_sep = "_") %>%
    ungroup()
  
  # 7. Change timezone including daylight saving
  sun_times_fieldvisit <- sun_times %>%
    mutate(
      sunrise = with_tz(sun_sunrise, "Europe/Amsterdam"),
      sunset  = with_tz(sun_sunset,  "Europe/Amsterdam")
    ) %>%
    select(Project_ID, Naam, Veldbezoek_ID, Stad, Coördinaten, sunrise, sunset)
  
  # 8. Optionally write to CSV
  if (!is.null(outfile)) {
    write.csv(sun_times_fieldvisit, outfile, row.names = FALSE)
  }
  
  return(sun_times_fieldvisit)
}
