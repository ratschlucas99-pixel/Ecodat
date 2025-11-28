library(dplyr)
library(lubridate)
library(stringr)

get_fieldvisit_timesuggest <- function(df, tz = "Europe/Amsterdam") {
  
  ################### !!!!!!!!!!! Link to seperate function #########################
  # helper: convert an instant to local tz (preserve moment), typical for suncalc outputs in UTC
  to_local <- function(x) {
    if (inherits(x, "POSIXt")) {
      with_tz(x, tz)
    } else {
      # if strings slipped in, try to parse assuming they represent an instant (UTC-ish)
      ymd_hms(x, tz = tz, quiet = TRUE)
    }
  }
  
  df %>%
    mutate(
      # 1) normalize datetimes
      Starttijd_Suggest = parse_local(Startdatum,  tz = "Europe/Amsterdam"),
      Eindtijd_Suggest  = parse_local(Einddatum,  tz = "Europe/Amsterdam"),
      sunset_local  = to_local(sunset),
      sunrise_local = to_local(sunrise),
      duur_suggest = NA,
      
      
      
      ### VM01 Avond
      Starttijd_Suggest = case_when(
        grepl("VM01", Project, ignore.case = TRUE) &
          grepl("^avond", Dagdeel, ignore.case = TRUE) &
          !is.na(Startdatum) & !is.na(sunset_local) &
          (Starttijd_Suggest > sunset_local | 
             Starttijd_Suggest < sunset_local - hours(1)) ~ sunset_local,
        TRUE ~ Starttijd_Suggest
      ),
      
      
      Eindtijd_Suggest = case_when(
        grepl("VM01", Project, TRUE) &
          grepl("^avond", Dagdeel, TRUE) &
          !is.na(Eindtijd_Suggest) & !is.na(sunset_local) &
          (Eindtijd_Suggest < sunset_local + hours(3) |  # outside [3h, 4h]
             Eindtijd_Suggest > sunset_local + hours(4)) ~ sunset_local + hours(3),
        TRUE ~ Eindtijd_Suggest
      ),
      
      
      #VM01 ochtend
      Eindtijd_Suggest = case_when(
        grepl("VM01", Project, TRUE) &
          grepl("^ochtend", Dagdeel, TRUE) &
          !is.na(Eindtijd_Suggest) & !is.na(sunrise_local) &
          (Eindtijd_Suggest < sunrise_local |            # outside [sunrise, sunrise+4h]
             Eindtijd_Suggest > sunrise_local + hours(4)) ~ sunrise_local,
        TRUE ~ Eindtijd_Suggest
      ),
      
      Starttijd_Suggest = case_when(
        grepl("VM01", Project, TRUE) &
          grepl("^ochtend", Dagdeel, TRUE) &
          !is.na(Starttijd_Suggest) & !is.na(sunrise_local) &
          (Starttijd_Suggest < sunrise_local - hours(4) |  # outside [sunrise-4h, sunrise-3h]
             Starttijd_Suggest > sunrise_local - hours(3)) ~ sunrise_local - hours(3),
        TRUE ~ Starttijd_Suggest
      ),
      
      
      # time-of-day in minutes
      start_min = hour(Starttijd_Suggest)*60 + minute(Starttijd_Suggest),
      end_min   = hour(Eindtijd_Suggest)*60 + minute(Eindtijd_Suggest),
      
      # --- VM02 avond: start must be in [22:59, 23:59]; otherwise set to 23:59 ---
      Starttijd_Suggest = case_when(
        grepl("^VM02$", Project, ignore.case = TRUE) &
          grepl("^avond", Dagdeel, ignore.case = TRUE) &
          !is.na(Starttijd_Suggest) &
          (start_min < (22*60 + 59) | start_min > (23*60 + 59)) ~
          update(Starttijd_Suggest, hour = 23, minute = 59, second = 0),
        TRUE ~ Starttijd_Suggest
      ),
      
      # If end isn't after start (e.g., same-day), move end to next day
      Eindtijd_Suggest = case_when(
        grepl("^VM02$", Project, ignore.case = TRUE) &
          grepl("^avond", Dagdeel, ignore.case = TRUE) &
          !is.na(Eindtijd_Suggest) & !is.na(Starttijd_Suggest) &
          Eindtijd_Suggest <= Starttijd_Suggest ~ Eindtijd_Suggest + days(1),
        TRUE ~ Eindtijd_Suggest
      )
    ) %>%
    # recompute end_min after possible +days(1)
    mutate(
      end_min = hour(Eindtijd_Suggest)*60 + minute(Eindtijd_Suggest),
      
      # --- VM02 avond: end must be within [02:00, 03:00] ---
      Eindtijd_Suggest = case_when(
        grepl("^VM02$", Project, ignore.case = TRUE) &
          grepl("^avond", Dagdeel, ignore.case = TRUE) &
          !is.na(Eindtijd_Suggest) & end_min < (2*60) ~
          update(Eindtijd_Suggest, hour = 2, minute = 0, second = 0),
        
        grepl("^VM02$", Project, ignore.case = TRUE) &
          grepl("^avond", Dagdeel, ignore.case = TRUE) &
          !is.na(Eindtijd_Suggest) & end_min > (3*60) ~
          update(Eindtijd_Suggest, hour = 3, minute = 0, second = 0),
        
        TRUE ~ Eindtijd_Suggest
      ),
      

      
      #GZ start 1.5 before sunset - 0.5 hour after sunset - duration 2 hours

      Starttijd_Suggest = case_when(
        grepl("GZ", Project, TRUE) &
          !is.na(Starttijd_Suggest) & !is.na(sunset_local) &
          (Starttijd_Suggest > sunset_local - minutes(90) |  # outside [sunrise-4h, sunrise-3h]
             Starttijd_Suggest < sunset_local - minutes(150)) ~ sunset_local - minutes(90),
        TRUE ~ Starttijd_Suggest
      ),
      
      Eindtijd_Suggest = case_when(
        grepl("GZ", Project, TRUE) &
          !is.na(Eindtijd_Suggest) & !is.na(sunset_local) &
          (Eindtijd_Suggest < sunset_local + minutes(30) |            # outside [sunrise, sunrise+4h]
             Eindtijd_Suggest > sunset_local + minutes(90)) ~ sunset_local + minutes(30),
        TRUE ~ Eindtijd_Suggest
      ),
      
      
      
      #GZ start 1.5 before sunrise - 0.5 hour after sunrise - duration 2 hours
      
      Starttijd_Suggest = case_when(
        grepl("ZR", Project, TRUE) &
          !is.na(Starttijd_Suggest) & !is.na(sunrise_local) &
          (Starttijd_Suggest > sunrise_local - minutes(90) |  
             Starttijd_Suggest < sunrise_local - minutes(150)) ~ sunrise_local - minutes(90),
        TRUE ~ Starttijd_Suggest
      ),
      
      Eindtijd_Suggest = case_when(
        grepl("ZR", Project, TRUE) &
          !is.na(Eindtijd_Suggest) & !is.na(sunrise_local) &
          (Eindtijd_Suggest < sunrise_local + minutes(30) |            
             Eindtijd_Suggest > sunrise_local + minutes(90)) ~ sunrise_local + minutes(30),
        TRUE ~ Eindtijd_Suggest
      ),
      
      duur_suggest = case_when(
        !is.na(Starttijd_Suggest) & !is.na(Eindtijd_Suggest) ~
          as.numeric(difftime(Eindtijd_Suggest, Starttijd_Suggest, units = "hours")),
        TRUE ~ duur_suggest
      )
    ) 
  
  
}

