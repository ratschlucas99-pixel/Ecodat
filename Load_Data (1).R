##############
#### Cleans data from the 'Veldapp' #############

############## Set work directory
setwd("C:/Users/Gebruiker/Ed Michels/Bedrijf - 01- Projecten/ZijnR_DataProjecten_2025/Velddata_opschonen/VeldApp__veldbezoeken_R/Data_Input")


########## Install packages

list.of.packages <- c("suncalc", "dplyr", "lubridate", "readr", "suncalc", "tidyr", "stringr")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)




################# Import data ##################
projecten_export <- read_delim("projecten_export_2025-09-23_17-15.csv", 
                               delim = ";", escape_double = FALSE,
                               col_types = cols(ID = col_character(), 
                                                `Aangemaakt op` = col_datetime(format = "%Y-%m-%d %H:%M:%S"), 
                                                `Bijgewerkt op` = col_datetime(format = "%Y-%m-%d %H:%M:%S")), 
                               trim_ws = TRUE)


veldbezoeken_export <- read_delim("veldbezoeken_export_2025-09-23_17-16.csv", 
                                  delim = ";", escape_double = FALSE,
                                  col_types = cols(ID = col_character(), 
                                                   `Aangemaakt op` = col_datetime(format = "%Y-%m-%d %H:%M:%S"), 
                                                   `Bijgewerkt op` = col_datetime(format = "%Y-%m-%d %H:%M:%S"), 
                                                   Startdatum = col_datetime(format = "%Y-%m-%d %H:%M:%S"), 
                                                   Einddatum = col_datetime(format = "%Y-%m-%d %H:%M:%S"), 
                                                   project_id = col_character(), client_id = col_character(), 
                                                   form_id = col_character(), created_by_id = col_character(), 
                                                   updated_by_id = col_character()), 
                                  trim_ws = TRUE)


waarnemingen_export <- read_delim("waarnemingen_export_2025-09-23_17-17.csv", 
                                  delim = ";", escape_double = FALSE,
                                  col_types = cols(`Gezien op` = col_datetime(format = "%Y-%m-%d %H:%M:%S"), 
                                                   `Aangemaakt op` = col_datetime(format = "%Y-%m-%d %H:%M:%S"), 
                                                   `Bijgewerkt op` = col_datetime(format = "%Y-%m-%d %H:%M:%S"), 
                                                   `Veldbezoek ID...23` = col_character(), 
                                                   `Project ID...24` = col_character(), 
                                                   `Opdrachtgever ID...25` = col_character(), 
                                                   `Formulier ID...26` = col_character(), 
                                                   `Aangemaakt door ID...27` = col_character(), 
                                                   `Bijgewerkt door ID...28` = col_character(), 
                                                   `Veldbezoek ID...35` = col_skip(), 
                                                   `Project ID...36` = col_skip(),
                                                   `Opdrachtgever ID...37` = col_skip(), 
                                                   `Formulier ID...38` = col_skip(), 
                                                   `Aangemaakt door ID...39` = col_skip(), 
                                                   `Bijgewerkt door ID...40` = col_skip()), 
                                  trim_ws = TRUE)





