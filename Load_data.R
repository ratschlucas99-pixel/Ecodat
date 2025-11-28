
### set WD
setwd("C:/Users/Gebruiker/Ed Michels/Bedrijf - 01- Projecten/ZijnR_DataProjecten_2025/Velddata_opschonen/VeldApp_waarnemingen_R/Data_input/")

### Load data
library(readr)
library(dplyr)
library(purrr)


install.packages("tidygeocoder")
library(tidygeocoder)

waarnemingen_export <- read_delim("Data_input/waarnemingen_export_2025-09-23_17-17.csv", 
                                         delim = ";", escape_double = FALSE, col_types = cols(`Gezien op` = col_datetime(format = "%Y-%m-%d %H:%M:%S"), 
                                                                                                        `Aangemaakt op` = col_datetime(format = "%Y-%m-%d %H:%M:%S"), 
                                                                                                        `Bijgewerkt op` = col_datetime(format = "%Y-%m-%d %H:%M:%S"), 
                                                                                                        `Veldbezoek ID...23` = col_character(), 
                                                                                                        `Project ID...24` = col_character(), 
                                                                                                        `Formulier ID...26` = col_character(), 
                                                                                                        `Veldbezoek ID...35` = col_character(), 
                                                                                                        `Project ID...36` = col_character(), 
                                                                                                       `Opdrachtgever ID...37` = col_character()), 
                                         trim_ws = TRUE)
