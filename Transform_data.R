# Adjust columns

waarnemingen_export1 <-  waarnemingen_export %>%
                              mutate(Verblijfnummer = NA,
                                     Functie = NA,
                                     Datum = as.Date(`Gezien op`, format = "%Y-%m-%d %H:%M:%S"),
                                     Tijd  = format(`Gezien op`, format = "%H:%M:%S"),
                                     Adres = NA,
                                     Plaats = NA,
                                     Locatie_adres = Opmerking,
                                     Functie = NA)


unique(waarnemingen_export1$Soort)



library(dplyr)
library(stringr)

######################### Add groep, this is Groep as used in the report app ##########


waarnemingen_export1 <- waarnemingen_export1 %>%
  mutate(
    Groep = case_when(
      # explicit mappings from Soort
      str_detect(Soort, regex("vleermuis|vlieger", ignore_case = TRUE)) ~ "Vleermuizen",
      str_detect(Soort, regex("\\bmuis\\b|vos|pad|salamander", ignore_case = TRUE)) ~ "Overig",
      
      # if Groep is NA → onbekend
      is.na(Soort) ~ "onbekend",
      
      # everything else → Vogel
      TRUE ~ "Vogel"
    )
  )


############### Fill in missing values for "Aantal" ############

## Fill in 1 for all observations with count == NA
waarnemingen_export1[is.na(waarnemingen_export1$Aantal),]$Aantal <- 1


################## Add location info to "Adres" and "Plaats"  #####################
## use Coordinates to get an approx street name and city/municipality

## Seperate latitude and longitude


# unique(waarnemingen_export1$Gedrag)

library(dplyr)
library(tidygeocoder)
library(purrr)

###  selection to add addresses 
locatie_verblijf <- c("Invliegend (algemeen)", "baltsend", "zwermend (algemeen)", 
  "uitvliegend (algemeen)", "nest-indicerend gedrag", "territoriumindicerend",
  "ter plaatse", "slaapplaats", "bezoek aan nestplaats", "baltsend/zingend",
  "rustend", "nestbouw", "parend / copula")



######################### Get Addresses ##########################

# Add addresses ONLY where Gedrag is in locatie_verblijf (and coords present)


options(timeout = 120)
min_pause <- 1.1  # respect Nominatim rate limit

# subset to what you want to geocode
coords_tbl <- waarnemingen_export1 %>%
  filter(Gedrag %in% locatie_verblijf,
         !is.na(Breedtegraad), !is.na(Lengtegraad))

# split into chunks
batch_size <- 100
batches <- split(coords_tbl, ceiling(seq_len(nrow(coords_tbl)) / batch_size))



# helper: geocode one batch with retry
geocode_batch <- function(df, i) {
  message("Processing batch ", i, " of ", length(batches))
  # try up to 3 times on transient failures
  for (attempt in 1:3) {
    res <- try(
      tidygeocoder::reverse_geocode(
        .tbl       = df,                # <- fix here
        lat        = Breedtegraad,
        long       = Lengtegraad,
        method     = "osm",
        address    = "address",
        limit      = 1,
        batch_limit = 1,
        min_time   = min_pause,
        return_input = TRUE
      ),
      silent = TRUE
    )
    if (!inherits(res, "try-error")) {
      saveRDS(res, sprintf("geo_batch_%03d.rds", as.integer(i)))
      # checkpoint
      return(res)
    }
    Sys.sleep(3 * attempt)  # backoff before retry
  }
  stop("Batch ", i, " failed after 3 attempts.")
}

results_list <- imap(batches, geocode_batch)
files <- list.files(pattern = "^geo_batch_\\d+\\.rds$")
geo_all <- bind_rows(lapply(files, readRDS))

# Join address DF and original DF
waarnemingen_export2 <- waarnemingen_export1 %>%
  left_join(geo_all[,c("Breedtegraad", "Lengtegraad","address")], by = c("Breedtegraad", "Lengtegraad"))










########### Add functie

waarnemingen_export2[waarnemingen_export2$Groep == "Vleermuis",]$Groep <- "Vleermuizen"
waarnemingen_export2[waarnemingen_export2$Groep == "Vogel",]$Groep <- "Vogels"


unique(waarnemingen_export2$Gedrag)


locatie_verblijf_vleer_VM01 <- c("Invliegend (algemeen)", "uitvliegend (algemeen)", "territoriumindicerend",
                                    "ter plaatse",  "bezoek aan nestplaats" 
                                    )

locatie_verblijf_vleer_VM023 <- c("baltsend", "zwermend (algemeen)",
                                  "baltsend/zingend", "parend / copula"
                                  )

vliegroute <- c("overvliegend","passerend (niet nader omschreven)", "overvliegend naar noord",
                "overvliegend naar zuid", "overvliegend naar oost", "overvliegend naar west")

locatie_verblijf_vogels <- c("Invliegend (algemeen)", "uitvliegend (algemeen)", "territoriumindicerend",
                                 "ter plaatse",  "bezoek aan nestplaats", "parend / copula",
                             "baltsend/zingend", "baltsend", "slaapplaats", "nest-indicerend gedrag",
                             "roepend", "nestbouw", "rustend"
                                )

foerageergebied <- "foeragerend"


waarnemingen_export2 <- waarnemingen_export2 %>%
  mutate(
    Functie = case_when(
      Groep == "Vleermuizen" &
        Gedrag %in% locatie_verblijf_vleer_VM01 &
        !is.na(Aantal) & Aantal < 10  ~ "zomerverblijfplaats",
      
      Groep == "Vleermuizen" &
        Gedrag %in% locatie_verblijf_vleer_VM01 &
        !is.na(Aantal) & Aantal > 9   ~ "kraamverblijfplaats",
      
      Groep == "Vleermuizen" &
        Gedrag %in% locatie_verblijf_vleer_VM023 &
        !is.na(Aantal)   ~ "paarverblijfplaats",
      
      Groep == "Vogels" &
        Gedrag %in% locatie_verblijf_vogels &
        !is.na(Aantal)   ~ "nestlocatie",
      
      Gedrag %in% vliegroute &
        !is.na(Aantal)   ~ "vliegroute",
      
      Gedrag %in% foerageergebied &
        !is.na(Aantal) ~ "foerageergebied",
      
      TRUE ~ Functie        # keep existing value otherwise
    )
  )

Nakijken <- waarnemingen_export2[is.na(waarnemingen_export2$Functie), ]
waarnemingen_export2[is.na(waarnemingen_export2$Functie), ]$Gedrag
waarnemingen_export2[is.na(waarnemingen_export2$Functie) & waarnemingen_export2$Gedrag == "onbepaald", ]

length(waarnemingen_export2[is.na(waarnemingen_export2$Functie), ]$Functie)


write.csv2(Nakijken, "Nakijken.csv")

library(dplyr)

waarnemingen_export3 <- waarnemingen_export2 %>%
  filter(!is.na(Functie))

############# add verblijfnummer  ###############

library(dplyr)

waarnemingen_export3 <- waarnemingen_export3 %>%
  mutate(
    Verblijfnummer = match(address, unique(address))   # 1,2,3.. by first occurrence
  )

library(stringr)

x <- "24, Cimbaalhof, Grauwe Polder, Etten-Leur, Noord-Brabant, Nederland, 4876 BP, Nederland"

parts  <- str_split(waarnemingen_export3$address, ",")[[1]] |> str_trim()
Adres  <- str_glue("{parts[2]} {parts[1]}")   # "Cimbaalhof 24"
Plaats <- parts[4]                            # "Etten-Leur"

Adres; Plaats

  
  
library(dplyr)
library(tidyr)
library(stringr)

df_parsed <- waarnemingen_export3 %>%                        # replace df with your data frame name
  mutate(address = str_squish(address)) %>%# replace 'address' with your column
  separate_wider_delim(
    address, delim = ",",
    names = c("nr","straat","wijk","plaats","prov","land","postcode","land2"),
    too_few = "align_start"
  ) %>%
  mutate(across(everything(), ~str_trim(.))) %>%
  transmute(Adres = str_c(straat, " ", nr), Plaats = plaats)

  
  
library(dplyr)
library(stringr)
library(purrr)
library(tidyr)

library(dplyr)
library(purrr)
library(stringr)
library(tidyr)

# helper from before
parse_addr <- function(s) {
  if (is.na(s) || !nzchar(s)) return(tibble(Adres = NA_character_, Plaats = NA_character_))
  parts <- str_split(s, ",")[[1]] |> str_trim() |> discard(~ .x == "")
  n <- length(parts)
  nr     <- if (n >= 1) parts[1] else NA_character_
  straat <- if (n >= 2) parts[2] else NA_character_
  plaats <- if (n >= 5) parts[n - 4] else NA_character_  # 5th from right
  tibble(Adres = str_squish(str_c(straat, " ", nr)), Plaats = plaats)
}

waarnemingen_export3 <- waarnemingen_export3 %>%
  mutate(parsed = map(address, parse_addr)) %>%
  unnest_wider(parsed, names_sep = "_") %>%         # creates parsed_Adres, parsed_Plaats
  mutate(
    Adres  = coalesce(Adres,  parsed_Adres),        # or: Adres  = parsed_Adres
    Plaats = coalesce(Plaats, parsed_Plaats)        # or: Plaats = parsed_Plaats
  ) %>%
  select(-parsed_Adres, -parsed_Plaats)


write.csv2(waarnemingen_export3, "waarnemingen_export3.csv")
####################### Add project name ######################
veldbezoeken_export_2025_09_23_17_16 <- read_delim("C:/Users/Gebruiker/Ed Michels/Bedrijf - 01- Projecten/ZijnR_DataProjecten_2025/Velddata_opschonen/VeldApp__veldbezoeken_R/Data_Input/veldbezoeken_export_2025-09-23_17-16.csv", 
                                                   delim = ";", escape_double = FALSE, trim_ws = TRUE)


waarnemingen_export3 <- waarnemingen_export3 %>%
  mutate(
    Projectnaam = veldbezoeken_export_2025_09_23_17_16$`Project Naam`[
      match(
        as.character(`Project ID...25`),
        as.character(veldbezoeken_export_2025_09_23_17_16$project_id )
      )
    ]
  )

