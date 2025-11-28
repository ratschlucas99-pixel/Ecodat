

waarnemingen_export_ZijnR <- waarnemingen_export3 %>%
  select(Verblijfnummer = Verblijfnummer,
         Groep = Groep,
         Soort = Soort,
         Datum = Datum, 
         Tijd = Tijd,
         Aantal = Aantal, 
         Gedrag = Gedrag,
         Verblijfplaats = Verblijfplaats,
         Sekse = Sekse,
         Adres = Adres,
         Plaats = Plaats,
         Locatie_adres = Locatie_adres,
         Functie = Functie,
         Projectnaam = Projectnaam
  )



# helper to make safe filenames
library(dplyr)
library(stringr)
out_dir <- "Data_Output" ; dir.create(out_dir, showWarnings = FALSE)


dir.create(out_dir, showWarnings = FALSE)

waarnemingen_export_ZijnR %>%
  filter(!is.na(Projectnaam)) %>%
  group_by(Projectnaam) %>%
  group_walk(~ {
    fname <- paste0("waarnemingen_export_", safe_name(.y$Projectnaam), ".csv")
    write.csv2(.x, file = file.path(out_dir, fname), row.names = FALSE, na = "")
  })

