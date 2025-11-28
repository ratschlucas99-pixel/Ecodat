
# helper: convert an instant to local tz (preserve moment), typical for suncalc outputs in UTC
to_local <- function(x, tz = "Europe/Amsterdam") {
  if (inherits(x, "POSIXt")) {
    with_tz(x, tz)
  } else {
    # if strings slipped in, try to parse assuming they represent an instant (UTC-ish)
    ymd_hms(x, tz = tz, quiet = TRUE)
  }
}