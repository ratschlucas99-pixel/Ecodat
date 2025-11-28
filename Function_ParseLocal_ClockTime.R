
# helper: parse a local clock time (character or POSIXct) and attach tz without shifting clock
parse_local <- function(x, tz = "Europe/Amsterdam") {
  if (inherits(x, "POSIXt")) {
    force_tz(x, tz)              # treat stored clock time as local
  } else {
    ymd_hms(x, tz = tz, quiet = TRUE)
  }
}
