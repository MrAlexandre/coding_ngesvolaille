# =====================================================================
# utils_time.R
# ---------------------------------------------------------------------
# Fonctions liées au temps, aux horodatages et à l'identifiant de run.
# =====================================================================

get_now_paris <- function() {
  as.POSIXct(Sys.time(), tz = "Europe/Paris")
}

format_timestamp <- function(x = get_now_paris(), format = "%Y-%m-%d %H:%M:%S") {
  format(as.POSIXct(x, tz = "Europe/Paris"), format = format, tz = "Europe/Paris")
}

generate_ingestion_id <- function(prefix = "ING", time = get_now_paris()) {
  paste0(prefix, "_", format(as.POSIXct(time, tz = "Europe/Paris"), "%Y%m%d_%H%M"))
}

generate_event_id <- function(prefix = "EVT", time = get_now_paris()) {
  paste0(prefix, "_", format(as.POSIXct(time, tz = "Europe/Paris"), "%Y%m%d_%H%M%S"))
}
