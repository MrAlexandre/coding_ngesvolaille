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
  
  # Vérification 1 : s'assurer que le temps fourni est convertible
  time_value <- tryCatch(
    as.POSIXct(time, tz = "Europe/Paris"),
    error = function(e) {
      stop(
        sprintf("Impossible de générer event_id : l'argument 'time' n'est pas convertible en date/heure. Détail : %s", e$message),
        call. = FALSE
      )
    }
  )
  
  # Vérification 2 : refuser une valeur vide ou NA
  if (is.na(time_value)) {
    stop(
      "Impossible de générer event_id : la valeur temporelle fournie est NA ou invalide.",
      call. = FALSE
    )
  }
  
  # Partie horodatage avec fractions de seconde
  time_part <- format(time_value, "%Y%m%d_%H%M%OS6", tz = "Europe/Paris")
  
  # Nettoyage : remplacement du point décimal pour garder un identifiant propre
  time_part <- gsub("\\.", "_", time_part)
  
  # Suffixe aléatoire court pour éviter les collisions dans une même micro-seconde
  random_part <- paste(sample(c(letters, LETTERS, 0:9), size = 6, replace = TRUE), collapse = "")
  
  paste0(prefix, "_", time_part, "_", random_part)
}