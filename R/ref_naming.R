# =====================================================================
# ref_naming.R
# ---------------------------------------------------------------------
# Contrôles de nommage (QC01) et parsing des métadonnées du nom (QC02).
# =====================================================================

validate_filename <- function(filename, regex_pattern) {
  is_valid <- stringr::str_detect(filename, regex_pattern)

  list(
    status = if (is_valid) "OK" else "KO",
    step_code = "QC01",
    message = if (is_valid) "Nommage conforme." else "Nommage non conforme.",
    details = list(filename = filename, regex = regex_pattern)
  )
}

parse_filename <- function(filename, regex_pattern) {
  m <- stringr::str_match(filename, regex_pattern)

  if (all(is.na(m))) {
    return(list(
      status = "KO",
      step_code = "QC02",
      message = "Parsing impossible du nom de fichier.",
      data = NULL,
      details = list(filename = filename)
    ))
  }

  # Lecture sécurisée des groupes capturés.
  parsed <- list(
    site = m[, 2],
    date_reference = as.Date(m[, 3], format = "%Y%m%d"),
    batiment = m[, 4],
    file_type = m[, 5],
    version_num = suppressWarnings(as.integer(m[, 6])),
    extension = m[, 7]
  )

  if (is.na(parsed$date_reference)) {
    return(list(
      status = "KO",
      step_code = "QC02",
      message = "Date présente dans le nom mais non interprétable.",
      data = NULL,
      details = list(filename = filename)
    ))
  }

  list(
    status = "OK",
    step_code = "QC02",
    message = "Parsing du nom réussi.",
    data = parsed,
    details = parsed
  )
}
