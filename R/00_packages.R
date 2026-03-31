# =====================================================================
# 00_packages.R
# ---------------------------------------------------------------------
# Ce fichier centralise le chargement des packages R nécessaires.
# L'objectif est d'avoir un seul endroit à modifier si une dépendance
# change au cours du projet.
# =====================================================================

required_packages <- c(
  "DBI",
  "RSQLite",
  "fs",
  "yaml",
  "stringr",
  "digest",
  "readr",
  "readxl",
  "dplyr",
  "tibble",
  "purrr",
  "glue",
  "jsonlite",
  "lubridate",
  "arrow",
  "cli"
)

load_required_packages <- function() {
  missing_pkgs <- required_packages[!vapply(required_packages, requireNamespace, logical(1), quietly = TRUE)]

  if (length(missing_pkgs) > 0) {
    stop(
      paste0(
        "Packages manquants : ",
        paste(missing_pkgs, collapse = ", "),
        ".\nInstallez-les avant l'exécution."
      ),
      call. = FALSE
    )
  }

  invisible(lapply(required_packages, library, character.only = TRUE))
}
