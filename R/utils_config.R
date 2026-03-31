# =====================================================================
# utils_config.R
# ---------------------------------------------------------------------
# Fonctions de chargement et de validation minimale de la configuration.
# =====================================================================

load_config <- function(config_path = "config/app.yml") {
  if (!file.exists(config_path)) {
    stop(sprintf("Fichier de configuration introuvable : %s", config_path), call. = FALSE)
  }

  config <- yaml::read_yaml(config_path)

  validate_config(config)

  config
}

validate_config <- function(config) {
  required_top_level <- c("project", "paths", "ingestion", "allowed_extensions", "naming", "features", "versions")
  missing_top_level <- setdiff(required_top_level, names(config))

  if (length(missing_top_level) > 0) {
    stop(
      sprintf(
        "Configuration invalide. Sections manquantes : %s",
        paste(missing_top_level, collapse = ", ")
      ),
      call. = FALSE
    )
  }

  required_paths <- c(
    "drop_root", "quarantine_root", "processing_root",
    "archive_root", "analysis_root", "logs_root", "registry_db"
  )

  missing_paths <- setdiff(required_paths, names(config$paths))
  if (length(missing_paths) > 0) {
    stop(
      sprintf("Configuration invalide. Chemins manquants : %s", paste(missing_paths, collapse = ", ")),
      call. = FALSE
    )
  }

  invisible(TRUE)
}
