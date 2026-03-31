# =====================================================================
# utils_paths.R
# ---------------------------------------------------------------------
# Fonctions utilitaires pour gérer les chemins et créer l'arborescence
# nécessaire au fonctionnement du pipeline.
# =====================================================================

ensure_dir <- function(path) {
  fs::dir_create(path, recurse = TRUE)
  invisible(path)
}

ensure_project_directories <- function(config) {
  # On crée les dossiers principaux même s'ils existent déjà.
  # Cela permet d'initialiser proprement une nouvelle machine.
  paths_to_create <- c(
    config$paths$drop_root,
    config$paths$quarantine_root,
    config$paths$processing_root,
    config$paths$archive_root,
    config$paths$analysis_root,
    config$paths$logs_root,
    dirname(config$paths$registry_db)
  )

  purrr::walk(paths_to_create, ensure_dir)
  invisible(TRUE)
}

build_quarantine_path <- function(config, partner, filename) {
  partner_dir <- fs::path(config$paths$quarantine_root, partner)
  ensure_dir(partner_dir)
  fs::path(partner_dir, filename)
}

build_processing_path <- function(config, partner, filename) {
  partner_dir <- fs::path(config$paths$processing_root, partner)
  ensure_dir(partner_dir)
  fs::path(partner_dir, filename)
}

build_archive_folder <- function(config, partner, stem, ingestion_id) {
  partner_dir <- fs::path(config$paths$archive_root, partner)
  ensure_dir(partner_dir)
  fs::path(partner_dir, paste0(stem, "_", ingestion_id))
}

build_log_path <- function(config, ingestion_id, suffix = "ingestion") {
  ensure_dir(config$paths$logs_root)
  fs::path(config$paths$logs_root, paste0(suffix, "_", ingestion_id, ".log"))
}
