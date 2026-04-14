# =========================================================
# process_qc03_technical.R
# NGESvolaille - Pipeline V1
# Orchestration du contrôle technique agrégé QC03
# =========================================================

# ---------------------------------------------------------
# Fonction : normalisation de chemin
# ---------------------------------------------------------
normalize_path_safe <- function(path) {
  normalizePath(path, winslash = "/", mustWork = FALSE)
}

# ---------------------------------------------------------
# Fonction : extraction du partenaire depuis le dossier dépôt
# Hypothèse V1 :
# ATTENTION : en V1, je travaille juste pour 1 partenaire : le partenaire correspond au premier sous-dossier sous drop_root
# Exemple :
# .../Depot/Partenaire_A/fichier.csv -> Partenaire_A
# ---------------------------------------------------------
extract_partner_from_path <- function(file_path, drop_root) {

  file_path_norm <- normalizePath(file_path, winslash = "/", mustWork = FALSE)
  drop_root_norm <- normalizePath(drop_root, winslash = "/", mustWork = FALSE)

  # On enlève le dossier dépôt du chemin
  relative_path <- sub(paste0("^", drop_root_norm, "/?"), "", file_path_norm)

  parts <- strsplit(relative_path, "/")[[1]]

  # Le partenaire est le premier dossier après Depot
  if (length(parts) >= 2) {
    return(parts[1])
  }

  return("Partenaire_inconnu")
}

# ---------------------------------------------------------
# Fonction : construction du nom de fichier
# en quarantaine
# Convention : TECH_<ingestion_id>_<nom_original>
# ---------------------------------------------------------
build_quarantine_filename <- function(file_path, ingestion_id, prefix = "TECH") {
  paste0(prefix, "_", ingestion_id, "_", basename(file_path))
}

# ---------------------------------------------------------
# Fonction : construction du chemin cible
# de quarantaine
#
# Règle V1 :
# une seule zone de quarantaine, avec sous-dossier partenaire
# ---------------------------------------------------------
build_quarantine_target_path <- function(file_path, ingestion_id, quarantine_root, drop_root, prefix = "TECH") {
  partner_name <- extract_partner_from_path(file_path, drop_root)

  if (is.na(partner_name) || !nzchar(partner_name)) {
    partner_name <- "Partenaire_inconnu"
  }

  quarantine_filename <- build_quarantine_filename(
    file_path = file_path,
    ingestion_id = ingestion_id,
    prefix = prefix
  )

  file.path(quarantine_root, partner_name, quarantine_filename)
}

# ---------------------------------------------------------
# Fonction : déplacement physique en quarantaine
# ---------------------------------------------------------
move_file_to_quarantine <- function(file_path, target_path) {
  dir.create(dirname(target_path), recursive = TRUE, showWarnings = FALSE)

  moved <- file.rename(from = file_path, to = target_path)

  if (!isTRUE(moved)) {
    stop(paste0(
      "Impossible de déplacer le fichier vers la quarantaine : ",
      target_path
    ))
  }

  target_path
}

# ---------------------------------------------------------
# Fonction principale : process_qc03_technical
# ---------------------------------------------------------
process_qc03_technical <- function(file_path, ingestion_id, config) {
  if (missing(config) || is.null(config)) {
    stop("Le paramètre 'config' est requis.")
  }

  if (!file.exists(file_path)) {
    stop(paste0("Le fichier n'existe pas : ", file_path))
  }

  qc03_result <- run_qc03_technical(
    file_path = file_path,
    ingestion_id = ingestion_id,
    config = config
  )

  quarantine_path <- NA_character_
  file_moved <- FALSE

  if (identical(qc03_result$status_qc03, "KO")) {
    quarantine_path <- build_quarantine_target_path(
      file_path = file_path,
      ingestion_id = ingestion_id,
      quarantine_root = config$paths$quarantine_root,
      drop_root = config$paths$drop_root,
      prefix = "TECH"
    )

    quarantine_path <- move_file_to_quarantine(
      file_path = file_path,
      target_path = quarantine_path
    )

    file_moved <- TRUE
  }

  list(
    file_path = file_path,
    ingestion_id = ingestion_id,
    status_qc03 = qc03_result$status_qc03,
    redirect_action = qc03_result$redirect_action,
    sha256 = qc03_result$sha256,
    manifest = qc03_result$manifest,
    manifest_output_path = qc03_result$manifest_output_path,
    manifest_written = qc03_result$manifest_written,
    quarantine_path = quarantine_path,
    file_moved = file_moved,
    subchecks = qc03_result$subchecks,
    message = qc03_result$message
  )
}

# ---------------------------------------------------------
# Fonction pratique : chargement config + exécution depuis app.yml
# ---------------------------------------------------------
run_process_qc03_from_config <- function(file_path, ingestion_id, config_path = "config/app.yml") {
  config <- yaml::read_yaml(config_path)

  process_qc03_technical(
    file_path = file_path,
    ingestion_id = ingestion_id,
    config = config
  )
}