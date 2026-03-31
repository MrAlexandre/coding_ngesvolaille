# =====================================================================
# process_detect_new_files.R
# ---------------------------------------------------------------------
# Détection des fichiers nouveaux présents dans les dépôts partenaires.
# Le principe est volontairement simple en V1 :
# - scan récursif du dossier de dépôt ;
# - exclusion des dossiers ;
# - comparaison au registre sur `source_path`.
# =====================================================================

list_dropzone_files <- function(drop_root) {
  if (!dir.exists(drop_root)) {
    warning(sprintf("Le dossier de dépôt n'existe pas encore : %s", drop_root), call. = FALSE)
    return(character(0))
  }

  fs::dir_ls(drop_root, recurse = TRUE, type = "file")
}

extract_partner_from_path <- function(file_path, drop_root) {
  rel_path <- fs::path_rel(file_path, start = drop_root)
  parts <- strsplit(rel_path, .Platform$file.sep, fixed = TRUE)[[1]]

  # Convention simple : le premier dossier sous drop_root représente le partenaire.
  if (length(parts) >= 2) {
    return(parts[1])
  }

  "INCONNU"
}

build_file_info <- function(file_path, config) {
  file_id <- generate_file_id()

  list(
    file_id = file_id,
    partner = extract_partner_from_path(file_path, config$paths$drop_root),
    source_filename = basename(file_path),
    source_path = normalizePath(file_path, winslash = "/", mustWork = FALSE),
    extension = get_file_extension(file_path),
    file_size_bytes = file.info(file_path)$size,
    detected_at = get_now_paris()
  )
}

detect_new_files <- function(conn, config, log_file) {
  all_files <- list_dropzone_files(config$paths$drop_root)

  if (length(all_files) == 0) {
    log_info(log_file, "Aucun fichier trouvé dans le dossier de dépôt.")
    return(list())
  }

  log_info(log_file, sprintf("%s fichier(s) trouvé(s) dans le dépôt.", length(all_files)))

  new_files <- list()

  for (file_path in all_files) {
    normalized_path <- normalizePath(file_path, winslash = "/", mustWork = FALSE)

    if (!is_already_registered(conn, normalized_path)) {
      new_files[[length(new_files) + 1]] <- build_file_info(normalized_path, config)
    }
  }

  log_info(log_file, sprintf("%s nouveau(x) fichier(s) détecté(s).", length(new_files)))
  new_files
}
