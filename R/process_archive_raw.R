# =====================================================================
# process_archive_raw.R
# ---------------------------------------------------------------------
# Archivage RAW minimal :
# 1. calcul du SHA-256
# 2. création d'un dossier d'archive propre au fichier + run
# 3. copie du fichier brut
# 4. génération d'un manifest JSON
# =====================================================================

archive_raw_file <- function(conn, file_info, ingestion_id, config, log_file) {
  sha256 <- compute_sha256(file_info$source_path)
  update_file_hash(conn, file_info$file_id, sha256)

  stem <- fs::path_ext_remove(file_info$source_filename)
  archive_folder <- build_archive_folder(config, file_info$partner, stem, ingestion_id)
  ensure_dir(archive_folder)

  archived_file_path <- fs::path(archive_folder, file_info$source_filename)
  safe_copy_file(file_info$source_path, archived_file_path, overwrite = TRUE)

  manifest <- list(
    project = config$project$name,
    ingestion_id = ingestion_id,
    file_id = file_info$file_id,
    source_filename = file_info$source_filename,
    source_path = file_info$source_path,
    archived_file_path = archived_file_path,
    partner = file_info$partner,
    sha256 = sha256,
    archived_at = format_timestamp(),
    protocol_version = config$versions$protocol,
    qc_rules_version = config$versions$qc_rules,
    scripts_version = config$versions$scripts
  )

  manifest_path <- fs::path(archive_folder, "manifest.json")
  jsonlite::write_json(manifest, manifest_path, pretty = TRUE, auto_unbox = TRUE)

  update_archive_path(conn, file_info$file_id, archived_file_path)

  log_event_db(
    conn = conn,
    ingestion_id = ingestion_id,
    file_id = file_info$file_id,
    step_code = "RAW",
    action_type = "ARCHIVE",
    status = "OK",
    message = "Archivage RAW réalisé.",
    details = list(
      sha256 = sha256,
      archive_folder = archive_folder,
      manifest_path = manifest_path
    )
  )

  log_info(log_file, sprintf("Archivage RAW réalisé pour %s", file_info$source_filename))

  invisible(list(
    sha256 = sha256,
    archive_folder = archive_folder,
    archived_file_path = archived_file_path,
    manifest_path = manifest_path
  ))
}
