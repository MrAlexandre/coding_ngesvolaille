# =====================================================================
# registry_suivi_depots.R
# ---------------------------------------------------------------------
# Fonctions CRUD simples sur la table de suivi des dépôts.
# =====================================================================

generate_file_id <- function() {
  paste0("F_", format(get_now_paris(), "%Y%m%d_%H%M%S"), "_", sprintf("%04d", sample.int(9999, 1)))
}

get_registered_file <- function(conn, source_path) {
  query <- "SELECT * FROM suivi_depots WHERE source_path = ? LIMIT 1"
  DBI::dbGetQuery(conn, query, params = list(source_path))
}

is_already_registered <- function(conn, source_path) {
  nrow(get_registered_file(conn, source_path)) > 0
}

register_new_file <- function(conn, file_info, ingestion_id, global_status = "DETECTE") {
  DBI::dbAppendTable(
    conn = conn,
    name = "suivi_depots",
    value = tibble::tibble(
      file_id = file_info$file_id,
      partner = file_info$partner,
      source_filename = file_info$source_filename,
      source_path = file_info$source_path,
      source_extension = file_info$extension,
      file_size_bytes = file_info$file_size_bytes,
      sha256 = NA_character_,
      site = NA_character_,
      batiment = NA_character_,
      date_reference = NA_character_,
      file_type = NA_character_,
      version_num = NA_integer_,
      first_seen_at = format_timestamp(file_info$detected_at),
      detected_at = format_timestamp(file_info$detected_at),
      ingested_at = NA_character_,
      archived_at = NA_character_,
      qualified_at = NA_character_,
      promoted_at = NA_character_,
      ingestion_id = ingestion_id,
      global_status = global_status,
      last_step_code = "DETECT",
      last_step_status = "OK",
      motif_ko = NA_character_,
      action_attendue = NA_character_,
      archive_path = NA_character_,
      qualified_path = NA_character_,
      comments = NA_character_
    )
  )

  invisible(TRUE)
}

update_file_metadata_from_name <- function(conn, file_id, parsed) {
  query <- "
    UPDATE suivi_depots
    SET site = ?, batiment = ?, date_reference = ?, file_type = ?, version_num = ?
    WHERE file_id = ?
  "

  DBI::dbExecute(
    conn, query,
    params = list(
      parsed$site,
      parsed$batiment,
      as.character(parsed$date_reference),
      parsed$file_type,
      parsed$version_num,
      file_id
    )
  )

  invisible(TRUE)
}

update_file_status <- function(conn, file_id, global_status, last_step_code, last_step_status,
                               motif_ko = NULL, action_attendue = NULL) {
  query <- "
    UPDATE suivi_depots
    SET global_status = ?, last_step_code = ?, last_step_status = ?, motif_ko = ?, action_attendue = ?
    WHERE file_id = ?
  "

  DBI::dbExecute(
    conn, query,
    params = list(global_status, last_step_code, last_step_status, motif_ko, action_attendue, file_id)
  )

  invisible(TRUE)
}

update_file_hash <- function(conn, file_id, sha256) {
  query <- "UPDATE suivi_depots SET sha256 = ? WHERE file_id = ?"
  DBI::dbExecute(conn, query, params = list(sha256, file_id))
  invisible(TRUE)
}

update_archive_path <- function(conn, file_id, archive_path) {
  query <- "UPDATE suivi_depots SET archive_path = ?, archived_at = ? WHERE file_id = ?"
  DBI::dbExecute(conn, query, params = list(archive_path, format_timestamp(), file_id))
  invisible(TRUE)
}
