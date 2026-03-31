# =====================================================================
# process_quarantine_naming.R
# ---------------------------------------------------------------------
# Gestion de la quarantaine de nommage. Ici, on bloque le fichier dans
# un dossier dédié et on journalise l'action attendue.
# =====================================================================

quarantine_bad_filename <- function(conn, file_info, ingestion_id, config, log_file, reason) {
  target_path <- build_quarantine_path(config, file_info$partner, file_info$source_filename)

  safe_copy_file(file_info$source_path, target_path, overwrite = TRUE)

  update_file_status(
    conn = conn,
    file_id = file_info$file_id,
    global_status = "QUARANTAINE_NOMMAGE",
    last_step_code = "QC01",
    last_step_status = "KO",
    motif_ko = reason,
    action_attendue = "Corriger le nom du fichier puis redéposer un fichier conforme."
  )

  log_event_db(
    conn = conn,
    ingestion_id = ingestion_id,
    file_id = file_info$file_id,
    step_code = "QC01",
    action_type = "QUARANTAINE",
    status = "KO",
    message = reason,
    details = list(
      source_path = file_info$source_path,
      quarantine_path = target_path
    )
  )

  log_warn(log_file, sprintf("Fichier %s placé en quarantaine de nommage.", file_info$source_filename))

  invisible(target_path)
}
