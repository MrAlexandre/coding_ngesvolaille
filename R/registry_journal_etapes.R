# =====================================================================
# registry_journal_etapes.R
# ---------------------------------------------------------------------
# Écriture du journal append-only. Toute étape importante doit être
# inscrite ici.
# =====================================================================

log_event_db <- function(conn, ingestion_id, file_id, step_code, action_type, status, message, details = list()) {
  DBI::dbAppendTable(
    conn = conn,
    name = "journal_etapes",
    value = tibble::tibble(
      event_id = generate_event_id(),
      ingestion_id = ingestion_id,
      file_id = file_id,
      step_code = step_code,
      action_type = action_type,
      status = status,
      event_time = format_timestamp(),
      message = message,
      details_json = jsonlite::toJSON(details, auto_unbox = TRUE, null = "null")
    )
  )

  invisible(TRUE)
}
