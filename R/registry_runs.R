# =====================================================================
# registry_runs.R
# ---------------------------------------------------------------------
# Traçabilité des micro-lots d'ingestion.
# =====================================================================

create_run_record <- function(conn, ingestion_id, run_type = "ingestion") {
  DBI::dbAppendTable(
    conn = conn,
    name = "runs",
    value = tibble::tibble(
      ingestion_id = ingestion_id,
      run_type = run_type,
      started_at = format_timestamp(),
      ended_at = NA_character_,
      status = "EN_COURS",
      files_detected = 0L,
      files_processed = 0L,
      files_failed = 0L,
      comments = NA_character_
    )
  )

  invisible(TRUE)
}

close_run_record <- function(conn, ingestion_id, status, files_detected, files_processed, files_failed, comments = NULL) {
  query <- "
    UPDATE runs
    SET ended_at = ?, status = ?, files_detected = ?, files_processed = ?, files_failed = ?, comments = ?
    WHERE ingestion_id = ?
  "

  DBI::dbExecute(
    conn, query,
    params = list(
      format_timestamp(),
      status,
      as.integer(files_detected),
      as.integer(files_processed),
      as.integer(files_failed),
      comments,
      ingestion_id
    )
  )

  invisible(TRUE)
}
