# =====================================================================
# registry_db.R
# ---------------------------------------------------------------------
# Initialisation et connexion de la base SQLite.
# La base porte l'état opérationnel du système.
# =====================================================================

connect_registry <- function(db_path) {
  fs::dir_create(dirname(db_path), recurse = TRUE)
  DBI::dbConnect(RSQLite::SQLite(), db_path)
}

initialize_registry <- function(conn) {
  # Table de suivi principal des fichiers.
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS suivi_depots (
      file_id TEXT PRIMARY KEY,
      partner TEXT,
      source_filename TEXT,
      source_path TEXT,
      source_extension TEXT,
      file_size_bytes INTEGER,
      sha256 TEXT,
      site TEXT,
      batiment TEXT,
      date_reference TEXT,
      file_type TEXT,
      version_num INTEGER,
      first_seen_at TEXT,
      detected_at TEXT,
      ingested_at TEXT,
      archived_at TEXT,
      qualified_at TEXT,
      promoted_at TEXT,
      ingestion_id TEXT,
      global_status TEXT,
      last_step_code TEXT,
      last_step_status TEXT,
      motif_ko TEXT,
      action_attendue TEXT,
      archive_path TEXT,
      qualified_path TEXT,
      comments TEXT
    )
  ")

  # Journal append-only : une ligne par événement.
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS journal_etapes (
      event_id TEXT PRIMARY KEY,
      ingestion_id TEXT,
      file_id TEXT,
      step_code TEXT,
      action_type TEXT,
      status TEXT,
      event_time TEXT,
      message TEXT,
      details_json TEXT
    )
  ")

  # Table des runs pour suivre chaque micro-lot.
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS runs (
      ingestion_id TEXT PRIMARY KEY,
      run_type TEXT,
      started_at TEXT,
      ended_at TEXT,
      status TEXT,
      files_detected INTEGER,
      files_processed INTEGER,
      files_failed INTEGER,
      comments TEXT
    )
  ")

  invisible(TRUE)
}
