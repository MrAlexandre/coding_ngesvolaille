source("00_packages.R")
load_required_packages()

source("utils_time.R")
source("utils_log.R")
source("utils_paths.R")
source("utils_config.R")
source("utils_io.R")
source("utils_hash.R")

source("registry_db.R")
source("registry_runs.R")
source("registry_journal_etapes.R")
source("registry_suivi_depots.R")

source("ref_naming.R")
source("process_detect_new_files.R")
source("process_quarantine_naming.R")
source("process_archive_raw.R")

config <- load_config("config/app.yml")
ensure_project_directories(config)

conn <- connect_registry(config$paths$registry_db)
initialize_registry(conn)

ingestion_id <- generate_ingestion_id()
log_file <- build_log_path(config, ingestion_id)

create_run_record(conn, ingestion_id, run_type = "test_single_partner")

regex_pattern <- "^([A-Z]{3})_(\\d{8})_(B\\d{2})_([A-Z]+)_V(\\d+)\\.(csv|xlsx)$"

new_files <- detect_new_files(conn, config, log_file)

files_processed <- 0L
files_failed <- 0L

for (file_info in new_files) {

  register_new_file(conn, file_info, ingestion_id)

  qc01 <- validate_filename(file_info$source_filename, regex_pattern)

  if (qc01$status == "KO") {
    quarantine_bad_filename(
      conn = conn,
      file_info = file_info,
      ingestion_id = ingestion_id,
      config = config,
      log_file = log_file,
      reason = qc01$message
    )
    files_failed <- files_failed + 1L
    next
  }

  qc02 <- parse_filename(file_info$source_filename, regex_pattern)

  if (qc02$status == "KO") {
    update_file_status(
      conn = conn,
      file_id = file_info$file_id,
      global_status = "KO_PARSING",
      last_step_code = "QC02",
      last_step_status = "KO",
      motif_ko = qc02$message,
      action_attendue = "Corriger le nom du fichier puis redéposer."
    )

    log_event_db(
      conn = conn,
      ingestion_id = ingestion_id,
      file_id = file_info$file_id,
      step_code = "QC02",
      action_type = "CONTROLE_NOMMAGE",
      status = "KO",
      message = qc02$message,
      details = qc02$details
    )

    files_failed <- files_failed + 1L
    next
  }

  update_file_metadata_from_name(conn, file_info$file_id, qc02$data)

  log_event_db(
    conn = conn,
    ingestion_id = ingestion_id,
    file_id = file_info$file_id,
    step_code = "QC02",
    action_type = "PARSING",
    status = "OK",
    message = qc02$message,
    details = qc02$details
  )

  archive_raw_file(conn, file_info, ingestion_id, config, log_file)

  update_file_status(
    conn = conn,
    file_id = file_info$file_id,
    global_status = "RAW_ARCHIVE",
    last_step_code = "RAW",
    last_step_status = "OK"
  )

  files_processed <- files_processed + 1L
}

close_run_record(
  conn = conn,
  ingestion_id = ingestion_id,
  status = if (files_failed == 0) "OK" else "PARTIEL",
  files_detected = length(new_files),
  files_processed = files_processed,
  files_failed = files_failed,
  comments = "Test V1 single partner"
)

DBI::dbDisconnect(conn)