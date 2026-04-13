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

config <- get_config()

conn <- connect_registry(config$paths$registry_db)
initialize_registry(conn)

ingestion_id <- generate_ingestion_id()
log_file <- build_log_path(config, ingestion_id)

create_run_record(conn, ingestion_id, "test")

regex_pattern <- config$naming$regex

new_files <- detect_new_files(conn, config, log_file)

print(paste("Fichiers détectés :", length(new_files)))

for (file_info in new_files) {
  
  print(paste("Test fichier :", file_info$source_filename))
  
  register_new_file(conn, file_info, ingestion_id)
  
  qc01 <- validate_filename(file_info$source_filename, regex_pattern)
  print(qc01)
  
  if (qc01$status == "KO") {
    quarantine_bad_filename(
      conn = conn,
      file_info = file_info,
      ingestion_id = ingestion_id,
      config = config,
      log_file = log_file,
      reason = qc01$message
    )
    next
  }
  
  qc02 <- parse_filename(file_info$source_filename, regex_pattern)
  print(qc02)
  
  if (qc02$status == "KO") {
    update_file_status(
      conn = conn,
      file_id = file_info$file_id,
      global_status = "KO_PARSING",
      last_step_code = "QC02",
      last_step_status = "KO",
      motif_ko = qc02$message,
      action_attendue = "Corriger le nom du fichier puis redéposer un fichier conforme."
    )
    
    log_event_db(
      conn = conn,
      ingestion_id = ingestion_id,
      file_id = file_info$file_id,
      step_code = "QC02",
      action_type = "PARSING",
      status = "KO",
      message = qc02$message,
      details = qc02$details
    )
    
    next
  }
  
  update_file_metadata_from_name(
    conn = conn,
    file_id = file_info$file_id,
    parsed = qc02$data
  )
  
  print(">>> QC02 OK - mise à jour statut")
  
  update_file_status(
    conn = conn,
    file_id = file_info$file_id,
    global_status = "NOMMAGE_PARSE",
    last_step_code = "QC02",
    last_step_status = "OK"
  )
  
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
}

DBI::dbDisconnect(conn)