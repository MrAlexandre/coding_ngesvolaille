# =====================================================================
# run_ingestion.R
# ---------------------------------------------------------------------
# Script principal V1 du micro-lot d'ingestion.
#
# Ce script peut être appelé :
# - manuellement ;
# - via le Planificateur de tâches Windows ;
# - via une commande `Rscript.exe`.
#
# Il traite les étapes suivantes :
# 1. chargement packages et modules
# 2. chargement configuration
# 3. création/connexion registre
# 4. détection des nouveaux fichiers
# 5. enregistrement dans le registre
# 6. QC01 / QC02 (nommage + parsing)
# 7. quarantaine si besoin
# 8. archivage RAW si activé
# =====================================================================

# ---------------------------------------------------------------------
# 1. Détermination du dossier racine du projet
# ---------------------------------------------------------------------
args <- commandArgs(trailingOnly = FALSE)
file_arg <- grep("^--file=", args, value = TRUE)
script_path <- normalizePath(sub("^--file=", "", file_arg))
project_root <- normalizePath(file.path(dirname(script_path), ".."))

# On force le répertoire de travail à la racine du projet afin que les
# chemins relatifs restent stables, que l'appel soit manuel ou planifié.
setwd(project_root)

# ---------------------------------------------------------------------
# 2. Chargement des modules
# ---------------------------------------------------------------------
source("R/00_packages.R")
load_required_packages()

source("R/utils_config.R")
source("R/utils_time.R")
source("R/utils_paths.R")
source("R/utils_log.R")
source("R/utils_hash.R")
source("R/utils_io.R")
source("R/ref_naming.R")
source("R/registry_db.R")
source("R/registry_runs.R")
source("R/registry_suivi_depots.R")
source("R/registry_journal_etapes.R")
source("R/process_detect_new_files.R")
source("R/process_quarantine_naming.R")
source("R/process_archive_raw.R")

# ---------------------------------------------------------------------
# 3. Fonction de traitement d'un fichier
# ---------------------------------------------------------------------
process_one_file <- function(conn, file_info, ingestion_id, config, log_file) {
  files_failed <- 0L

  register_new_file(conn, file_info, ingestion_id, global_status = "DETECTE")

  log_event_db(
    conn = conn,
    ingestion_id = ingestion_id,
    file_id = file_info$file_id,
    step_code = "DETECT",
    action_type = "DETECTION",
    status = "OK",
    message = "Nouveau fichier détecté.",
    details = list(
      source_filename = file_info$source_filename,
      source_path = file_info$source_path,
      partner = file_info$partner
    )
  )

  # ------------------------------
  # QC01 : nommage conforme
  # ------------------------------
  naming_check <- validate_filename(file_info$source_filename, config$naming$regex)

  if (identical(naming_check$status, "KO")) {
    quarantine_bad_filename(
      conn = conn,
      file_info = file_info,
      ingestion_id = ingestion_id,
      config = config,
      log_file = log_file,
      reason = naming_check$message
    )
    return(list(processed = 0L, failed = 1L))
  }

  log_event_db(
    conn = conn,
    ingestion_id = ingestion_id,
    file_id = file_info$file_id,
    step_code = "QC01",
    action_type = "CONTROLE",
    status = "OK",
    message = naming_check$message,
    details = naming_check$details
  )

  # ------------------------------
  # QC02 : parsing du nom
  # ------------------------------
  parsed <- parse_filename(file_info$source_filename, config$naming$regex)

  if (identical(parsed$status, "KO")) {
    quarantine_bad_filename(
      conn = conn,
      file_info = file_info,
      ingestion_id = ingestion_id,
      config = config,
      log_file = log_file,
      reason = parsed$message
    )
    return(list(processed = 0L, failed = 1L))
  }

  update_file_metadata_from_name(conn, file_info$file_id, parsed$data)

  log_event_db(
    conn = conn,
    ingestion_id = ingestion_id,
    file_id = file_info$file_id,
    step_code = "QC02",
    action_type = "CONTROLE",
    status = "OK",
    message = parsed$message,
    details = parsed$details
  )

  update_file_status(
    conn = conn,
    file_id = file_info$file_id,
    global_status = "IDENTIFIE",
    last_step_code = "QC02",
    last_step_status = "OK"
  )

  # ------------------------------
  # Archivage RAW si activé
  # ------------------------------
  if (isTRUE(config$features$archive_raw_enabled)) {
    archive_raw_file(
      conn = conn,
      file_info = file_info,
      ingestion_id = ingestion_id,
      config = config,
      log_file = log_file
    )

    update_file_status(
      conn = conn,
      file_id = file_info$file_id,
      global_status = "ARCHIVE_RAW",
      last_step_code = "RAW",
      last_step_status = "OK"
    )
  }

  list(processed = 1L, failed = 0L)
}

# ---------------------------------------------------------------------
# 4. Pipeline principal
# ---------------------------------------------------------------------
run_ingestion_pipeline <- function() {
  config <- load_config("config/app.yml")
  ensure_project_directories(config)

  ingestion_id <- generate_ingestion_id(prefix = config$ingestion$id_prefix)
  log_file <- build_log_path(config, ingestion_id, suffix = "ingestion")

  log_info(log_file, sprintf("Démarrage du run %s", ingestion_id))

  conn <- connect_registry(config$paths$registry_db)
  on.exit(DBI::dbDisconnect(conn), add = TRUE)

  initialize_registry(conn)
  create_run_record(conn, ingestion_id, run_type = "ingestion")

  new_files <- detect_new_files(conn, config, log_file)

  files_detected <- length(new_files)
  files_processed <- 0L
  files_failed <- 0L

  if (files_detected == 0) {
    close_run_record(
      conn = conn,
      ingestion_id = ingestion_id,
      status = "OK",
      files_detected = 0L,
      files_processed = 0L,
      files_failed = 0L,
      comments = "Aucun nouveau fichier."
    )

    log_info(log_file, "Fin du run : aucun nouveau fichier.")
    return(invisible(TRUE))
  }

  for (file_info in new_files) {
    # Chaque fichier est protégé par son propre tryCatch.
    # Ainsi, un fichier en erreur ne bloque pas le micro-lot complet.
    one_result <- tryCatch(
      {
        process_one_file(conn, file_info, ingestion_id, config, log_file)
      },
      error = function(e) {
        log_error(log_file, sprintf("Erreur sur le fichier %s : %s", file_info$source_filename, e$message))

        update_file_status(
          conn = conn,
          file_id = file_info$file_id,
          global_status = "ERREUR_TECHNIQUE",
          last_step_code = "RUN",
          last_step_status = "KO",
          motif_ko = e$message,
          action_attendue = "Analyser le log technique et corriger la cause."
        )

        log_event_db(
          conn = conn,
          ingestion_id = ingestion_id,
          file_id = file_info$file_id,
          step_code = "RUN",
          action_type = "ERREUR",
          status = "KO",
          message = e$message,
          details = list(source_filename = file_info$source_filename)
        )

        list(processed = 0L, failed = 1L)
      }
    )

    files_processed <- files_processed + one_result$processed
    files_failed <- files_failed + one_result$failed
  }

  final_status <- if (files_failed == 0L) "OK" else "WARN"

  close_run_record(
    conn = conn,
    ingestion_id = ingestion_id,
    status = final_status,
    files_detected = files_detected,
    files_processed = files_processed,
    files_failed = files_failed,
    comments = "Run terminé."
  )

  log_info(
    log_file,
    sprintf(
      "Fin du run %s | détectés=%s | traités=%s | en échec=%s",
      ingestion_id, files_detected, files_processed, files_failed
    )
  )

  invisible(TRUE)
}

# ---------------------------------------------------------------------
# 5. Point d'entrée du script
# ---------------------------------------------------------------------
main <- function() {
  tryCatch(
    {
      run_ingestion_pipeline()
      quit(save = "no", status = 0)
    },
    error = function(e) {
      message("ERREUR FATALE DU RUN : ", e$message)
      quit(save = "no", status = 1)
    }
  )
}

main()
