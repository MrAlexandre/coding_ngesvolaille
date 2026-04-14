# =========================================================
# qc_technical_checks.R
# NGESvolaille - Pipeline V1
# QC03 : contrôle technique agrégé
# Sous-contrôles :
# - QC10 : format autorisé
# - QC11 : contenu minimal
# - QC12 : lisibilité technique
# - QC13 : calcul SHA-256
# - QC14 : génération du manifest technique
# =========================================================

# Dépendances :
# library(digest)
# library(readxl)
# library(jsonlite)

# ---------------------------------------------------------
# Fonction : créer résultat standardisé
# ---------------------------------------------------------
build_qc_result <- function(code, status, message, details = list()) {
  list(
    code = code,
    status = status,
    message = message,
    details = details
  )
}

# ---------------------------------------------------------
# Fonction : extension normalisée
# ---------------------------------------------------------
get_file_extension_normalized <- function(file_path) {
  tolower(tools::file_ext(file_path))
}

# ---------------------------------------------------------
# Fonction : lecture de lignes texte
# ---------------------------------------------------------
safe_read_lines <- function(file_path, encodings = c("UTF-8", "latin1")) {
  for (enc in encodings) {
    res <- tryCatch({
      lines <- readLines(file_path, warn = FALSE, encoding = enc)
      list(success = TRUE, lines = lines, encoding = enc, error = NULL)
    }, error = function(e) {
      list(success = FALSE, lines = NULL, encoding = enc, error = e$message)
    })

    if (isTRUE(res$success)) {
      return(res)
    }
  }

  list(
    success = FALSE,
    lines = NULL,
    encoding = NA_character_,
    error = "Impossible de lire le fichier avec les encodages testés."
  )
}

# ---------------------------------------------------------
# QC10 - Vérification du format autorisé
# Source de configuration : config$allowed_extensions
# ---------------------------------------------------------
check_qc10_allowed_format <- function(file_path, allowed_extensions) {
  ext <- get_file_extension_normalized(file_path)
  allowed_extensions <- tolower(allowed_extensions)

  if (!nzchar(ext)) {
    return(build_qc_result(
      code = "QC10",
      status = "KO",
      message = "Extension absente ou non détectable.",
      details = list(
        file_path = file_path,
        extension = ext,
        allowed_extensions = allowed_extensions
      )
    ))
  }

  if (!(ext %in% allowed_extensions)) {
    return(build_qc_result(
      code = "QC10",
      status = "KO",
      message = "Format non autorisé.",
      details = list(
        file_path = file_path,
        extension = ext,
        allowed_extensions = allowed_extensions
      )
    ))
  }

  build_qc_result(
    code = "QC10",
    status = "OK",
    message = "Format autorisé.",
    details = list(
      file_path = file_path,
      extension = ext,
      allowed_extensions = allowed_extensions
    )
  )
}

# ---------------------------------------------------------
# Fonction CSV : contenu minimal
# Règle V1 :
# - au moins 2 lignes non vides :
#   1 en-tête + 1 ligne de données
# ---------------------------------------------------------
has_minimal_content_csv <- function(file_path) {
  lines_result <- safe_read_lines(file_path)

  if (!isTRUE(lines_result$success)) {
    return(list(
      ok = FALSE,
      details = list(
        reason = "read_error",
        error = lines_result$error
      )
    ))
  }

  lines <- lines_result$lines
  non_empty_lines <- lines[nzchar(trimws(lines))]

  ok <- length(non_empty_lines) >= 2

  list(
    ok = ok,
    details = list(
      total_lines = length(lines),
      non_empty_lines = length(non_empty_lines),
      encoding_used = lines_result$encoding
    )
  )
}

# ---------------------------------------------------------
# Fonction XLSX : contenu minimal
# Règle V1 :
# - au moins une feuille accessible
# - au moins une feuille contenant au moins 1 ligne de données
# ---------------------------------------------------------
has_minimal_content_xlsx <- function(file_path) {
  sheets <- tryCatch(
    readxl::excel_sheets(file_path),
    error = function(e) NULL
  )

  if (is.null(sheets) || length(sheets) == 0) {
    return(list(
      ok = FALSE,
      details = list(
        reason = "no_sheet"
      )
    ))
  }

  for (sheet_name in sheets) {
    df <- tryCatch(
      readxl::read_excel(
        path = file_path,
        sheet = sheet_name,
        col_names = TRUE
      ),
      error = function(e) NULL
    )

    if (is.null(df)) {
      next
    }

    if (nrow(df) > 0) {
      return(list(
        ok = TRUE,
        details = list(
          sheet_name = sheet_name,
          nrow = nrow(df),
          ncol = ncol(df)
        )
      ))
    }
  }

  list(
    ok = FALSE,
    details = list(
      reason = "no_data_row",
      sheets = sheets
    )
  )
}

# ---------------------------------------------------------
# QC11 - Vérification du contenu minimal
# ---------------------------------------------------------
check_qc11_minimal_content <- function(file_path) {
  if (!file.exists(file_path)) {
    return(build_qc_result(
      code = "QC11",
      status = "KO",
      message = "Fichier introuvable.",
      details = list(file_path = file_path)
    ))
  }

  info <- file.info(file_path)
  ext <- get_file_extension_normalized(file_path)

  if (nrow(info) == 0 || is.na(info$size) || info$size <= 0) {
    return(build_qc_result(
      code = "QC11",
      status = "KO",
      message = "Fichier vide ou taille nulle.",
      details = list(
        file_path = file_path,
        size_bytes = ifelse(nrow(info) == 0, NA, info$size),
        extension = ext
      )
    ))
  }

  content_result <- tryCatch({
    if (ext == "csv") {
      has_minimal_content_csv(file_path)
    } else if (ext == "xlsx") {
      has_minimal_content_xlsx(file_path)
    } else {
      list(
        ok = FALSE,
        details = list(reason = "unsupported_extension_for_qc11")
      )
    }
  }, error = function(e) {
    list(
      ok = FALSE,
      details = list(
        reason = "qc11_error",
        error = e$message
      )
    )
  })

  if (!isTRUE(content_result$ok)) {
    return(build_qc_result(
      code = "QC11",
      status = "KO",
      message = "Absence de contenu minimal exploitable.",
      details = c(
        list(
          file_path = file_path,
          extension = ext,
          size_bytes = info$size
        ),
        content_result$details
      )
    ))
  }

  build_qc_result(
    code = "QC11",
    status = "OK",
    message = "Contenu minimal détecté.",
    details = c(
      list(
        file_path = file_path,
        extension = ext,
        size_bytes = info$size
      ),
      content_result$details
    )
  )
}

# ---------------------------------------------------------
# Fonction CSV : lisibilité technique
# Essaie plusieurs séparateurs et encodages
# ---------------------------------------------------------
check_csv_readability <- function(file_path) {
  separators <- c(",", ";", "\t")
  encodings <- c("UTF-8", "latin1")

  attempts <- list()

  for (enc in encodings) {
    for (sep in separators) {
      test <- tryCatch({
        df <- utils::read.table(
          file = file_path,
          header = TRUE,
          sep = sep,
          quote = "\"",
          comment.char = "",
          fileEncoding = enc,
          fill = TRUE,
          nrows = 5,
          stringsAsFactors = FALSE
        )

        list(
          success = TRUE,
          encoding = enc,
          separator = sep,
          nrow = nrow(df),
          ncol = ncol(df),
          error = NULL
        )
      }, error = function(e) {
        list(
          success = FALSE,
          encoding = enc,
          separator = sep,
          nrow = NA_integer_,
          ncol = NA_integer_,
          error = e$message
        )
      })

      attempts[[length(attempts) + 1]] <- test

      if (isTRUE(test$success)) {
        return(list(
          ok = TRUE,
          details = list(
            encoding_used = enc,
            separator_used = sep,
            nrow_preview = test$nrow,
            ncol_preview = test$ncol,
            attempts = attempts
          )
        ))
      }
    }
  }

  list(
    ok = FALSE,
    details = list(
      attempts = attempts
    )
  )
}

# ---------------------------------------------------------
# Fonction utilitaire XLSX : lisibilité technique
# ---------------------------------------------------------
check_xlsx_readability <- function(file_path) {
  sheets <- tryCatch(
    readxl::excel_sheets(file_path),
    error = function(e) NULL
  )

  if (is.null(sheets) || length(sheets) == 0) {
    return(list(
      ok = FALSE,
      details = list(
        reason = "xlsx_open_failed_or_no_sheet"
      )
    ))
  }

  for (sheet_name in sheets) {
    test <- tryCatch({
      df <- readxl::read_excel(
        path = file_path,
        sheet = sheet_name,
        n_max = 5
      )

      list(
        success = TRUE,
        sheet_name = sheet_name,
        nrow = nrow(df),
        ncol = ncol(df),
        error = NULL
      )
    }, error = function(e) {
      list(
        success = FALSE,
        sheet_name = sheet_name,
        nrow = NA_integer_,
        ncol = NA_integer_,
        error = e$message
      )
    })

    if (isTRUE(test$success)) {
      return(list(
        ok = TRUE,
        details = list(
          sheet_name = test$sheet_name,
          nrow_preview = test$nrow,
          ncol_preview = test$ncol
        )
      ))
    }
  }

  list(
    ok = FALSE,
    details = list(
      reason = "xlsx_sheet_read_failed",
      sheets = sheets
    )
  )
}

# ---------------------------------------------------------
# QC12 - Vérification de lisibilité
# ---------------------------------------------------------
check_qc12_readability <- function(file_path) {
  ext <- get_file_extension_normalized(file_path)

  read_result <- tryCatch({
    if (ext == "csv") {
      check_csv_readability(file_path)
    } else if (ext == "xlsx") {
      check_xlsx_readability(file_path)
    } else {
      list(
        ok = FALSE,
        details = list(reason = "unsupported_extension_for_qc12")
      )
    }
  }, error = function(e) {
    list(
      ok = FALSE,
      details = list(
        reason = "qc12_error",
        error = e$message
      )
    )
  })

  if (!isTRUE(read_result$ok)) {
    return(build_qc_result(
      code = "QC12",
      status = "KO",
      message = "Lecture technique impossible.",
      details = c(
        list(
          file_path = file_path,
          extension = ext
        ),
        read_result$details
      )
    ))
  }

  build_qc_result(
    code = "QC12",
    status = "OK",
    message = "Lecture technique possible.",
    details = c(
      list(
        file_path = file_path,
        extension = ext
      ),
      read_result$details
    )
  )
}

# ---------------------------------------------------------
# QC13 - Calcul de l'empreinte SHA-256
# ---------------------------------------------------------
check_qc13_sha256 <- function(file_path) {
  result <- tryCatch({
    sha256_value <- digest::digest(
      file = file_path,
      algo = "sha256",
      serialize = FALSE
    )

    list(
      code = "QC13",
      status = "OK",
      message = "Empreinte SHA-256 calculée.",
      details = list(
        file_path = file_path,
        sha256 = sha256_value
      ),
      sha256 = sha256_value
    )
  }, error = function(e) {
    list(
      code = "QC13",
      status = "KO",
      message = paste0("Impossible de calculer le SHA-256 : ", e$message),
      details = list(
        file_path = file_path,
        error = e$message
      ),
      sha256 = NA_character_
    )
  })

  result
}

# ---------------------------------------------------------
# QC14 - Génération du manifest technique
# Si config$manifests$output_dir existe, écriture JSON
# Sinon, retour en mémoire uniquement
# ---------------------------------------------------------
check_qc14_manifest <- function(file_path, ingestion_id, sha256, config = NULL) {
  result <- tryCatch({
    info <- file.info(file_path)
    ext <- get_file_extension_normalized(file_path)

    manifest <- list(
      ingestion_id = ingestion_id,
      file_name = basename(file_path),
      file_basename = tools::file_path_sans_ext(basename(file_path)),
      file_path = normalizePath(file_path, winslash = "/", mustWork = FALSE),
      file_extension = ext,
      file_size_bytes = unname(info$size),
      file_mtime = as.character(info$mtime),
      sha256 = sha256,
      quarantine_reason = NA_character_,
      partner_name = NA_character_,
      detected_at = as.character(Sys.time()),
      source_root = if (!is.null(config$paths$drop_root)) config$paths$drop_root else NA_character_,
      manifest_timestamp = as.character(Sys.time())
    )

    manifest_written <- FALSE
    manifest_output_path <- NA_character_

    if (!is.null(config) &&
        !is.null(config$manifests) &&
        !is.null(config$manifests$output_dir) &&
        nzchar(config$manifests$output_dir)) {

      output_dir <- config$manifests$output_dir
      dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

      manifest_filename <- paste0(
        tools::file_path_sans_ext(basename(file_path)),
        "_",
        ingestion_id,
        "_manifest.json"
      )

      manifest_output_path <- file.path(output_dir, manifest_filename)

      jsonlite::write_json(
        x = manifest,
        path = manifest_output_path,
        pretty = TRUE,
        auto_unbox = TRUE,
        null = "null"
      )

      manifest_written <- TRUE
    }

    list(
      code = "QC14",
      status = "OK",
      message = if (manifest_written) {
        "Manifest technique généré et écrit sur disque."
      } else {
        "Manifest technique généré en mémoire."
      },
      details = list(
        file_path = file_path,
        manifest_output_path = manifest_output_path,
        manifest_written = manifest_written
      ),
      manifest = manifest,
      manifest_output_path = manifest_output_path,
      manifest_written = manifest_written
    )
  }, error = function(e) {
    list(
      code = "QC14",
      status = "KO",
      message = paste0("Impossible de générer le manifest technique : ", e$message),
      details = list(
        file_path = file_path,
        ingestion_id = ingestion_id,
        error = e$message
      ),
      manifest = NULL,
      manifest_output_path = NA_character_,
      manifest_written = FALSE
    )
  })

  result
}

# ---------------------------------------------------------
# Fonction : calcul du statut agrégé QC03
# ---------------------------------------------------------
compute_qc03_status <- function(subchecks, blocking_codes = c("QC10", "QC11", "QC12", "QC13", "QC14")) {
  statuses <- vapply(subchecks, function(x) x$status, character(1))
  codes <- vapply(subchecks, function(x) x$code, character(1))

  blocking_statuses <- statuses[codes %in% blocking_codes]

  if (any(blocking_statuses == "KO")) {
    return("KO")
  }

  if (any(statuses == "WARN")) {
    return("WARN")
  }

  "OK"
}

# ---------------------------------------------------------
# QC03 - Contrôle technique agrégé
# IMPORTANT :
# - allowed_extensions est lu dans config$allowed_extensions
# - QC14 utilise config$manifests$output_dir si disponible
# ---------------------------------------------------------
run_qc03_technical <- function(file_path, ingestion_id, config) {
  allowed_extensions <- config$allowed_extensions

  qc10 <- check_qc10_allowed_format(
    file_path = file_path,
    allowed_extensions = allowed_extensions
  )

  qc11 <- if (qc10$status == "OK") {
    check_qc11_minimal_content(file_path)
  } else {
    build_qc_result(
      code = "QC11",
      status = "NOT_RUN",
      message = "QC11 non exécuté car QC10 est KO.",
      details = list(file_path = file_path)
    )
  }

  qc12 <- if (qc11$status == "OK") {
    check_qc12_readability(file_path)
  } else {
    build_qc_result(
      code = "QC12",
      status = "NOT_RUN",
      message = "QC12 non exécuté car QC11 n'est pas OK.",
      details = list(file_path = file_path)
    )
  }

  qc13 <- if (qc12$status == "OK") {
    check_qc13_sha256(file_path)
  } else {
    list(
      code = "QC13",
      status = "NOT_RUN",
      message = "QC13 non exécuté car QC12 n'est pas OK.",
      details = list(file_path = file_path),
      sha256 = NA_character_
    )
  }

  qc14 <- if (!is.null(qc13$sha256) && !is.na(qc13$sha256)) {
    check_qc14_manifest(
      file_path = file_path,
      ingestion_id = ingestion_id,
      sha256 = qc13$sha256,
      config = config
    )
  } else {
    list(
      code = "QC14",
      status = "NOT_RUN",
      message = "QC14 non exécuté car le SHA-256 est indisponible.",
      details = list(
        file_path = file_path,
        ingestion_id = ingestion_id
      ),
      manifest = NULL,
      manifest_output_path = NA_character_,
      manifest_written = FALSE
    )
  }

  subchecks <- list(qc10, qc11, qc12, qc13, qc14)

  status_qc03 <- compute_qc03_status(subchecks)

  redirect_action <- if (status_qc03 == "KO") {
    "QUARANTINE_TECHNICAL"
  } else {
    "CONTINUE"
  }

  list(
    file_path = file_path,
    ingestion_id = ingestion_id,
    status_qc03 = status_qc03,
    redirect_action = redirect_action,
    sha256 = if (!is.null(qc13$sha256)) qc13$sha256 else NA_character_,
    manifest = if (!is.null(qc14$manifest)) qc14$manifest else NULL,
    manifest_output_path = if (!is.null(qc14$manifest_output_path)) qc14$manifest_output_path else NA_character_,
    manifest_written = if (!is.null(qc14$manifest_written)) qc14$manifest_written else FALSE,
    subchecks = subchecks,
    message = paste0("QC03 terminé avec statut ", status_qc03, ".")
  )
}