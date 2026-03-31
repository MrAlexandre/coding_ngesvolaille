# =====================================================================
# utils_io.R
# ---------------------------------------------------------------------
# Fonctions de lecture et d'écriture de fichiers.
# Dans la V1, elles sont simples mais déjà centralisées.
# =====================================================================

get_file_extension <- function(file_path) {
  tolower(fs::path_ext(file_path))
}

read_source_file <- function(file_path) {
  ext <- get_file_extension(file_path)

  if (ext == "csv") {
    return(readr::read_csv(file_path, show_col_types = FALSE))
  }

  if (ext == "xlsx") {
    return(readxl::read_excel(file_path))
  }

  stop(sprintf("Format non pris en charge : %s", ext), call. = FALSE)
}

safe_copy_file <- function(from, to, overwrite = FALSE) {
  if (!file.exists(from)) {
    stop(sprintf("Copie impossible. Source introuvable : %s", from), call. = FALSE)
  }

  target_dir <- dirname(to)
  fs::dir_create(target_dir, recurse = TRUE)

  ok <- file.copy(from = from, to = to, overwrite = overwrite, copy.mode = TRUE, copy.date = TRUE)

  if (!ok) {
    stop(sprintf("Échec de copie vers : %s", to), call. = FALSE)
  }

  invisible(to)
}
