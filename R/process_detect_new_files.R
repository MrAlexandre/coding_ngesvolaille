# =====================================================================

# process_detect_new_files.R

# ---------------------------------------------------------------------

# Détection des fichiers nouveaux présents dans les dépôts partenaires.

# Le principe en V1 :

# - scan récursif du dossier de dépôt ;

# - exclusion des dossiers ;

# - exclusion des fichiers techniques du pipeline ;

# - comparaison au registre sur `source_path`.

# =====================================================================

list_dropzone_files <- function(drop_root) {
if (!dir.exists(drop_root)) {
warning(
sprintf("Le dossier de dépôt n'existe pas encore : %s", drop_root),
call. = FALSE
)
return(character(0))
}

files <- fs::dir_ls(drop_root, recurse = TRUE, type = "file")

if (length(files) == 0) {
return(character(0))
}

normalizePath(files, winslash = "/", mustWork = FALSE)
}

extract_partner_from_path <- function(file_path, drop_root) {
rel_path <- fs::path_rel(file_path, start = drop_root)
parts <- strsplit(rel_path, .Platform$file.sep, fixed = TRUE)[[1]]

# Convention simple : le premier dossier sous drop_root représente le partenaire.

if (length(parts) >= 2) {
return(parts[1])
}

"INCONNU"
}

build_file_info <- function(file_path, config) {
file_size <- file.info(file_path)$size

if (length(file_size) != 1 || is.na(file_size)) {
stop(
sprintf(
"Impossible de construire file_info pour '%s' : taille de fichier introuvable ou invalide.",
file_path
),
call. = FALSE
)
}

file_id <- generate_file_id()

list(
file_id = file_id,
partner = extract_partner_from_path(file_path, config$paths$drop_root),
source_filename = basename(file_path),
source_path = normalizePath(file_path, winslash = "/", mustWork = FALSE),
extension = get_file_extension(file_path),
file_size_bytes = file_size,
detected_at = get_now_paris()
)
}

is_internal_pipeline_file <- function(file_path, config) {
normalized_file <- normalizePath(file_path, winslash = "/", mustWork = FALSE)

internal_paths <- c(
config$paths$registry_db,
config$paths$logs_root
)

internal_paths <- internal_paths[!is.na(internal_paths) & nzchar(internal_paths)]
internal_paths <- normalizePath(internal_paths, winslash = "/", mustWork = FALSE)

file_name <- basename(normalized_file)
extension <- tolower(tools::file_ext(normalized_file))

# Cas 1 : le fichier est exactement la base SQLite du registre

if (normalized_file %in% internal_paths) {
return(TRUE)
}

# Cas 2 : le fichier est situé dans le dossier de logs du pipeline

logs_root <- normalizePath(config$paths$logs_root, winslash = "/", mustWork = FALSE)
if (startsWith(normalized_file, logs_root)) {
return(TRUE)
}

# Cas 3 : on exclut explicitement certains fichiers techniques fréquents

if (tolower(file_name) == "registry.sqlite") {
return(TRUE)
}

if (extension %in% c("log", "sqlite")) {
return(TRUE)
}

FALSE
}

detect_new_files <- function(conn, config, log_file) {
all_files <- list_dropzone_files(config$paths$drop_root)

if (length(all_files) == 0) {
log_info(log_file, "Aucun fichier trouvé dans le dossier de dépôt.")
return(list())
}

log_info(log_file, sprintf("%s fichier(s) trouvé(s) dans le dépôt.", length(all_files)))

candidate_files <- all_files[!vapply(
all_files,
function(x) is_internal_pipeline_file(x, config),
logical(1)
)]

excluded_count <- length(all_files) - length(candidate_files)
if (excluded_count > 0) {
log_info(
log_file,
sprintf(
"%s fichier(s) technique(s) interne(s) exclu(s) de la détection.",
excluded_count
)
)
}

new_files <- list()

for (file_path in candidate_files) {
normalized_path <- normalizePath(file_path, winslash = "/", mustWork = FALSE)

if (!is_already_registered(conn, normalized_path)) {
  new_files[[length(new_files) + 1]] <- build_file_info(normalized_path, config)
}

}

log_info(log_file, sprintf("%s nouveau(x) fichier(s) détecté(s).", length(new_files)))
new_files
}
