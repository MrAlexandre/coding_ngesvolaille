# =====================================================================
# utils_config.R
# --------------------------------------------------------------------
# Fonctions de chargement et de validation minimale de la configuration.
# Version corrigée avec gestion explicite des erreurs.

# =====================================================================
# ---------------------------------------------------------------------
# Recherche du fichier YAML 
find_config_file <- function(filename = "app.yml", max_depth = 10) {
  current_dir <- normalizePath(getwd(), winslash = "/", mustWork = TRUE)
  
  for (i in seq_len(max_depth)) {
    candidate <- file.path(current_dir, "config", filename)
    if (file.exists(candidate)) {
      return(candidate)
    }
    
    # remonter d’un niveau
    parent <- dirname(current_dir)
    
    # stop si on atteint la racine
    if (parent == current_dir) {
      break
    }
    
    current_dir <- parent
  }
  
  stop("Fichier de configuration introuvable dans l’arborescence.")
}
# ---------------------------------------------------------------------

# ---------------------------------------------------------------------
# Récupération du chemin du fichier YAML 
# ---------------------------------------------------------------------
get_config <- function() {
  config_path <- find_config_file("app.yml")
  load_config(config_path)
}
# ---------------------------------------------------------------------

# ---------------------------------------------------------------------
# Chargement de la configuration YAML
# ---------------------------------------------------------------------

load_config <- function(config_file = "config/app.yml") {

# Vérification 1 : le fichier existe

if (!file.exists(config_file)) {
stop(
sprintf("Fichier de configuration introuvable : %s", config_file),
call. = FALSE
)
}

# Vérification 2 : lecture du YAML avec gestion d'erreur

config <- tryCatch(
yaml::read_yaml(config_file),
error = function(e) {
stop(
sprintf("Erreur de lecture du fichier YAML (%s) : %s", config_file, e$message),
call. = FALSE
)
}
)

# Vérification 3 : validation de la structure
validate_config(config)
return(config)
}
# ---------------------------------------------------------------------
# Validation minimale de la structure du YAML
# --------------------------------------------------------------------
validate_config <- function(config) {
# Vérification des sections principales
required_top_level <- c(
"project",
"paths",
"ingestion",
"allowed_extensions",
"naming",
"features",
"versions"
)

missing_top_level <- setdiff(required_top_level, names(config))

if (length(missing_top_level) > 0) {
stop(
sprintf(
"Configuration invalide. Sections manquantes : %s",
paste(missing_top_level, collapse = ", ")
),
call. = FALSE
)
}

# Vérification des chemins obligatoires

required_paths <- c(
"drop_root",
"quarantine_root",
"processing_root",
"archive_root",
"analysis_root",
"logs_root",
"registry_db"
)

missing_paths <- setdiff(required_paths, names(config$paths))

if (length(missing_paths) > 0) {
stop(
sprintf(
"Configuration invalide. Chemins manquants : %s",
paste(missing_paths, collapse = ", ")
),
call. = FALSE
)
}

# Si tout est OK

invisible(TRUE)
}
