# =========================================================
# test_qc03_technical.R
# NGESvolaille - Test manuel QC03
# =========================================================

# ---------------------------------------------------------
# Chargement des packages
# ---------------------------------------------------------
library(yaml)
library(digest)
library(readxl)
library(jsonlite)

# ---------------------------------------------------------
# Chargement des scripts
# Hypothèse :
# le script est lancé depuis la racine _Coding_NGESvolaille
# ---------------------------------------------------------
source("R/qc_technical_checks.R")
source("R/process_qc03_technical.R")

# ---------------------------------------------------------
# Chargement de la configuration
# ---------------------------------------------------------
config <- yaml::read_yaml("config/app.yml")

# ---------------------------------------------------------
# Paramètres de test
# Adapter file_path selon le fichier que vous voulez tester
# ---------------------------------------------------------
ingestion_id <- paste0(
  config$ingestion$id_prefix,
  "_",
  format(Sys.time(), "%Y%m%d_%H%M")
)

file_path <- "C:/Users/alexandre.martin/OneDrive - SOBAC/Projet NGESvolaille - Documents/_Coding_NGESvolaille/données pour test/qc03/qc03_ko_csv_header_only.csv"

# ---------------------------------------------------------
# Exécution du process QC03
# ---------------------------------------------------------
result <- process_qc03_technical(
  file_path = file_path,
  ingestion_id = ingestion_id,
  config = config
)

# ---------------------------------------------------------
# Affichage synthétique
# ---------------------------------------------------------
cat("\n=====================================\n")
cat("RESULTAT GLOBAL QC03\n")
cat("=====================================\n")
cat("Fichier        :", result$file_path, "\n")
cat("Ingestion ID   :", result$ingestion_id, "\n")
cat("Statut QC03    :", result$status_qc03, "\n")
cat("Action         :", result$redirect_action, "\n")
cat("SHA-256        :", result$sha256, "\n")
cat("Manifest écrit :", result$manifest_written, "\n")
cat("Manifest path  :", result$manifest_output_path, "\n")
cat("Quarantaine    :", result$quarantine_path, "\n")
cat("Fichier déplacé:", result$file_moved, "\n")
cat("Message        :", result$message, "\n")

# ---------------------------------------------------------
# Affichage détaillé des sous-contrôles
# ---------------------------------------------------------
cat("\n=====================================\n")
cat("DETAIL DES SOUS-CONTROLES\n")
cat("=====================================\n")

for (subcheck in result$subchecks) {
  cat(
    subcheck$code, " | ",
    subcheck$status, " | ",
    subcheck$message, "\n",
    sep = ""
  )
}

# ---------------------------------------------------------
# Affichage du manifest technique si disponible
# ---------------------------------------------------------
cat("\n=====================================\n")
cat("MANIFEST TECHNIQUE\n")
cat("=====================================\n")

if (!is.null(result$manifest)) {
  print(result$manifest)
} else {
  cat("Aucun manifest disponible.\n")
}