# =====================================================================
# utils_hash.R
# ---------------------------------------------------------------------
# Calcul de l'empreinte SHA-256 d'un fichier.
# Sert à la traçabilité, à l'intégrité et à la détection robuste.
# =====================================================================

compute_sha256 <- function(file_path) {
  if (!file.exists(file_path)) {
    stop(sprintf("Impossible de calculer le hash. Fichier introuvable : %s", file_path), call. = FALSE)
  }

  digest::digest(file = file_path, algo = "sha256", serialize = FALSE)
}
