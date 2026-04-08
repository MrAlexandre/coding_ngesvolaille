# ==========================================================
# ==========================================================
# 
# Interractions avec base de donnée Ingestion
# 
# ==========================================================
# ==========================================================


# ==========================================================
library(DBI)
library(RSQLite)

# Connexion à la base
con <- dbConnect(
  SQLite(),
  "C:/Users/alexandre.martin/OneDrive - SOBAC/Projet NGESvolaille - Documents/04_Données/01_Zootech. Dépot/test/registry/registry.sqlite"
)

dbListTables(con)
# ----------------------------------------------------------


# ==========================================================
# Lecture de la base de donnée sur ingestion
# ----------------------------------------------------------
# Lecture du journal
journals <- dbReadTable(con, "journal_etapes")
journals

# Lecture des runs
runs <- dbReadTable(con, "runs")
runs

# Lecture du suivi
suivi <- dbReadTable(con, "suivi_depots")
suivi

# Suppression de l’historique des runs
dbExecute(con, "DELETE FROM runs;")

# Vérification
dbGetQuery(con, "SELECT COUNT(*) FROM runs;")

# Fermeture
dbDisconnect(con)


# ==========================================================
# Interventions sur la base de donnée sur ingestion -> les runs
# ----------------------------------------------------------


# connexion 
con <- dbConnect(
  SQLite(),
  "C:/Users/alexandre.martin/OneDrive - SOBAC/Projet NGESvolaille - Documents/04_Données/01_Zootech. Dépot/test/registry/registry.sqlite"
)

# Vérifier avant suppression
dbGetQuery(con, "SELECT COUNT(*) AS n_runs FROM runs;")

# Supprimer les lignes de la table runs
dbExecute(con, "DELETE FROM runs;")

# Vérification de l'existance de la table malgès la suppression du contenu 
dbListTables(con, "DELETE FROM runs;")


# ==========================================================
# Interventions sur la base de donnée sur ingestion -> le journal
# ----------------------------------------------------------

# connexion 
con <- dbConnect(
  SQLite(),
  "C:/Users/alexandre.martin/OneDrive - SOBAC/Projet NGESvolaille - Documents/04_Données/01_Zootech. Dépot/test/registry/registry.sqlite"
)

# exploration 
dbGetQuery(con, "
  SELECT *
  FROM journal_etapes
  ORDER BY event_time DESC
  LIMIT 1;
")

# Vérifier avant
dbGetQuery(con, "SELECT COUNT(*) AS n_events FROM journal_etapes;")

# Supprimer toutes les lignes
dbExecute(con, "DELETE FROM journal_etapes;")

# Vérifier après
dbGetQuery(con, "SELECT COUNT(*) AS n_events FROM journal_etapes;")

# Vérifier structure
dbListTables(con)

# Fermer
dbDisconnect(con)


# ==========================================================
# Interventions sur la base de donnée sur ingestion -> le suivi_depots
# ----------------------------------------------------------

# connexion 
con <- dbConnect(
  SQLite(),
  "C:/Users/alexandre.martin/OneDrive - SOBAC/Projet NGESvolaille - Documents/04_Données/01_Zootech. Dépot/test/registry/registry.sqlite"
)

# Compter le nombre de lignes
dbGetQuery(con, "SELECT COUNT(*) AS n_files FROM suivi_depots;")

# Voir quelques lignes
dbGetQuery(con, "
  SELECT *
  FROM suivi_depots
  LIMIT 1;
")

# Voir les colonnes
suivi <- dbReadTable(con, "suivi_depots")
names(suivi)
suivi

# Supprimer proprement le contenu de suivi_depots
dbExecute(con, "DELETE FROM suivi_depots;")

# Vérification après suppression
dbGetQuery(con, "SELECT COUNT(*) AS n_files FROM suivi_depots;")

# Vérifiation que la table existe toujours
dbListTables(con)

# Fermer la connexion
dbDisconnect(con)

# ==========================================================
# Vider les logs fichier
# ----------------------------------------------------------

# Le pipeline garde une mémoire dans fichiers .log.
# De vieux fichiers .log peuvent induire en erreur si pas supprimé suite aux tests

# ce qu’il y a dans le dossier
logs_dir <- "C:/Users/alexandre.martin/OneDrive - SOBAC/Projet NGESvolaille - Documents/04_Données/01_Zootech. Dépot/test/logs"

list.files(logs_dir) # list.files(logs_dir, full.names = TRUE) pour afficher chemins complets

# voir ce qu’il y a avant suppression :
log_files <- list.files(logs_dir, full.names = F)
log_files
length(log_files)

# Supprimer les fichiers .log. Sélectionne spécifiquement les fichiers avec extension ".log"
log_files <- list.files(logs_dir, pattern = "\\.log$", full.names = TRUE)
file.remove(log_files)

# Vérifier après suppression
list.files(logs_dir)
