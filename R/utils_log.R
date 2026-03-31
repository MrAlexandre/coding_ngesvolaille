# =====================================================================
# utils_log.R
# ---------------------------------------------------------------------
# Journal technique fichier + impression console.
# Ce log est distinct du journal métier stocké en base.
# =====================================================================

write_log_line <- function(log_file, level = "INFO", message) {
  timestamp <- format_timestamp()
  line <- sprintf("[%s] [%s] %s", timestamp, level, message)

  cat(line, "\n")
  write(line, file = log_file, append = TRUE)
}

log_info <- function(log_file, message) {
  write_log_line(log_file, "INFO", message)
}

log_warn <- function(log_file, message) {
  write_log_line(log_file, "WARN", message)
}

log_error <- function(log_file, message) {
  write_log_line(log_file, "ERROR", message)
}
