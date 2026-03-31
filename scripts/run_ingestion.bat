@echo off
REM ================================================================
REM run_ingestion.bat
REM ---------------------------------------------------------------
REM Exemple d'appel Windows du script d'ingestion.
REM Adapter les chemins à votre machine.
REM ================================================================

set R_EXE="C:\Program Files\R\R-4.4.0\bin\Rscript.exe"
set PROJECT_ROOT="C:\NGESvolaille_dataops"
set SCRIPT_PATH=%PROJECT_ROOT%\scripts\run_ingestion.R

%R_EXE% %SCRIPT_PATH%
exit /b %ERRORLEVEL%
