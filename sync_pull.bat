@echo off
:: Sync automatico - fa git pull per scaricare i dati aggiornati dal bot Railway
:: Eseguito da Task Scheduler ogni 15 minuti

cd /d "%~dp0"

:: Check connessione internet
ping -n 1 github.com >nul 2>&1
if errorlevel 1 (
    exit /b 0
)

:: Pull solo se ci sono aggiornamenti remoti
git fetch origin main --quiet 2>nul
git diff --quiet HEAD origin/main 2>nul
if errorlevel 1 (
    git pull --quiet origin main 2>nul
)
