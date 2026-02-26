@echo off
setlocal
set APP_DIR=%~dp0..
cd /d "%APP_DIR%"
if not exist data\sessions mkdir data\sessions
if not exist data\backups mkdir data\backups
if not exist data\logs mkdir data\logs
call npm run deploy:ready
if errorlevel 1 (
  echo Maintenance failed.
  pause
  exit /b 1
)
echo Maintenance completed successfully.
pause
