@echo off
setlocal
set APP_DIR=%~dp0..
cd /d "%APP_DIR%"
if not exist data\sessions mkdir data\sessions
if not exist data\backups mkdir data\backups
call npm run deploy:ready
if errorlevel 1 (
  echo Maintenance failed.
  pause
  exit /b 1
)
echo Maintenance completed successfully.
pause
