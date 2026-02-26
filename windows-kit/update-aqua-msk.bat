@echo off
setlocal
set APP_DIR=%~dp0..
cd /d "%APP_DIR%"
if not exist data\sessions mkdir data\sessions
if not exist data\backups mkdir data\backups
where git >nul 2>&1
if %errorlevel%==0 (
  echo Pulling latest changes...
  git pull
)
call npm install
if errorlevel 1 (
  echo npm install failed.
  pause
  exit /b 1
)
call npm run deploy:ready
if errorlevel 1 (
  echo Update failed.
  pause
  exit /b 1
)
echo Update completed successfully.
pause
