@echo off
setlocal
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0stop-aqua-msk.ps1"
ping 127.0.0.1 -n 3 >nul
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0start-aqua-msk.ps1"
if errorlevel 1 (
  echo Restart failed.
  pause
  exit /b 1
)
exit /b 0
