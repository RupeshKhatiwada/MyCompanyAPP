@echo off
setlocal
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0stop-aqua-msk.ps1"
if /I "%~1"=="--no-pause" exit /b 0
echo.
pause
