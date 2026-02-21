@echo off
setlocal
set APP_DIR=%~dp0..
cd /d "%APP_DIR%"
if not exist data\sessions mkdir data\sessions
if not exist data\backups mkdir data\backups
start "" http://localhost:3000
node src\server.js
