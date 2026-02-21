@echo off
setlocal

cd /d "%~dp0\..\.."

if not exist "data\sessions" mkdir "data\sessions"

if not exist "public\css\styles.css" (
  call npm run build:css
)

set "NODE_ENV=production"
if "%SESSION_SECRET%"=="" (
  set "SESSION_SECRET=AQUA_MSK_LOCAL_%COMPUTERNAME%"
)

node src\server.js
