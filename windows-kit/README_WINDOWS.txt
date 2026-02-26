AQUA MSK Windows Deployment Kit
================================

1) Place this "windows-kit" folder inside your AQUA MSK project root.
2) Double click: start-aqua-msk.bat
3) Open browser: http://localhost:3000

Auto start when Windows logs in:
- Right click PowerShell and Run:
  powershell -ExecutionPolicy Bypass -File .\install-startup-task.ps1

One-click maintenance (health check + backup test + CSS build + kit refresh):
- Double click: maintenance-one-click.bat

One-click update (git pull + npm install + maintenance):
- Double click: update-aqua-msk.bat

Stop server:
- Double click: stop-aqua-msk.bat

Backup to USB drive:
- Example:
  powershell -ExecutionPolicy Bypass -File .\backup-to-usb.ps1 -TargetPath E:\
