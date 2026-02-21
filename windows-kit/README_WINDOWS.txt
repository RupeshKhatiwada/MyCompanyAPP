AQUA MSK Windows Deployment Kit
================================

1) Place this "windows-kit" folder inside your AQUA MSK project root.
2) Double click: start-aqua-msk.bat
3) Open browser: http://localhost:3000

Auto start when Windows logs in:
- Right click PowerShell and Run:
  powershell -ExecutionPolicy Bypass -File .\install-startup-task.ps1

Stop server:
- Double click: stop-aqua-msk.bat

Backup to USB drive:
- Example:
  powershell -ExecutionPolicy Bypass -File .\backup-to-usb.ps1 -TargetPath E:\
