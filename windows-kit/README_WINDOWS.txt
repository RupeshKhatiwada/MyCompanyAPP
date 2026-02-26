AQUA MSK Windows Production Package
===================================

Goal: staff run AQUA MSK with one click, no terminal.

First-time setup on each staff PC:
1) Install Node.js 20 LTS.
2) Open PowerShell in this windows-kit folder.
3) Run:
   powershell -ExecutionPolicy Bypass -File .\install-staff-pc.ps1

Daily staff use:
- Double click desktop shortcut: "AQUA MSK"

Auto-start on login:
- Installed automatically by install-staff-pc.ps1
- Task name: AQUA_MSK_AutoStart

Manual controls:
- Start: start-aqua-msk.bat
- Stop: stop-aqua-msk.bat
- Restart: restart-aqua-msk.bat

Maintenance:
- maintenance-one-click.bat

Update package:
- update-aqua-msk.bat

Backup to USB:
- powershell -ExecutionPolicy Bypass -File .\backup-to-usb.ps1 -TargetPath E:\
