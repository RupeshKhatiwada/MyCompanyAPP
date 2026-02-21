# AQUA MSK Offline Web App

Offline, localhost-only management system for water jar production.

## Key Features
- Role-based access: `SUPER_ADMIN`, `ADMIN`, `WORKER`
- Exports, credits, imports, jar sales, savings, staff and salary flows
- English/Nepali UI support
- Backup + restore tools
- Auto backup settings (hour + retention)
- Day-close lock (workers cannot edit closed dates)
- Audit log with old -> new values on major edit flows
- CSV/print reports

## Run Locally
1. Install dependencies:
   - `npm install`
2. Build CSS:
   - `npm run build:css`
3. Start app:
   - `npm start`
4. Open:
   - `http://localhost:3000`

## Windows Deployment Kit
Generate helper scripts:

1. Build kit:
   - `npm run make:windows-kit`
2. Use generated folder:
   - `windows-kit/start-aqua-msk.bat` (one-click run)
   - `windows-kit/install-startup-task.ps1` (auto-start on login)
   - `windows-kit/backup-to-usb.ps1` (copy DB/backups to USB/cloud folder)

## Data & Backup Notes
- Database file: `data/aqua.db`
- Session files: `data/sessions`
- Backup files: `data/backups`
- Upload files (photos/docs): `public/uploads`

## Security
- Set `SESSION_SECRET` in the environment for production.
- App is designed for local/offline LAN usage; do not expose directly to public internet without reverse proxy + TLS + auth hardening.
