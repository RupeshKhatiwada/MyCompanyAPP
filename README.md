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

## Stability & Deployment Readiness
- Run full health check:
  - `npm run health:check`
- Create + verify backup:
  - `npm run backup:test`
- One-click maintenance (build CSS + health + backup test + regenerate Windows kit):
  - `npm run maintenance:one-click`
- Unix shell helper:
  - `bash scripts/maintenance-one-click.sh`

## Windows Deployment Kit
Generate helper scripts:

1. Build kit:
   - `npm run make:windows-kit`
2. Use generated folder:
   - `windows-kit/start-aqua-msk.bat` (one-click run)
   - `windows-kit/install-startup-task.ps1` (auto-start on login)
   - `windows-kit/backup-to-usb.ps1` (copy DB/backups to USB/cloud folder)
   - `windows-kit/maintenance-one-click.bat` (one-click maintenance)
   - `windows-kit/update-aqua-msk.bat` (one-click update + maintenance)

## Data & Backup Notes
- Database file: `data/aqua.db`
- Session files: `data/sessions`
- Backup files: `data/backups`
- Upload files (photos/docs): `public/uploads`

## Hybrid Mode (Offline + PostgreSQL Online Sync)
- Keep using local SQLite for daily offline work.
- Open `Admin -> Settings` and enable **Hybrid Sync**.
- Configure:
  - `PostgreSQL URL` (Railway connection string)
  - `Site ID` (unique for each branch/machine)
  - `Auto sync interval`
- Use **Run Hybrid Sync Now** for immediate push.
- Synced data is mirrored to PostgreSQL tables:
  - `hybrid_records`
  - `hybrid_sync_runs`

## Security
- Set `SESSION_SECRET` in the environment for production.
- App is designed for local/offline LAN usage; do not expose directly to public internet without reverse proxy + TLS + auth hardening.
