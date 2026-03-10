# AQUA MSK 2-Day Pilot + Bug Freeze

## Goal
Run the live office workflow for 2 days, fix only blocking bugs, then freeze features for production rollout.

## Scope
- Use real data entry for both roles:
  - `ADMIN` / `SUPER_ADMIN`
  - `WORKER`
- Cover modules:
  - Exports, Credits, Imports, Purchases, Vehicle Expenses, Jar Sales, Salaries
  - Vendor Aging reminders
  - CSV / Print / Payment receipt flows

## Day 1 Checklist
1. Start app and verify login/logout for all roles.
2. Create 5-10 real exports (company + non-company vehicles).
3. Add customer credits and partial payments (cash/bank/e-wallet mix).
4. Add supplier credit entries in imports/purchases with due date + reminder days.
5. Print and verify payment receipts for:
   - export credit payment
   - customer credit payment
   - import payment
   - purchase payment
   - vehicle expense payment
   - jar sale payment
   - salary payment
6. Verify vendor aging page shows `Due Soon` and `Overdue` correctly.
7. Verify customer/vehicle invoice CSV includes `Date (AD)` and `Date (BS)`.
8. Log only blocking bugs in a single list.

## Day 2 Checklist
1. Repeat full daily operations with actual office users.
2. Verify permission matrix behavior:
   - Super Admin toggles module access in settings.
   - Admin/Worker menu visibility updates correctly.
   - Blocked modules return unauthorized page.
3. Test backup and restore on a test copy.
4. Validate printed A4 outputs from each critical report.
5. Re-test all Day 1 blocking bug fixes only.

## Bug Severity Rules
- `BLOCKER`: cannot continue office workflow, wrong money totals, cannot save critical record, app crash.
- `MAJOR`: key flow works but with high friction/risk.
- `MINOR`: UI polish, wording, spacing, non-critical.

During pilot:
- Fix `BLOCKER` immediately.
- Fix `MAJOR` only if low-risk and same-day.
- Defer `MINOR` until after freeze.

## Freeze Rules
After Day 2 sign-off:
1. Freeze feature scope.
2. Apply only:
   - blocker fixes
   - data integrity fixes
   - deployment packaging fixes
3. No new modules/UI redesign until first production week completes.

## Production Rollout Checklist
1. Build CSS: `npm run build:css`
2. Generate Windows kit from Admin -> Windows Kit.
3. Install on staff PC via `windows-kit/install-staff-pc.ps1`.
4. Validate:
   - auto-start task
   - backup folder writes
   - localhost launch shortcut
5. Keep one daily backup off-device (USB/cloud folder).
