# AQUA MSK Simplified Architecture

This document captures the first-pass IA refactor for AQUA MSK and the deeper product direction for future cleanup work. The goal is to make the app easier for office staff to learn, faster for daily use, and safer to extend later without scattering features across too many pages.

## Main Sections

The app should stay inside these six sections:

1. Dashboard
2. Workforce
3. Operations
4. Finance
5. Inventory
6. Reports & Settings

## Daily Workflow

The UI should guide staff through one practical flow:

1. Start day and check alerts
2. Mark attendance
3. Record trips and operations
4. Record credits, payments, and expenses
5. Update stock only when needed
6. Review reconciliation and close the day

## What Each Section Owns

### Dashboard

- Today summary
- Search
- Alerts
- Quick actions
- Snapshot entry point

### Workforce

- Staff and workers
- Attendance
- Overtime
- Salary and advances
- Documents
- Active / inactive status

### Operations

- Vehicles
- Trip / export entry
- Returns
- Leakage
- Cleaning routines
- Water tests

### Finance

- Customer credits
- Vehicle credits
- Payment ledger
- Company expenses
- Vehicle expenses
- Jar sales and leakage sales
- Rent income
- Savings
- Invoices
- Reconciliation

### Inventory

- Imports
- Item types
- Jar types
- Future stock controls

### Reports & Settings

- Reports
- Payroll summary
- Audit
- Backup
- Settings
- Recycle bin
- Windows deployment kit
- Super admin tools

## Refactor Direction

### 1. Workforce Unification

Merge staff and workers into one workforce model with role-based permissions. This reduces duplicate forms, salary logic, attendance logic, and document handling.

### 2. Operation Entry Unification

Move toward one operation entry flow that captures:

- trip basics
- quantities sent
- returns
- leakage
- sold items
- route and staff check notes

This should replace scattered operational entry points over time.

### 3. Finance Ledger Unification

Move all money movement into one transaction engine with types like:

- sale payment
- credit creation
- credit payment
- expense
- purchase
- salary payment
- rent income
- savings adjustment

### 4. Inventory Unification

Track all physical items through one stock engine:

- stock in
- stock out
- usage
- adjustments

## Phase-1 UI Changes

The current first pass focuses on:

- compressing navigation into the six-section model
- reducing dashboard quick actions to the most-used daily actions
- keeping all existing routes available while regrouping them more logically

## Implementation Notes

- Keep dates stored in AD and convert to BS in the UI
- Keep the app offline-first with local database priority
- Preserve hybrid-ready structure for future sync
- Prefer action-first layouts over report-heavy landing pages
- Keep menus shallow and predictable for non-technical users
