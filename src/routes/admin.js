const express = require("express");
const path = require("path");
const fs = require("fs");
const multer = require("multer");
const dayjs = require("dayjs");
const bcrypt = require("bcryptjs");
const crypto = require("crypto");
const { db, dbPath } = require("../db");
const { requireRole, requireModule } = require("../middleware/auth");
const { adToBs } = require("../utils/calendar");
const { formatActivityRows } = require("../utils/activity");
const { createReceiptNo, createInvoiceNo, getNumberingConfig, getFiscalYearLabel } = require("../utils/numbering");
const {
  createRecycleEntry,
  listRecycleEntries,
  restoreEntry,
  removeAllRecycleEntries,
  removeRecycleEntry,
  getRecycleEntryById
} = require("../utils/recycleBin");
const { buildWindowsKit, windowsKitDir } = require("../utils/windowsKit");
const {
  createBackupFile,
  listBackupFiles,
  getLatestBackup,
  testBackupFile,
  pruneOldBackups
} = require("../utils/backup");
const {
  getHybridSyncStatus,
  normalizeSiteId,
  syncLocalToPostgres
} = require("../utils/hybridSync");
const {
  getRetentionConfig,
  getRetentionStatus,
  listArchiveRuns,
  runRetentionArchive
} = require("../utils/retention");
const {
  computeAccruedSalaryUntilMonth,
  computeSalaryDue: computeSalaryDueWithHistory,
  getDailyCollectionBalance,
  getMonthlySalaryForMonth,
  getSalaryAdjustments,
  parseWholeMoney,
  recordSalaryAdjustment,
  roundMoney
} = require("../utils/salary");
const {
  MODULE_PERMISSION_ITEMS,
  ROLE_PERMISSION_ROLES,
  ensureRolePermissionRows,
  getRolePermissionMap
} = require("../utils/modulePermissions");
const {
  buildExportCreditTotalsJoin,
  buildExportRemainingCreditAggregateSql,
  buildExportOpeningPaymentAggregateSql
} = require("../utils/exportPayments");
const { getFinancialIntegrityReport } = require("../utils/financialIntegrity");
const {
  getVehicleCompliance,
  getVehicleComplianceMap,
  saveVehicleCompliance,
  getVehicleComplianceAlertThresholdDays,
  listVehicleComplianceAlerts
} = require("../utils/vehicleCompliance");

const router = express.Router();
const defaultStaffRoleCodes = ["CLEANER", "MACHINE_MANAGER", "VEHICLE_CONDUCTOR", "KITCHEN_COOK"];
const documentTypeOptions = ["CITIZENSHIP", "LICENSE", "PASSPORT", "PAN", "NATIONAL_ID", "VOTER_CARD", "OTHERS"];
const alertItemThresholdPrefix = "alert_item_threshold_";
const importLabelKeyByCode = {
  JAR_CONTAINER: "importItemJarContainer",
  JAR_CAP: "importItemJarCap",
  CHEMICAL_LABEL: "importItemChemicalLabel",
  LABEL_STICKER: "importItemLabelSticker",
  DATE_LABEL: "importItemDateLabel",
  BOTTLE_CASE: "importItemBottleCase",
  DISPENSER: "importItemDispenser"
};
const workerDocumentFields = [
  { name: "photo", maxCount: 1 },
  { name: "doc_front", maxCount: 1 },
  { name: "doc_back", maxCount: 1 },
  { name: "doc_single", maxCount: 1 }
];
const themePresetOptions = ["classic", "ocean", "slate", "sunrise", "forest"];
const normalizeThemePreset = (value) => {
  const safe = String(value || "").trim().toLowerCase();
  return themePresetOptions.includes(safe) ? safe : "classic";
};
const parseMoneyValue = (value) => {
  const num = Number(value || 0);
  if (Number.isNaN(num)) return 0;
  return Math.round((num + Number.EPSILON) * 100) / 100;
};

const normalizeCustomerText = (value) => String(value || "").trim();

const parseOptionalId = (value) => {
  const num = Number(value || 0);
  if (Number.isNaN(num) || num <= 0) return null;
  return num;
};

const getCustomerById = (customerId) => {
  const safeId = Number(customerId || 0);
  if (!Number.isFinite(safeId) || safeId <= 0) return null;
  return db.prepare(
    `SELECT customers.id,
            customers.name,
            COALESCE(customers.phone, '') AS phone,
            COALESCE(customers.location, '') AS location,
            COALESCE(customers.is_active, 1) AS is_active
     FROM customers
     WHERE customers.id = ?`
  ).get(safeId);
};

const getCustomerDirectoryForSelect = ({ includeInactive = false } = {}) => {
  const whereSql = includeInactive ? "" : "WHERE COALESCE(customers.is_active, 1) = 1";
  return db.prepare(
    `SELECT customers.id,
            customers.name,
            COALESCE(customers.phone, '') AS phone,
            COALESCE(customers.location, '') AS location,
            COALESCE(customers.is_active, 1) AS is_active
     FROM customers
     ${whereSql}
     ORDER BY COALESCE(customers.is_active, 1) DESC, customers.name COLLATE NOCASE ASC, customers.id ASC`
  ).all();
};

const findCustomerByName = (name) => {
  const safeName = normalizeCustomerText(name);
  if (!safeName) return null;
  return db.prepare(
    `SELECT customers.id,
            customers.name,
            COALESCE(customers.phone, '') AS phone,
            COALESCE(customers.location, '') AS location,
            COALESCE(customers.is_active, 1) AS is_active
     FROM customers
     WHERE lower(trim(customers.name)) = lower(trim(?))
     ORDER BY COALESCE(customers.is_active, 1) DESC, customers.id ASC
     LIMIT 1`
  ).get(safeName);
};

const getCustomerReportOptions = () => {
  const customerRows = getCustomerDirectoryForSelect({ includeInactive: true }).map((row) => ({
    ...row,
    value: `id:${row.id}`,
    is_legacy: 0
  }));
  const seenNames = new Set(customerRows.map((row) => normalizeCustomerText(row.name).toLowerCase()).filter(Boolean));
  const legacyRows = db.prepare(
    `SELECT DISTINCT trim(customer_name) AS name
     FROM credits
     WHERE COALESCE(customer_id, 0) = 0
       AND trim(COALESCE(customer_name, '')) != ''
     ORDER BY trim(customer_name) COLLATE NOCASE ASC`
  ).all()
    .map((row) => normalizeCustomerText(row.name))
    .filter(Boolean)
    .filter((name) => {
      const key = name.toLowerCase();
      if (seenNames.has(key)) return false;
      seenNames.add(key);
      return true;
    })
    .map((name) => ({
      id: 0,
      name,
      phone: "",
      location: "",
      is_active: 1,
      is_legacy: 1,
      value: `name:${name}`
    }));
  return [...customerRows, ...legacyRows];
};

const resolveCustomerReportSelection = (query = {}) => {
  const customerRef = String(query.customer_ref || "").trim();
  let customerId = parseOptionalId(query.customer_id);
  let customerName = normalizeCustomerText(query.customer);

  if (customerRef.startsWith("id:")) {
    customerId = parseOptionalId(customerRef.slice(3));
    customerName = "";
  } else if (customerRef.startsWith("name:")) {
    customerId = null;
    customerName = normalizeCustomerText(customerRef.slice(5));
  }

  const selectedCustomer = customerId ? getCustomerById(customerId) : findCustomerByName(customerName);
  const customer = selectedCustomer ? selectedCustomer.name : customerName;
  const resolvedCustomerRef = selectedCustomer
    ? `id:${selectedCustomer.id}`
    : (customer ? `name:${customer}` : "");

  return {
    selectedCustomer,
    customer,
    customerId: selectedCustomer ? selectedCustomer.id : 0,
    customerRef: resolvedCustomerRef
  };
};

const getCustomerCreditMatch = ({ selectedCustomer, customer }) => {
  if (selectedCustomer) {
    return {
      sql: "(credits.customer_id = ? OR (COALESCE(credits.customer_id, 0) = 0 AND lower(trim(credits.customer_name)) = lower(trim(?))))",
      params: [selectedCustomer.id, selectedCustomer.name]
    };
  }
  const safeCustomer = normalizeCustomerText(customer);
  if (safeCustomer) {
    return {
      sql: "lower(trim(credits.customer_name)) = lower(trim(?))",
      params: [safeCustomer]
    };
  }
  return { sql: "1 = 0", params: [] };
};

const parseKeyValueDetails = (details) => {
  const map = {};
  String(details || "")
    .split(/[;,]/)
    .map((part) => part.trim())
    .filter(Boolean)
    .forEach((part) => {
      const idx = part.indexOf("=");
      if (idx <= 0) return;
      const key = part.slice(0, idx).trim().toLowerCase();
      const value = part.slice(idx + 1).trim();
      if (key) map[key] = value;
    });
  return map;
};

const parseOptionalDate = (value) => {
  const text = String(value || "").trim();
  return /^\d{4}-\d{2}-\d{2}$/.test(text) ? text : null;
};

const parsePaymentAmountFromDetails = (details) => {
  const match = String(details || "").match(/payment=([0-9.]+)/i);
  if (!match) return 0;
  const value = Number(match[1]);
  return Number.isNaN(value) ? 0 : value;
};

const getCustomerSettlementPayments = ({ customer, from, to }) => {
  const safeCustomer = String(customer || "").trim().toLowerCase();
  if (!safeCustomer) return [];
  return db.prepare(
    `SELECT activity_logs.created_at,
            activity_logs.details,
            users.full_name AS recorded_by
     FROM activity_logs
     LEFT JOIN users ON users.id = activity_logs.user_id
     WHERE activity_logs.action = 'payment'
       AND activity_logs.entity_type = 'credit_customer_settlement'`
  ).all().map((row) => {
    const detailsMap = parseKeyValueDetails(row.details);
    const logCustomer = String(detailsMap.customer || "").trim().toLowerCase();
    if (logCustomer !== safeCustomer) return null;
    const paymentDate = parseOptionalDate(detailsMap.payment_date)
      || (row.created_at ? dayjs(row.created_at).format("YYYY-MM-DD") : null);
    if (!paymentDate || paymentDate < from || paymentDate > to) return null;
    const amount = parsePaymentAmountFromDetails(row.details);
    if (amount <= 0) return null;
    return {
      amount,
      note: detailsMap.receipt ? `Customer settlement (${detailsMap.receipt})` : "Customer settlement",
      paid_at: row.created_at || paymentDate,
      payment_date: paymentDate,
      vehicle_number: "-",
      recorded_by: row.recorded_by || "-"
    };
  }).filter(Boolean);
};

const buildCustomerLedgerEntries = ({ selectedCustomer, customer, from, to, t }) => {
  const match = getCustomerCreditMatch({ selectedCustomer, customer });
  const credits = db.prepare(
    `SELECT credits.id, credits.credit_date, credits.amount, credits.credit_jars, credits.credit_bottle_cases,
            vehicles.vehicle_number, vehicles.owner_name
     FROM credits
     JOIN vehicles ON credits.vehicle_id = vehicles.id
     WHERE ${match.sql}
       AND credits.credit_date BETWEEN ? AND ?
     ORDER BY credits.credit_date ASC`
  ).all(...match.params, from, to);

  const payments = db.prepare(
    `SELECT credit_payments.amount, credit_payments.note, credit_payments.paid_at,
            credits.id as credit_id, vehicles.vehicle_number
     FROM credit_payments
     JOIN credits ON credit_payments.credit_id = credits.id
     JOIN vehicles ON credits.vehicle_id = vehicles.id
     WHERE ${match.sql}
       AND date(credit_payments.paid_at) BETWEEN ? AND ?
       AND COALESCE(credit_payments.note, '') NOT LIKE 'Customer settlement (%'
     ORDER BY credit_payments.paid_at ASC`
  ).all(...match.params, from, to);
  const settlementPayments = getCustomerSettlementPayments({ customer, from, to });

  const creditEntries = credits.map((row) => ({
    type: "credit",
    date: row.credit_date,
    sortDate: `${row.credit_date} 00:00:00`,
    amount: Number(row.amount || 0),
    vehicle: row.vehicle_number,
    note: `${t("credits")} (${row.credit_jars || 0} ${t("jars")}, ${row.credit_bottle_cases || 0} ${t("bottleCases")})`
  }));
  const paymentEntries = [...payments, ...settlementPayments].map((row) => ({
    type: "payment",
    date: row.payment_date || row.paid_at,
    sortDate: row.paid_at || `${row.payment_date} 00:00:00`,
    amount: -Number(row.amount || 0),
    vehicle: row.vehicle_number,
    note: row.note || t("payment")
  }));

  let running = 0;
  const totals = { credit_total: 0, payment_total: 0, balance: 0 };
  const entries = [...creditEntries, ...paymentEntries]
    .sort((a, b) => new Date(a.sortDate) - new Date(b.sortDate))
    .map((entry) => {
      running += entry.amount;
      if (entry.type === "credit") totals.credit_total += entry.amount;
      if (entry.type === "payment") totals.payment_total += Math.abs(entry.amount);
      return { ...entry, balance: running };
    });
  totals.balance = running;

  return { entries, totals };
};

const getCustomerCreditPaymentReportRows = ({ from, to }) => {
  const directRows = db.prepare(
    `SELECT date(credit_payments.paid_at) AS payment_date,
            credit_payments.paid_at AS created_at,
            credit_payments.amount,
            credit_payments.payment_method,
            credit_payments.note,
            credit_payments.receipt_no,
            credits.credit_date,
            credits.customer_name,
            vehicles.vehicle_number,
            vehicles.owner_name,
            users.full_name AS recorded_by
     FROM credit_payments
     JOIN credits ON credits.id = credit_payments.credit_id
     JOIN vehicles ON vehicles.id = credits.vehicle_id
     LEFT JOIN users ON users.id = credit_payments.created_by
     WHERE date(credit_payments.paid_at) BETWEEN ? AND ?
       AND COALESCE(credit_payments.note, '') NOT LIKE 'Customer settlement (%'`
  ).all(from, to).map((row) => ({
    ...row,
    amount: Number(row.amount || 0),
    note: row.note || ""
  }));

  const settlementRows = db.prepare(
    `SELECT activity_logs.created_at,
            activity_logs.details,
            users.full_name AS recorded_by
     FROM activity_logs
     LEFT JOIN users ON users.id = activity_logs.user_id
     WHERE activity_logs.action = 'payment'
       AND activity_logs.entity_type = 'credit_customer_settlement'`
  ).all().map((row) => {
    const detailsMap = parseKeyValueDetails(row.details);
    const paymentDate = parseOptionalDate(detailsMap.payment_date)
      || (row.created_at ? dayjs(row.created_at).format("YYYY-MM-DD") : null);
    if (!paymentDate || paymentDate < from || paymentDate > to) return null;
    const amount = parsePaymentAmountFromDetails(row.details);
    if (amount <= 0) return null;
    return {
      payment_date: paymentDate,
      created_at: row.created_at,
      amount,
      payment_method: String(detailsMap.method || "CASH").toUpperCase(),
      note: detailsMap.receipt ? `Customer settlement (${detailsMap.receipt})` : "Customer settlement",
      receipt_no: detailsMap.receipt || "",
      credit_date: "",
      customer_name: detailsMap.customer || "",
      vehicle_number: "",
      owner_name: "",
      recorded_by: row.recorded_by || "-"
    };
  }).filter(Boolean);

  return [...directRows, ...settlementRows].sort((a, b) => {
    const dateCompare = String(b.payment_date || "").localeCompare(String(a.payment_date || ""));
    if (dateCompare !== 0) return dateCompare;
    return String(b.created_at || "").localeCompare(String(a.created_at || ""));
  });
};

const getExportCreditPaymentReportRows = ({ from, to }) => {
  const directRows = db.prepare(
    `SELECT export_credit_payments.payment_date,
            export_credit_payments.created_at,
            export_credit_payments.amount,
            export_credit_payments.payment_method,
            export_credit_payments.note,
            export_credit_payments.receipt_no,
            exports.export_date,
            exports.receipt_no AS export_receipt_no,
            vehicles.vehicle_number,
            vehicles.owner_name,
            users.full_name AS recorded_by
     FROM export_credit_payments
     LEFT JOIN exports ON exports.id = export_credit_payments.export_id
     LEFT JOIN vehicles ON vehicles.id = COALESCE(export_credit_payments.vehicle_id, exports.vehicle_id)
     LEFT JOIN users ON users.id = export_credit_payments.created_by
     WHERE export_credit_payments.payment_date BETWEEN ? AND ?
       AND NOT (
         COALESCE(export_credit_payments.note, '') = 'Vehicle cumulative export credit payment'
         OR COALESCE(export_credit_payments.note, '') LIKE 'Daily export credit payment for %'
       )`
  ).all(from, to).map((row) => ({
    ...row,
    amount: Number(row.amount || 0),
    note: row.note || ""
  }));

  const vehicleCache = new Map();
  const getVehicle = (vehicleId) => {
    const safeId = Number(vehicleId || 0);
    if (!safeId) return null;
    if (!vehicleCache.has(safeId)) {
      vehicleCache.set(
        safeId,
        db.prepare("SELECT vehicle_number, owner_name FROM vehicles WHERE id = ?").get(safeId) || null
      );
    }
    return vehicleCache.get(safeId);
  };
  const settlementRows = db.prepare(
    `SELECT activity_logs.entity_type,
            activity_logs.entity_id,
            activity_logs.created_at,
            activity_logs.details,
            users.full_name AS recorded_by
     FROM activity_logs
     LEFT JOIN users ON users.id = activity_logs.user_id
     WHERE activity_logs.action = 'payment'
       AND activity_logs.entity_type IN ('export_day_credit', 'export_vehicle_cumulative_settlement')`
  ).all().map((row) => {
    const detailsMap = parseKeyValueDetails(row.details);
    const paymentDate = parseOptionalDate(detailsMap.payment_date)
      || (row.created_at ? dayjs(row.created_at).format("YYYY-MM-DD") : null);
    if (!paymentDate || paymentDate < from || paymentDate > to) return null;
    const amount = parsePaymentAmountFromDetails(row.details);
    if (amount <= 0) return null;
    const entityParts = String(row.entity_id || "").split(":");
    const vehicle = getVehicle(detailsMap.vehicle_id || entityParts[0]);
    const note = row.entity_type === "export_day_credit"
      ? `Daily export credit payment${detailsMap.export_date ? ` (${detailsMap.export_date})` : ""}`
      : "Vehicle cumulative export credit payment";
    return {
      payment_date: paymentDate,
      created_at: row.created_at,
      amount,
      payment_method: String(detailsMap.method || "CASH").toUpperCase(),
      note: detailsMap.receipt ? `${note} (${detailsMap.receipt})` : note,
      receipt_no: detailsMap.receipt || "",
      export_date: detailsMap.export_date || entityParts[1] || "",
      export_receipt_no: "",
      vehicle_number: vehicle?.vehicle_number || "",
      owner_name: vehicle?.owner_name || String(detailsMap.vehicle || ""),
      recorded_by: row.recorded_by || "-"
    };
  }).filter(Boolean);

  return [...directRows, ...settlementRows].sort((a, b) => {
    const dateCompare = String(b.payment_date || "").localeCompare(String(a.payment_date || ""));
    if (dateCompare !== 0) return dateCompare;
    return String(b.created_at || "").localeCompare(String(a.created_at || ""));
  });
};

const buildCompanyEffectiveCollectionSql = (exportAlias = "exports", vehicleAlias = "vehicles") => `
CASE
  WHEN COALESCE(${vehicleAlias}.is_company, 0) = 1
    THEN ROUND(COALESCE(${exportAlias}.collection_amount, 0) + COALESCE(${exportAlias}.sold_jar_amount, 0), 2)
  ELSE COALESCE(${exportAlias}.collection_amount, 0)
END`;

const buildCompanyEffectiveStoredAmountSql = (fieldName, exportAlias = "exports", vehicleAlias = "vehicles") => {
  const effectiveCollectionSql = buildCompanyEffectiveCollectionSql(exportAlias, vehicleAlias);
  return `
CASE
  WHEN COALESCE(${vehicleAlias}.is_company, 0) = 1
   AND COALESCE(${exportAlias}.${fieldName}, 0) < ${effectiveCollectionSql}
    THEN ${effectiveCollectionSql}
  ELSE COALESCE(${exportAlias}.${fieldName}, 0)
END`;
};

const buildCompanyMissingAmountSql = (fieldName, exportAlias = "exports", vehicleAlias = "vehicles") => `
(${buildCompanyEffectiveStoredAmountSql(fieldName, exportAlias, vehicleAlias)} - COALESCE(${exportAlias}.${fieldName}, 0))`;
const clearWorkerReferenceStatements = [
  "UPDATE daily_sales SET created_by = NULL WHERE created_by = ?",
  "UPDATE exports SET created_by = NULL WHERE created_by = ?",
  "UPDATE credits SET created_by = NULL WHERE created_by = ?",
  "UPDATE credit_payments SET created_by = NULL WHERE created_by = ?",
  "UPDATE export_credit_payments SET created_by = NULL WHERE created_by = ?",
  "UPDATE stock_ledger SET created_by = NULL WHERE created_by = ?",
  "UPDATE jar_sales SET created_by = NULL WHERE created_by = ?",
  "UPDATE jar_container_lendings SET created_by = NULL WHERE created_by = ?",
  "UPDATE jar_container_lending_payments SET created_by = NULL WHERE created_by = ?",
  "UPDATE jar_container_lending_returns SET created_by = NULL WHERE created_by = ?",
  "UPDATE import_entries SET created_by = NULL WHERE created_by = ?",
  "UPDATE vehicle_savings SET created_by = NULL WHERE created_by = ?",
  "UPDATE staff_salary_payments SET created_by = NULL WHERE created_by = ?",
  "UPDATE worker_salary_payments SET created_by = NULL WHERE created_by = ?",
  "UPDATE staff_attendance SET recorded_by = NULL WHERE recorded_by = ?",
  "UPDATE user_attendance SET recorded_by = NULL WHERE recorded_by = ?"
].map((sql) => db.prepare(sql));

const normalizeSalaryPaymentSource = (value) => {
  const safe = String(value || "").trim().toUpperCase();
  if (safe === "OWNER_PERSONAL") return "OWNER_PERSONAL";
  if (safe === "BANK_OTHER") return "BANK_OTHER";
  return "DAILY_COLLECTION";
};

const runInTransaction = (callback) => {
  const savepoint = `sp_admin_${Date.now()}_${Math.floor(Math.random() * 100000)}`;
  db.exec(`SAVEPOINT ${savepoint};`);
  try {
    const result = callback();
    db.exec(`RELEASE ${savepoint};`);
    return result;
  } catch (err) {
    try {
      db.exec(`ROLLBACK TO ${savepoint};`);
      db.exec(`RELEASE ${savepoint};`);
    } catch (_) {
      // Preserve the original error.
    }
    throw err;
  }
};

const getSetting = (key, fallback = "") => {
  const row = db.prepare("SELECT value FROM settings WHERE key = ?").get(key);
  if (!row) return fallback;
  const num = Number(row.value);
  return Number.isNaN(num) ? row.value : num;
};

const setSetting = (key, value) => {
  const exists = db.prepare("SELECT key FROM settings WHERE key = ?").get(key);
  if (exists) {
    db.prepare("UPDATE settings SET value = ? WHERE key = ?").run(String(value), key);
  } else {
    db.prepare("INSERT INTO settings (key, value) VALUES (?, ?)").run(key, String(value));
  }
};

const getProfileUserById = (userId) => db.prepare(
  "SELECT id, username, full_name, phone, role, theme_preset FROM users WHERE id = ?"
).get(userId);

const getBackupStatus = () => {
  const lastBackupAt = getSetting("last_backup_at", "");
  const backupParsed = lastBackupAt ? dayjs(lastBackupAt) : null;
  const backupDays = backupParsed && backupParsed.isValid() ? dayjs().diff(backupParsed, "day") : null;
  const backupOverdue = !backupParsed || !backupParsed.isValid() || backupDays >= 7;
  return { lastBackupAt, backupDays, backupOverdue };
};

const getBackupConfig = () => {
  const enabledSetting = String(getSetting("auto_backup_enabled", "1"));
  const hourRaw = Number(getSetting("auto_backup_hour", 18));
  const hour = Number.isNaN(hourRaw) ? 18 : Math.max(0, Math.min(23, Math.floor(hourRaw)));
  const keepRaw = Number(getSetting("auto_backup_keep", 30));
  const keepCount = Number.isNaN(keepRaw) ? 30 : Math.max(3, Math.min(180, Math.floor(keepRaw)));
  return {
    enabled: enabledSetting !== "0",
    hour,
    keepCount
  };
};

const getPrintableCollectionSummary = (from, to) => {
  const operations = db.prepare(
    `SELECT
        COALESCE(SUM(
          CASE
            WHEN (jar_count - return_jar_count - leakage_jar_count) < 0 THEN 0
            ELSE (jar_count - return_jar_count - leakage_jar_count)
          END
        ), 0) AS net_jars,
        COALESCE(SUM(sold_jar_count), 0) AS sold_from_exports,
        COALESCE(SUM(
          CASE
            WHEN (bottle_case_count - return_bottle_case_count - damaged_bottle_case_count) < 0 THEN 0
            ELSE (bottle_case_count - return_bottle_case_count - damaged_bottle_case_count)
          END
        ), 0) AS net_bottles
     FROM exports
     WHERE export_date BETWEEN ? AND ?`
  ).get(from, to);

  const jarContainerSales = db.prepare(
    `SELECT COALESCE(SUM(quantity), 0) AS total
     FROM jar_sales
     WHERE sale_date BETWEEN ? AND ?`
  ).get(from, to);

  const rangeExportMethod = db.prepare(
    `SELECT
        ${buildExportOpeningPaymentAggregateSql("exports", "export_credit_totals")},
        COALESCE(SUM(${buildCompanyMissingAmountSql("paid_cash_amount", "exports", "vehicles")}), 0) AS company_cash_adjustment
     FROM exports
     JOIN vehicles ON vehicles.id = exports.vehicle_id
     ${buildExportCreditTotalsJoin("exports", "export_credit_totals")}
     WHERE export_date BETWEEN ? AND ?`
  ).get(from, to);
  const rangeExportCreditMethod = db.prepare(
    `SELECT
        COALESCE(SUM(cash_amount), 0) AS cash_paid,
        COALESCE(SUM(bank_amount), 0) AS bank_paid,
        COALESCE(SUM(ewallet_amount), 0) AS ewallet_paid
     FROM export_credit_payments
     WHERE payment_date BETWEEN ? AND ?`
  ).get(from, to);
  const rangeCustomerMethod = db.prepare(
    `SELECT
        COALESCE(SUM(CASE WHEN payment_method = 'CASH' THEN amount ELSE 0 END), 0) AS cash_paid,
        COALESCE(SUM(CASE WHEN payment_method = 'BANK' THEN amount ELSE 0 END), 0) AS bank_paid,
        COALESCE(SUM(CASE WHEN payment_method = 'E_WALLET' THEN amount ELSE 0 END), 0) AS ewallet_paid
     FROM credit_payments
     WHERE date(paid_at) BETWEEN ? AND ?`
  ).get(from, to);
  const rangeRentMethod = db.prepare(
    `SELECT
        COALESCE(SUM(CASE WHEN add_to_collection = 1 AND payment_method = 'CASH' THEN amount ELSE 0 END), 0) AS cash_paid,
        COALESCE(SUM(CASE WHEN add_to_collection = 1 AND payment_method = 'BANK' THEN amount ELSE 0 END), 0) AS bank_paid,
        COALESCE(SUM(CASE WHEN add_to_collection = 1 AND payment_method = 'E_WALLET' THEN amount ELSE 0 END), 0) AS ewallet_paid
     FROM rent_entries
     WHERE rent_date BETWEEN ? AND ?`
  ).get(from, to);
  const rangeLeakageMethod = db.prepare(
    `SELECT
        COALESCE(SUM(cash_amount), 0) AS cash_paid,
        COALESCE(SUM(bank_amount), 0) AS bank_paid,
        COALESCE(SUM(ewallet_amount), 0) AS ewallet_paid
     FROM leakage_jar_sale_payments
     WHERE payment_date BETWEEN ? AND ?`
  ).get(from, to);
  const rangeJarSalesMethod = db.prepare(
    `SELECT
        COALESCE(SUM(cash_amount), 0) AS cash_paid,
        COALESCE(SUM(bank_amount), 0) AS bank_paid,
        COALESCE(SUM(ewallet_amount), 0) AS ewallet_paid
     FROM jar_sale_payments
     WHERE payment_date BETWEEN ? AND ?`
  ).get(from, to);
  const rangeContainerLendingMethod = db.prepare(
    `SELECT
        COALESCE(SUM(cash_amount), 0) AS cash_paid,
        COALESCE(SUM(bank_amount), 0) AS bank_paid,
        COALESCE(SUM(ewallet_amount), 0) AS ewallet_paid
     FROM jar_container_lending_payments
     WHERE payment_date BETWEEN ? AND ?`
  ).get(from, to);
  const rangeSavingsDeposits = db.prepare(
    `SELECT COALESCE(SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END), 0) AS amount
     FROM vehicle_savings
     WHERE entry_date BETWEEN ? AND ?`
  ).get(from, to);

  const rangeImportsFromCollection = db.prepare(
    `SELECT COALESCE(SUM(amount), 0) AS amount
     FROM import_payments
     WHERE payment_date BETWEEN ? AND ? AND payment_source = 'DAILY_COLLECTION'`
  ).get(from, to);
  const rangePurchasesFromCollection = db.prepare(
    `SELECT COALESCE(SUM(amount), 0) AS amount
     FROM company_purchase_payments
     WHERE payment_date BETWEEN ? AND ? AND payment_source = 'DAILY_COLLECTION'`
  ).get(from, to);
  const rangeVehicleExpensesFromCollection = db.prepare(
    `SELECT COALESCE(SUM(amount), 0) AS amount
     FROM vehicle_expense_payments
     WHERE payment_date BETWEEN ? AND ? AND payment_source = 'DAILY_COLLECTION'`
  ).get(from, to);
  const rangeStaffSalaryFromCollection = db.prepare(
    `SELECT COALESCE(SUM(amount), 0) AS amount
     FROM staff_salary_payments
     WHERE payment_date BETWEEN ? AND ? AND payment_source = 'DAILY_COLLECTION'`
  ).get(from, to);
  const rangeWorkerSalaryFromCollection = db.prepare(
    `SELECT COALESCE(SUM(amount), 0) AS amount
     FROM worker_salary_payments
     WHERE payment_date BETWEEN ? AND ? AND payment_source = 'DAILY_COLLECTION'`
  ).get(from, to);
  const rangeSavingsWithdrawFromCollection = db.prepare(
    `SELECT COALESCE(SUM(CASE WHEN amount < 0 AND payment_source = 'DAILY_COLLECTION' THEN ABS(amount) ELSE 0 END), 0) AS amount
     FROM vehicle_savings
     WHERE entry_date BETWEEN ? AND ?`
  ).get(from, to);
  const rangeContainerLendingRefunds = db.prepare(
    `SELECT COALESCE(SUM(refund_amount), 0) AS amount
     FROM jar_container_lending_returns
     WHERE return_date BETWEEN ? AND ?`
  ).get(from, to);

  const exportCashCollected = parseMoneyValue(
    Number(rangeExportMethod.cash_paid || 0) +
    Number(rangeExportMethod.company_cash_adjustment || 0)
  );
  const cashCollected = parseMoneyValue(
    exportCashCollected +
    Number(rangeExportCreditMethod.cash_paid || 0) +
    Number(rangeCustomerMethod.cash_paid || 0) +
    Number(rangeRentMethod.cash_paid || 0) +
    Number(rangeLeakageMethod.cash_paid || 0) +
    Number(rangeJarSalesMethod.cash_paid || 0) +
    Number(rangeContainerLendingMethod.cash_paid || 0) +
    Number(rangeSavingsDeposits.amount || 0)
  );
  const bankCollected = parseMoneyValue(
    Number(rangeExportMethod.bank_paid || 0) +
    Number(rangeExportCreditMethod.bank_paid || 0) +
    Number(rangeCustomerMethod.bank_paid || 0) +
    Number(rangeRentMethod.bank_paid || 0) +
    Number(rangeLeakageMethod.bank_paid || 0) +
    Number(rangeJarSalesMethod.bank_paid || 0) +
    Number(rangeContainerLendingMethod.bank_paid || 0)
  );
  const eWalletCollected = parseMoneyValue(
    Number(rangeExportMethod.ewallet_paid || 0) +
    Number(rangeExportCreditMethod.ewallet_paid || 0) +
    Number(rangeCustomerMethod.ewallet_paid || 0) +
    Number(rangeRentMethod.ewallet_paid || 0) +
    Number(rangeLeakageMethod.ewallet_paid || 0) +
    Number(rangeJarSalesMethod.ewallet_paid || 0) +
    Number(rangeContainerLendingMethod.ewallet_paid || 0)
  );

  const totalAmountCollected = parseMoneyValue(cashCollected + bankCollected + eWalletCollected);
  const vehicleExpenses = parseMoneyValue(rangeVehicleExpensesFromCollection.amount || 0);
  const companyExpenses = parseMoneyValue(
    Number(rangeImportsFromCollection.amount || 0) +
    Number(rangePurchasesFromCollection.amount || 0)
  );
  const salaryGiven = parseMoneyValue(
    Number(rangeStaffSalaryFromCollection.amount || 0) +
    Number(rangeWorkerSalaryFromCollection.amount || 0)
  );
  const otherDeductions = parseMoneyValue(rangeSavingsWithdrawFromCollection.amount || 0);
  const depositRefunds = parseMoneyValue(rangeContainerLendingRefunds.amount || 0);
  const totalDeductions = parseMoneyValue(
    vehicleExpenses + companyExpenses + salaryGiven + otherDeductions + depositRefunds
  );
  const netCollectionAmount = parseMoneyValue(totalAmountCollected - totalDeductions);

  return {
    netJarSold: Number(operations.net_jars || 0),
    netJarContainerSold: Number(jarContainerSales.total || 0),
    netBottleCases: Number(operations.net_bottles || 0),
    totalAmountCollected,
    vehicleExpenses,
    companyExpenses,
    salaryGiven,
    otherDeductions,
    depositRefunds,
    totalDeductions,
    netCollectionAmount,
    collectedByMethod: {
      cash: cashCollected,
      bank: bankCollected,
      eWallet: eWalletCollected
    }
  };
};

const getImportItemAlertThresholdMap = () => {
  const rows = db.prepare("SELECT key, value FROM settings WHERE key LIKE ?").all(`${alertItemThresholdPrefix}%`);
  return rows.reduce((acc, row) => {
    const key = String(row.key || "");
    if (!key.startsWith(alertItemThresholdPrefix)) return acc;
    const code = key.slice(alertItemThresholdPrefix.length);
    const value = Number(row.value);
    if (!code || Number.isNaN(value) || value < 0) return acc;
    acc[code] = Math.floor(value);
    return acc;
  }, {});
};

const getAlertSnapshot = () => {
  const today = dayjs().format("YYYY-MM-DD");
  const { lastBackupAt, backupDays, backupOverdue } = getBackupStatus();
  const jarLowThresholdRaw = Number(getSetting("alert_low_stock_jars", 20));
  const itemLowThresholdRaw = Number(getSetting("alert_low_stock_items", 10));
  const overdueDaysRaw = Number(getSetting("alert_overdue_credit_days", 7));
  const vehicleComplianceDays = getVehicleComplianceAlertThresholdDays(db);
  const jarLowThreshold = Number.isNaN(jarLowThresholdRaw) ? 20 : Math.max(0, Math.floor(jarLowThresholdRaw));
  const itemLowThreshold = Number.isNaN(itemLowThresholdRaw) ? 10 : Math.max(0, Math.floor(itemLowThresholdRaw));
  const overdueDays = Number.isNaN(overdueDaysRaw) ? 7 : Math.max(1, Math.floor(overdueDaysRaw));
  const overdueBefore = dayjs().subtract(overdueDays, "day").format("YYYY-MM-DD");
  const itemThresholdMap = getImportItemAlertThresholdMap();
  const vehicleComplianceAlerts = listVehicleComplianceAlerts(db, {
    today,
    dueSoonDays: vehicleComplianceDays
  });

  const jarTypes = db.prepare("SELECT id, name FROM jar_types WHERE active = 1 ORDER BY name").all();
  const jarImportTotals = db.prepare(
    `SELECT jar_type_id, COALESCE(SUM(quantity), 0) as qty
     FROM import_entries
     WHERE item_type = 'JAR_CONTAINER' AND direction = 'IN' AND jar_type_id IS NOT NULL
     GROUP BY jar_type_id`
  ).all();
  const jarSalesTotals = db.prepare(
    `SELECT jar_type_id, COALESCE(SUM(quantity), 0) as qty
     FROM jar_sales
     GROUP BY jar_type_id`
  ).all();
  const jarImportMap = jarImportTotals.reduce((acc, row) => {
    acc[row.jar_type_id] = Number(row.qty || 0);
    return acc;
  }, {});
  const jarSalesMap = jarSalesTotals.reduce((acc, row) => {
    acc[row.jar_type_id] = Number(row.qty || 0);
    return acc;
  }, {});
  const lowJarStocks = jarTypes
    .map((type) => ({
      type_name: type.name,
      balance: (jarImportMap[type.id] || 0) - (jarSalesMap[type.id] || 0)
    }))
    .filter((row) => row.balance <= jarLowThreshold)
    .sort((a, b) => a.balance - b.balance);

  const itemBalances = db.prepare(
    `SELECT import_entries.item_type, import_item_types.name as item_name, import_item_types.unit_label as unit,
            COALESCE(SUM(CASE WHEN import_entries.direction = 'OUT' THEN -import_entries.quantity ELSE import_entries.quantity END), 0) as balance
     FROM import_entries
     LEFT JOIN import_item_types ON import_entries.item_type = import_item_types.code
     WHERE import_entries.item_type <> 'JAR_CONTAINER'
     GROUP BY import_entries.item_type, import_item_types.name, import_item_types.unit_label
     ORDER BY balance ASC`
  ).all();
  const lowItemStocks = itemBalances
    .map((row) => {
      const threshold = Number.isFinite(itemThresholdMap[row.item_type]) ? itemThresholdMap[row.item_type] : itemLowThreshold;
      return {
        ...row,
        threshold,
        item_name: row.item_name || row.item_type
      };
    })
    .filter((row) => Number(row.balance || 0) <= Number(row.threshold || 0))
    .sort((a, b) => Number(a.balance || 0) - Number(b.balance || 0));

  const overdueCredits = db.prepare(
    `SELECT credits.id, credits.credit_date, credits.customer_name, credits.amount, credits.paid_amount,
            vehicles.vehicle_number, vehicles.owner_name,
            CASE WHEN credits.amount - credits.paid_amount < 0 THEN 0 ELSE credits.amount - credits.paid_amount END as remaining_amount
     FROM credits
     JOIN vehicles ON credits.vehicle_id = vehicles.id
     WHERE credits.credit_date <= ?
       AND credits.amount - credits.paid_amount > 0
     ORDER BY credits.credit_date ASC, remaining_amount DESC`
  ).all(overdueBefore).map((row) => ({
    ...row,
    overdue_days: dayjs(today).diff(dayjs(row.credit_date), "day")
  }));

  const staffRows = db.prepare(
    `SELECT staff.id, staff.full_name, staff.start_date, staff.monthly_salary,
            COALESCE(SUM(staff_salary_payments.amount), 0) as paid_total
     FROM staff
     LEFT JOIN staff_salary_payments ON staff_salary_payments.staff_id = staff.id
     WHERE COALESCE(staff.is_active, 1) = 1
     GROUP BY staff.id
     ORDER BY staff.full_name ASC`
  ).all();
  const unpaidStaff = staffRows.map((row) => ({
    ...row,
    due_salary: computeSalaryDue("STAFF", row, row.paid_total, today)
  })).filter((row) => Number(row.due_salary || 0) > 0);

  const workerRows = db.prepare(
    `SELECT users.id, users.full_name, users.start_date, users.monthly_salary,
            COALESCE(SUM(worker_salary_payments.amount), 0) as paid_total
     FROM users
     LEFT JOIN worker_salary_payments ON worker_salary_payments.worker_id = users.id
     WHERE users.role = 'WORKER' AND users.is_active = 1
     GROUP BY users.id
     ORDER BY users.full_name ASC`
  ).all();
  const unpaidWorkers = workerRows.map((row) => ({
    ...row,
    due_salary: computeSalaryDue("WORKER", row, row.paid_total, today)
  })).filter((row) => Number(row.due_salary || 0) > 0);
  const integrityReport = getFinancialIntegrityReport(db);
  return {
    backup: {
      lastBackupAt,
      backupDays,
      backupOverdue
    },
    thresholds: {
      jarLowThreshold,
      itemLowThreshold,
      overdueDays,
      vehicleComplianceDays,
      itemThresholdMap
    },
    lowJarStocks,
    lowItemStocks,
    overdueCredits,
    vehicleComplianceAlerts,
    unpaidStaff,
    unpaidWorkers,
    integrityReport,
    summary: {
      backupWarnings: backupOverdue ? 1 : 0,
      lowStockItems: lowJarStocks.length + lowItemStocks.length,
      overdueCredits: overdueCredits.length,
      vehicleComplianceAlerts: vehicleComplianceAlerts.summary.totalAlerts,
      unpaidSalaryPeople: unpaidStaff.length + unpaidWorkers.length,
      integrityWarnings: integrityReport.totalIssues
    }
  };
};

const getDayClosure = (dateText) => db.prepare(
  "SELECT closure_date, is_closed, note, closed_at, reopened_at FROM day_closures WHERE closure_date = ?"
).get(dateText);

const getRecentDayClosures = () => db.prepare(
  `SELECT day_closures.*,
          closer.full_name as closed_by_name,
          opener.full_name as reopened_by_name
   FROM day_closures
   LEFT JOIN users as closer ON day_closures.closed_by = closer.id
   LEFT JOIN users as opener ON day_closures.reopened_by = opener.id
   ORDER BY day_closures.closure_date DESC
   LIMIT 20`
).all();

const normalizeIsoDate = (value) => {
  const safe = String(value || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(safe)) return null;
  const parsed = dayjs(safe);
  if (!parsed.isValid()) return null;
  return parsed.format("YYYY-MM-DD") === safe ? safe : null;
};

const listClosureDatesInRange = (fromDate, toDate, maxDays = 366) => {
  const start = dayjs(fromDate);
  const end = dayjs(toDate);
  if (!start.isValid() || !end.isValid()) return { ok: false, reason: "INVALID_DATE" };
  if (end.isBefore(start)) return { ok: false, reason: "INVALID_RANGE" };
  const totalDays = end.diff(start, "day") + 1;
  if (totalDays > maxDays) return { ok: false, reason: "TOO_LARGE" };
  const dates = [];
  let cursor = start;
  while (!cursor.isAfter(end)) {
    dates.push(cursor.format("YYYY-MM-DD"));
    cursor = cursor.add(1, "day");
  }
  return { ok: true, dates };
};

const renderSettingsPage = (req, res, overrides = {}) => {
  const { lastBackupAt, backupDays, backupOverdue } = getBackupStatus();
  const backupReminderText = !lastBackupAt || backupDays === null ? req.t("backupNever") : `${backupDays} ${req.t("daysAgo")}`;
  const logoPath = getSetting("logo_path", "");
  const brandName = getSetting("brand_name", "");
  const brandTaglineText = getSetting("brand_tagline", "") || req.t("brandTagline");
  const backupConfig = getBackupConfig();
  const numberingConfig = getNumberingConfig(db);
  const currentFiscalYearLabel = getFiscalYearLabel(dayjs().format("YYYY-MM-DD"), numberingConfig.fiscalStartMonth);
  const retentionConfig = getRetentionConfig(db);
  const retentionStatus = getRetentionStatus(db);
  const archiveRuns = listArchiveRuns(db, 12);
  const hybridSync = getHybridSyncStatus(db);
  const iotAttendanceEnabled = String(getSetting("iot_attendance_enabled", "0")) === "1";
  const iotAttendanceTokenRow = db.prepare("SELECT value FROM settings WHERE key = ?").get("iot_attendance_token");
  const iotAttendanceToken = iotAttendanceTokenRow ? String(iotAttendanceTokenRow.value || "") : "";
  const backupFiles = listBackupFiles().slice(0, 10);
  const jarLowThresholdRaw = Number(getSetting("alert_low_stock_jars", 20));
  const itemLowThresholdRaw = Number(getSetting("alert_low_stock_items", 10));
  const overdueDaysRaw = Number(getSetting("alert_overdue_credit_days", 7));
  const vehicleComplianceDays = getVehicleComplianceAlertThresholdDays(db);
  const jarLowThreshold = Number.isNaN(jarLowThresholdRaw) ? 20 : Math.max(0, Math.floor(jarLowThresholdRaw));
  const itemLowThreshold = Number.isNaN(itemLowThresholdRaw) ? 10 : Math.max(0, Math.floor(itemLowThresholdRaw));
  const overdueDays = Number.isNaN(overdueDaysRaw) ? 7 : Math.max(1, Math.floor(overdueDaysRaw));
  const importItemTypes = db.prepare(
    "SELECT code, name, unit_label FROM import_item_types WHERE is_active = 1 ORDER BY name"
  ).all();
  const itemThresholdMap = getImportItemAlertThresholdMap();
  ensureRolePermissionRows(db);
  const rolePermissionValues = ROLE_PERMISSION_ROLES.reduce((acc, role) => {
    acc[role] = getRolePermissionMap(db, role);
    return acc;
  }, {});
  const selectedClosureDate = overrides.selectedClosureDate || req.query.closure_date || dayjs().format("YYYY-MM-DD");
  const selectedClosure = getDayClosure(selectedClosureDate);

  return res.render("admin/settings", {
    title: req.t("settingsTitle"),
    lastBackupAt,
    backupDays,
    backupOverdue,
    backupReminderText,
    logoPath,
    brandName,
    brandTaglineText,
    autoBackupEnabled: backupConfig.enabled,
    autoBackupHour: backupConfig.hour,
    autoBackupKeep: backupConfig.keepCount,
    numberingConfig,
    currentFiscalYearLabel,
    retentionConfig,
    retentionStatus,
    archiveRuns,
    hybridSync,
    iotAttendanceEnabled,
    iotAttendanceToken,
    backupFiles,
    latestBackup: backupFiles[0] || null,
    jarLowThreshold,
    itemLowThreshold,
    overdueDays,
    vehicleComplianceDays,
    importItemTypes,
    itemThresholdMap,
    modulePermissionItems: MODULE_PERMISSION_ITEMS,
    rolePermissionRoles: ROLE_PERMISSION_ROLES,
    rolePermissionValues,
    selectedClosureDate,
    selectedClosure,
    recentClosures: getRecentDayClosures(),
    error: req.query.branding_error ? req.t(String(req.query.branding_error)) : null,
    success: req.query.branding_saved === "1" ? req.t("brandingSaved") : null,
    backupTest: null,
    ...overrides
  });
};

const logActivity = ({ userId, action, entityType, entityId, details }) => {
  if (!action || !entityType) return;
  db.prepare(
    "INSERT INTO activity_logs (user_id, action, entity_type, entity_id, details) VALUES (?, ?, ?, ?, ?)"
  ).run(userId || null, action, entityType, entityId ? String(entityId) : null, details || null);
};

const formatDiffValue = (value) => {
  if (value === null || value === undefined || value === "") return "empty";
  return String(value);
};

const buildDiffDetails = (beforeRow, afterRow, fields) => {
  const changes = [];
  fields.forEach((field) => {
    const key = typeof field === "string" ? field : field.key;
    const label = typeof field === "string" ? field : (field.label || field.key);
    const beforeValue = beforeRow ? beforeRow[key] : undefined;
    const afterValue = afterRow ? afterRow[key] : undefined;
    if (String(beforeValue ?? "") === String(afterValue ?? "")) return;
    changes.push(`${label}: ${formatDiffValue(beforeValue)} -> ${formatDiffValue(afterValue)}`);
  });
  return changes.length > 0 ? changes.join("; ") : "no_changes";
};

const normalizeFingerprintId = (value) => {
  const safe = String(value || "").trim();
  return safe || null;
};

const findFingerprintConflict = ({ fingerprintId, skipStaffId = null, skipWorkerId = null }) => {
  const safe = normalizeFingerprintId(fingerprintId);
  if (!safe) return null;
  const staff = db.prepare(
    "SELECT id, full_name FROM staff WHERE lower(trim(fingerprint_id)) = lower(trim(?)) AND (? IS NULL OR id != ?)"
  ).get(safe, skipStaffId, skipStaffId);
  if (staff) return { type: "STAFF", id: staff.id, name: staff.full_name };
  const worker = db.prepare(
    "SELECT id, full_name FROM users WHERE role = 'WORKER' AND lower(trim(fingerprint_id)) = lower(trim(?)) AND (? IS NULL OR id != ?)"
  ).get(safe, skipWorkerId, skipWorkerId);
  if (worker) return { type: "WORKER", id: worker.id, name: worker.full_name };
  return null;
};

const setVehicleActiveStatus = (vehicleId, isActive, userId) => {
  if (isActive) {
    return db.prepare(
      "UPDATE vehicles SET is_active = 1, deactivated_at = NULL, deactivated_by = NULL, updated_at = datetime('now') WHERE id = ?"
    ).run(vehicleId);
  }
  return db.prepare(
    "UPDATE vehicles SET is_active = 0, deactivated_at = datetime('now'), deactivated_by = ?, updated_at = datetime('now') WHERE id = ?"
  ).run(userId || null, vehicleId);
};

const setStaffActiveStatus = (staffId, isActive, userId) => {
  if (isActive) {
    return db.prepare(
      "UPDATE staff SET is_active = 1, deactivated_at = NULL, deactivated_by = NULL, updated_at = datetime('now') WHERE id = ?"
    ).run(staffId);
  }
  return db.prepare(
    "UPDATE staff SET is_active = 0, deactivated_at = datetime('now'), deactivated_by = ?, updated_at = datetime('now') WHERE id = ?"
  ).run(userId || null, staffId);
};

const normalizeAnswer = (value) => String(value || "").trim().toLowerCase();

const generateRecoveryKey = () => {
  const raw = crypto.randomBytes(12).toString("hex").toUpperCase();
  const chunks = raw.match(/.{1,4}/g) || [raw];
  return chunks.join("-");
};

const uploadDir = path.join(__dirname, "..", "..", "public", "uploads");
fs.mkdirSync(uploadDir, { recursive: true });
const restoreDir = path.join(__dirname, "..", "..", "data", "restore");
fs.mkdirSync(restoreDir, { recursive: true });
const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, uploadDir),
  filename: (req, file, cb) => {
    const ext = path.extname(file.originalname) || ".jpg";
    const base = `vehicle_${Date.now()}_${Math.random().toString(16).slice(2)}`;
    cb(null, `${base}${ext}`);
  }
});
const upload = multer({ storage });
const logoStorage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, uploadDir),
  filename: (req, file, cb) => {
    const ext = path.extname(file.originalname) || ".png";
    cb(null, `logo_${Date.now()}${ext}`);
  }
});
const logoUpload = multer({ storage: logoStorage });
const wordmarkStorage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, uploadDir),
  filename: (req, file, cb) => {
    const ext = path.extname(file.originalname) || ".png";
    cb(null, `brand_${Date.now()}${ext}`);
  }
});
const wordmarkUpload = multer({ storage: wordmarkStorage });
const restoreUpload = multer({ dest: restoreDir });
const staffStorage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, uploadDir),
  filename: (req, file, cb) => {
    const ext = path.extname(file.originalname) || ".jpg";
    const base = `staff_${Date.now()}_${Math.random().toString(16).slice(2)}`;
    cb(null, `${base}${ext}`);
  }
});
const staffUpload = multer({
  storage: staffStorage,
  limits: { fileSize: 5 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    if (file.mimetype && file.mimetype.startsWith("image/")) return cb(null, true);
    cb(new Error("Invalid file type"));
  }
});

const normalizeDocumentType = (value) => {
  const safe = String(value || "").trim().toUpperCase();
  return documentTypeOptions.includes(safe) ? safe : null;
};

const buildImportItemCode = (name) => {
  const base = String(name || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "") || "ITEM";
  let code = base;
  let suffix = 2;
  while (db.prepare("SELECT id FROM import_item_types WHERE code = ?").get(code)) {
    code = `${base}_${suffix}`;
    suffix += 1;
  }
  return code;
};

const resolveImportItemLabel = (code, name, t) => {
  const key = importLabelKeyByCode[code];
  return key ? t(key) : (name || code);
};

router.use(requireRole(["SUPER_ADMIN", "ADMIN"]));
router.use("/vehicles", requireModule("vehicles", "view"));
router.use("/staffs", requireModule("staffs", "view"));
router.use("/staff-roles", requireModule("staffs", "view"));
router.use("/worker-attendance", requireModule("attendance", "view"));
router.use("/activity", requireModule("history", "view"));
router.use("/savings", requireModule("savings", "view"));

router.get("/", (req, res) => {
  const today = dayjs().format("YYYY-MM-DD");
  const weekStart = dayjs().startOf("week").format("YYYY-MM-DD");
  const monthStart = dayjs().startOf("month").format("YYYY-MM-DD");
  const yearStart = dayjs().startOf("year").format("YYYY-MM-DD");
  const allTimeStart = db.prepare(
    `SELECT MIN(dt) as min_date
     FROM (
       SELECT MIN(export_date) as dt FROM exports
       UNION ALL SELECT MIN(credit_date) as dt FROM credits
       UNION ALL SELECT MIN(entry_date) as dt FROM import_entries
       UNION ALL SELECT MIN(purchase_date) as dt FROM company_purchases
       UNION ALL SELECT MIN(expense_date) as dt FROM vehicle_expenses
       UNION ALL SELECT MIN(payment_date) as dt FROM staff_salary_payments
       UNION ALL SELECT MIN(payment_date) as dt FROM worker_salary_payments
       UNION ALL SELECT MIN(entry_date) as dt FROM vehicle_savings
       UNION ALL SELECT MIN(rent_date) as dt FROM rent_entries
       UNION ALL SELECT MIN(business_date) as dt FROM day_reconciliations
     )`
  ).get()?.min_date || today;
  const vehicleCount = db.prepare("SELECT COUNT(*) as count FROM vehicles").get().count;
  const workerCount = db.prepare("SELECT COUNT(*) as count FROM users WHERE role = 'WORKER' AND is_active = 1").get().count;
  const staffCount = db.prepare("SELECT COUNT(*) as count FROM staff WHERE COALESCE(is_active, 1) = 1").get().count;
  const todaySales = db.prepare("SELECT COALESCE(SUM(total_amount), 0) as total FROM exports WHERE export_date = ?")
    .get(today).total || 0;
  const monthSales = db.prepare("SELECT COALESCE(SUM(total_amount), 0) as total FROM exports WHERE export_date BETWEEN ? AND ?")
    .get(monthStart, today).total || 0;
  const todayExport = db.prepare(
    `SELECT
        COALESCE(SUM(total_amount), 0) as total,
        COALESCE(SUM(paid_amount), 0) as paid,
        COALESCE(SUM(credit_amount), 0) as credit
     FROM exports
     WHERE export_date = ?`
  ).get(today);
  const monthExport = db.prepare(
    `SELECT
        COALESCE(SUM(total_amount), 0) as total,
        COALESCE(SUM(paid_amount), 0) as paid,
        COALESCE(SUM(credit_amount), 0) as credit
     FROM exports
     WHERE export_date BETWEEN ? AND ?`
  ).get(monthStart, today);
  const monthCustomerCredit = db.prepare(
    `SELECT
        COALESCE(SUM(amount), 0) as total,
        COALESCE(SUM(CASE WHEN amount - paid_amount < 0 THEN 0 ELSE amount - paid_amount END), 0) as credit
     FROM credits
     WHERE credit_date BETWEEN ? AND ?`
  ).get(monthStart, today);
  const monthCustomerPaid = db.prepare(
    `SELECT COALESCE(SUM(amount), 0) as paid
     FROM credit_payments
     WHERE date(paid_at) BETWEEN ? AND ?`
  ).get(monthStart, today);
  const monthExportMethod = db.prepare(
    `SELECT
        COALESCE(SUM(paid_cash_amount), 0) as cash_paid,
        COALESCE(SUM(paid_bank_amount), 0) as bank_paid,
        COALESCE(SUM(paid_ewallet_amount), 0) as ewallet_paid
     FROM exports
     WHERE export_date BETWEEN ? AND ?`
  ).get(monthStart, today);
  const monthCustomerMethod = db.prepare(
    `SELECT
        COALESCE(SUM(CASE WHEN payment_method = 'CASH' THEN amount ELSE 0 END), 0) as cash_paid,
        COALESCE(SUM(CASE WHEN payment_method = 'BANK' THEN amount ELSE 0 END), 0) as bank_paid,
        COALESCE(SUM(CASE WHEN payment_method = 'E_WALLET' THEN amount ELSE 0 END), 0) as ewallet_paid
     FROM credit_payments
     WHERE date(paid_at) BETWEEN ? AND ?`
  ).get(monthStart, today);
  const monthCustomer = {
    total: Number(monthCustomerCredit.total || 0),
    paid: Number(monthCustomerPaid.paid || 0),
    credit: Number(monthCustomerCredit.credit || 0)
  };
  const monthPaidByMethod = {
    cash: Number(monthExportMethod.cash_paid || 0) + Number(monthCustomerMethod.cash_paid || 0),
    bank: Number(monthExportMethod.bank_paid || 0) + Number(monthCustomerMethod.bank_paid || 0),
    eWallet: Number(monthExportMethod.ewallet_paid || 0) + Number(monthCustomerMethod.ewallet_paid || 0)
  };
  const monthCombined = {
    paid: Number(monthExport.paid || 0) + monthCustomer.paid,
    credit: Number(monthExport.credit || 0) + Number(monthCustomer.credit || 0)
  };
  const outstandingTotals = db.prepare(
    `SELECT
        COALESCE(SUM(CASE WHEN amount - paid_amount < 0 THEN 0 ELSE amount - paid_amount END), 0) as customer_credit
     FROM credits`
  ).get();
  const vehicleOutstanding = db.prepare(
    `SELECT ${buildExportRemainingCreditAggregateSql("exports")} as vehicle_credit
     FROM exports
     JOIN vehicles ON vehicles.id = exports.vehicle_id
     WHERE vehicles.is_company = 0`
  ).get();
  const { lastBackupAt, backupDays, backupOverdue } = getBackupStatus();
  const backupReminderText =
    !lastBackupAt || backupDays === null ? req.t("backupNever") : `${backupDays} ${req.t("daysAgo")}`;
  const vehicleComplianceAlerts = listVehicleComplianceAlerts(db, { today });
  const topCredits = db.prepare(
    `SELECT credits.id, credits.credit_date, credits.customer_name, credits.amount, credits.paid_amount,
            vehicles.vehicle_number, vehicles.owner_name,
            CASE WHEN credits.amount - credits.paid_amount < 0 THEN 0 ELSE credits.amount - credits.paid_amount END AS remaining_amount
     FROM credits
     JOIN vehicles ON credits.vehicle_id = vehicles.id
     WHERE credits.amount - credits.paid_amount > 0
     ORDER BY remaining_amount DESC, credits.credit_date DESC
     LIMIT 5`
  ).all();
  const recentActivity = db.prepare(
    `SELECT activity_logs.action, activity_logs.entity_type, activity_logs.details, activity_logs.created_at,
            users.full_name as user_name
     FROM activity_logs
     LEFT JOIN users ON activity_logs.user_id = users.id
     ORDER BY activity_logs.created_at DESC
     LIMIT 8`
  ).all();
  const recentActivityRows = formatActivityRows(recentActivity, req.t);
  const collectionSummary = getPrintableCollectionSummary(today, today);
  res.render("admin/dashboard", {
    title: req.t("adminDashboard"),
    topCredits,
    recentActivity: recentActivityRows,
    collectionSummary,
    snapshotRanges: {
      daily: { from: today, to: today, date: today },
      weekly: { from: weekStart, to: today, date: today },
      monthly: { from: monthStart, to: today, date: today },
      yearly: { from: yearStart, to: today, date: today },
      all: { from: allTimeStart, to: today, date: today }
    },
    stats: {
      vehicleCount,
      workerCount,
      staffCount,
      todaySales,
      monthSales,
      todayExport,
      monthExport,
      monthCustomer,
      monthPaidByMethod,
      monthCombined,
      outstandingCustomerCredit: Number(outstandingTotals.customer_credit || 0),
      outstandingVehicleCredit: Number(vehicleOutstanding.vehicle_credit || 0),
      vehicleComplianceAlerts: vehicleComplianceAlerts.summary.totalAlerts,
      vehicleComplianceAffected: vehicleComplianceAlerts.summary.vehiclesAffected,
      vehicleComplianceDays: vehicleComplianceAlerts.dueSoonDays,
      lastBackupAt,
      backupDays,
      backupOverdue,
      backupReminderText
    }
  });
});

router.get("/alerts", (req, res) => {
  const alertData = getAlertSnapshot();
  res.render("admin/alerts", {
    title: req.t("alertCenterTitle"),
    ...alertData
  });
});

router.get("/recycle-bin", (req, res) => {
  const q = String(req.query.q || "").trim();
  const entityType = String(req.query.entity_type || "all").trim() || "all";
  const status = ["active", "restored", "all"].includes(req.query.status) ? req.query.status : "active";
  const rows = listRecycleEntries({ q, entityType, status });
  const bulkDeletedCount = Number(req.query.bulk_deleted || 0);
  const success = req.query.restored === "1"
    ? req.t("recycleRestored")
    : req.query.deleted === "1"
      ? req.t("recycleDeleted")
      : bulkDeletedCount > 0
        ? req.t("recycleBulkDeleted", { count: bulkDeletedCount })
      : null;
  const error = req.query.error ? req.t(req.query.error) : null;
  const entityTypes = db.prepare(
    "SELECT DISTINCT entity_type FROM recycle_bin ORDER BY entity_type"
  ).all().map((row) => row.entity_type);

  res.render("admin/recycle_bin", {
    title: req.t("recycleBinTitle"),
    q,
    entityType,
    status,
    rows,
    entityTypes,
    success,
    error
  });
});

router.post("/recycle-bin/:id/restore", (req, res) => {
  try {
    const result = restoreEntry(req.params.id, req.session.userId || null);
    logActivity({
      userId: req.session.userId,
      action: "restore",
      entityType: result.entityType || "recycle_bin",
      entityId: result.restoredEntityId || req.params.id,
      details: `recycle_id=${req.params.id}`
    });
    res.redirect("/admin/recycle-bin?restored=1");
  } catch (err) {
    const errorMap = {
      not_found: "recycleNotFound",
      already_restored: "recycleAlreadyRestored",
      expired: "recycleExpiredError",
      invalid_payload: "recycleInvalidPayload"
    };
    const errorKey = errorMap[err.message] || "restoreFailed";
    res.redirect(`/admin/recycle-bin?error=${encodeURIComponent(errorKey)}`);
  }
});

router.post("/recycle-bin/:id/delete", (req, res) => {
  const entry = getRecycleEntryById(req.params.id);
  if (!entry) return res.redirect("/admin/recycle-bin");
  removeRecycleEntry(req.params.id);
  logActivity({
    userId: req.session.userId,
    action: "delete",
    entityType: "recycle_bin",
    entityId: req.params.id,
    details: `entity_type=${entry.entity_type}, entity_id=${entry.entity_id || ""}`
  });
  res.redirect("/admin/recycle-bin?deleted=1");
});

router.post("/recycle-bin/delete-all", (req, res) => {
  const status = ["active", "restored", "all"].includes(String(req.body.status || "").trim())
    ? String(req.body.status || "").trim()
    : "all";
  const removedCount = removeAllRecycleEntries({ status });
  logActivity({
    userId: req.session.userId,
    action: "delete",
    entityType: "recycle_bin",
    entityId: "bulk",
    details: `status=${status}; removed=${removedCount}`
  });
  res.redirect(`/admin/recycle-bin?bulk_deleted=${removedCount}`);
});

router.get("/windows-kit", (req, res) => {
  const files = fs.existsSync(windowsKitDir) ? fs.readdirSync(windowsKitDir).sort() : [];
  const success = req.query.generated ? req.t("windowsKitGenerated") : null;
  const error = req.query.error ? req.t(req.query.error) : null;
  res.render("admin/windows_kit", {
    title: req.t("windowsKitTitle"),
    files,
    success,
    error
  });
});

router.post("/windows-kit/generate", (req, res) => {
  try {
    const result = buildWindowsKit();
    logActivity({
      userId: req.session.userId,
      action: "create",
      entityType: "windows_kit",
      entityId: "windows-kit",
      details: `files=${result.files.length}`
    });
    res.redirect("/admin/windows-kit?generated=1");
  } catch (err) {
    res.redirect("/admin/windows-kit?error=windowsKitFailed");
  }
});

router.get("/vehicles", (req, res) => {
  const includeInactive = String(req.query.include_inactive || "1") === "1";
  const whereParts = ["COALESCE(is_system, 0) = 0"];
  if (!includeInactive) whereParts.push("COALESCE(is_active, 1) = 1");
  const where = `WHERE ${whereParts.join(" AND ")}`;
  const vehicles = db.prepare(
    `SELECT *
     FROM vehicles
     ${where}
     ORDER BY COALESCE(is_active, 1) DESC, created_at DESC`
  ).all();
  const vehicleComplianceById = getVehicleComplianceMap(db, vehicles.map((vehicle) => vehicle.id));
  const success = req.query.archived
    ? req.t("vehicleArchived")
    : req.query.unarchived || req.query.activated
      ? req.t("vehicleActivated")
      : null;
  res.render("admin/vehicles", {
    title: req.t("vehicles"),
    vehicles,
    vehicleComplianceById,
    includeInactive,
    success
  });
});

router.get("/savings", (req, res) => {
  const from = req.query.from || dayjs().startOf("month").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const vehicleId = req.query.vehicle_id || "all";
  const vehicles = db.prepare(
    "SELECT id, vehicle_number, owner_name FROM vehicles WHERE is_company = 0 ORDER BY vehicle_number"
  ).all();

  const vehicleClause = vehicleId === "all" ? "" : "AND vehicle_savings.vehicle_id = ?";
  const params = vehicleId === "all" ? [from, to] : [from, to, vehicleId];
  const entries = db.prepare(
    `SELECT vehicle_savings.*, vehicles.vehicle_number, vehicles.owner_name, users.full_name as recorded_by
     FROM vehicle_savings
     JOIN vehicles ON vehicle_savings.vehicle_id = vehicles.id
     LEFT JOIN users ON vehicle_savings.created_by = users.id
     WHERE entry_date BETWEEN ? AND ?
     ${vehicleClause}
     ORDER BY entry_date DESC, created_at DESC`
  ).all(...params);

  const balances = db.prepare(
    `SELECT vehicle_id, COALESCE(SUM(amount), 0) as balance
     FROM vehicle_savings
     GROUP BY vehicle_id`
  ).all().reduce((acc, row) => {
    acc[row.vehicle_id] = Number(row.balance || 0);
    return acc;
  }, {});

  const totals = entries.reduce(
    (acc, row) => {
      const amt = Number(row.amount || 0);
      acc.total += amt;
      acc.deposits += amt > 0 ? amt : 0;
      acc.withdraws += amt < 0 ? Math.abs(amt) : 0;
      return acc;
    },
    { total: 0, deposits: 0, withdraws: 0 }
  );

  res.render("admin/savings", {
    title: req.t("savingsTitle"),
    from,
    to,
    vehicleId,
    vehicles,
    entries,
    balances,
    totals
  });
});

router.post("/savings", (req, res) => {
  const { vehicle_id, entry_date, amount, entry_type, payment_source, note } = req.body;
  if (!vehicle_id || !entry_date) {
    return res.redirect("/admin/savings");
  }
  let amt = Number(amount || 0);
  if (Number.isNaN(amt) || amt <= 0) {
    return res.redirect("/admin/savings");
  }
  const type = entry_type === "withdraw" ? "withdraw" : "deposit";
  if (type === "withdraw") amt = -Math.abs(amt);
  const source = type === "withdraw" ? normalizeSalaryPaymentSource(payment_source) : "DAILY_COLLECTION";
  if (type === "withdraw" && source === "DAILY_COLLECTION") {
    const collectionBalance = getDailyCollectionBalance(db, entry_date);
    if (collectionBalance < Math.abs(amt)) {
      return res.redirect(`/admin/savings?from=${entry_date}&to=${entry_date}&vehicle_id=${vehicle_id}&error=dailyCollectionInsufficient`);
    }
  }

  db.prepare(
    "INSERT INTO vehicle_savings (vehicle_id, entry_date, amount, payment_source, note, created_by) VALUES (?, ?, ?, ?, ?, ?)"
  ).run(vehicle_id, entry_date, amt, source, note || null, req.session.userId);

  logActivity({
    userId: req.session.userId,
    action: "create",
    entityType: "vehicle_savings",
    entityId: `${vehicle_id}_${entry_date}`,
    details: `type=${type}, source=${source}, amount=${amt}`
  });

  res.redirect(`/admin/savings?from=${entry_date}&to=${entry_date}&vehicle_id=${vehicle_id}`);
});

const computeSalaryDue = (personType, personRow, paidTotal, asOf) => computeSalaryDueWithHistory(db, {
  personType,
  personRow,
  paidTotal,
  asOf
});

const getStaffPaidTotal = (staffId, { excludePaymentId = null } = {}) => {
  if (!staffId) return 0;
  const row = excludePaymentId
    ? db.prepare(
      "SELECT COALESCE(SUM(amount), 0) as paid_total FROM staff_salary_payments WHERE staff_id = ? AND id != ?"
    ).get(staffId, excludePaymentId)
    : db.prepare(
      "SELECT COALESCE(SUM(amount), 0) as paid_total FROM staff_salary_payments WHERE staff_id = ?"
    ).get(staffId);
  return roundMoney(row?.paid_total || 0);
};

const getWorkerPaidTotal = (workerId, { excludePaymentId = null } = {}) => {
  if (!workerId) return 0;
  const row = excludePaymentId
    ? db.prepare(
      "SELECT COALESCE(SUM(amount), 0) as paid_total FROM worker_salary_payments WHERE worker_id = ? AND id != ?"
    ).get(workerId, excludePaymentId)
    : db.prepare(
      "SELECT COALESCE(SUM(amount), 0) as paid_total FROM worker_salary_payments WHERE worker_id = ?"
    ).get(workerId);
  return roundMoney(row?.paid_total || 0);
};

const parseMonthToken = (value) => {
  const safe = String(value || "").trim();
  return /^\d{4}-(0[1-9]|1[0-2])$/.test(safe) ? safe : dayjs().format("YYYY-MM");
};

const buildPayrollRows = (rows, attendanceMap, monthStart, prevMonthStart, personType) => rows.map((row) => {
  const adjustments = getSalaryAdjustments(db, personType, row.id);
  const monthlySalary = getMonthlySalaryForMonth(db, {
    personType,
    personRow: row,
    month: monthStart,
    adjustments
  });
  const accruedBefore = computeAccruedSalaryUntilMonth(db, {
    personType,
    personRow: row,
    month: prevMonthStart,
    adjustments
  });
  const accruedNow = computeAccruedSalaryUntilMonth(db, {
    personType,
    personRow: row,
    month: monthStart,
    adjustments
  });
  const monthAccrued = Math.max(0, roundMoney(accruedNow - accruedBefore));
  const paidBefore = roundMoney(row.paid_before || 0);
  const salaryPaid = roundMoney(row.salary_paid_month || 0);
  const advancePaid = roundMoney(row.advance_paid_month || 0);
  const monthPaid = roundMoney(salaryPaid + advancePaid);
  const openingNet = roundMoney(accruedBefore - paidBefore);
  const openingDue = Math.max(0, openingNet);
  const openingAdvance = Math.max(0, roundMoney(-openingNet));
  const closingNet = roundMoney(openingNet + monthAccrued - monthPaid);
  const closingDue = Math.max(0, closingNet);
  const closingAdvance = Math.max(0, roundMoney(-closingNet));
  const attendance = attendanceMap.get(Number(row.id)) || { present: 0, absent: 0 };
  return {
    ...row,
    person_type: personType,
    monthly_salary: monthlySalary,
    paid_before: paidBefore,
    salary_paid_month: salaryPaid,
    advance_paid_month: advancePaid,
    month_paid: monthPaid,
    month_accrued: monthAccrued,
    opening_due: openingDue,
    opening_advance: openingAdvance,
    closing_due: closingDue,
    closing_advance: closingAdvance,
    present_days: Number(attendance.present || 0),
    absent_days: Number(attendance.absent || 0)
  };
});

const summarizePayrollRows = (rows) => rows.reduce((acc, row) => {
  acc.count += 1;
  acc.monthAccrued += roundMoney(row.month_accrued || 0);
  acc.salaryPaid += roundMoney(row.salary_paid_month || 0);
  acc.advancePaid += roundMoney(row.advance_paid_month || 0);
  acc.monthPaid += roundMoney(row.month_paid || 0);
  acc.openingDue += roundMoney(row.opening_due || 0);
  acc.closingDue += roundMoney(row.closing_due || 0);
  acc.openingAdvance += roundMoney(row.opening_advance || 0);
  acc.closingAdvance += roundMoney(row.closing_advance || 0);
  acc.presentDays += Number(row.present_days || 0);
  acc.absentDays += Number(row.absent_days || 0);
  return acc;
}, {
  count: 0,
  monthAccrued: 0,
  salaryPaid: 0,
  advancePaid: 0,
  monthPaid: 0,
  openingDue: 0,
  closingDue: 0,
  openingAdvance: 0,
  closingAdvance: 0,
  presentDays: 0,
  absentDays: 0
});

const getPayrollSummaryPayload = (monthToken) => {
  const safeMonth = parseMonthToken(monthToken);
  const monthStart = dayjs(`${safeMonth}-01`).startOf("month");
  const monthEnd = monthStart.endOf("month");
  const prevMonthStart = monthStart.subtract(1, "month");
  const from = monthStart.format("YYYY-MM-DD");
  const to = monthEnd.format("YYYY-MM-DD");

  const staffRowsRaw = db.prepare(
    `SELECT staff.id, staff.full_name, staff.start_date, staff.monthly_salary, staff.staff_role,
            COALESCE(SUM(CASE WHEN staff_salary_payments.payment_date < ? THEN staff_salary_payments.amount ELSE 0 END), 0) as paid_before,
            COALESCE(SUM(CASE WHEN staff_salary_payments.payment_date BETWEEN ? AND ? AND staff_salary_payments.payment_type = 'SALARY' THEN staff_salary_payments.amount ELSE 0 END), 0) as salary_paid_month,
            COALESCE(SUM(CASE WHEN staff_salary_payments.payment_date BETWEEN ? AND ? AND staff_salary_payments.payment_type = 'ADVANCE' THEN staff_salary_payments.amount ELSE 0 END), 0) as advance_paid_month
     FROM staff
     LEFT JOIN staff_salary_payments ON staff_salary_payments.staff_id = staff.id
     WHERE COALESCE(staff.is_active, 1) = 1
        OR EXISTS (
          SELECT 1
          FROM staff_salary_payments paid_in_month
          WHERE paid_in_month.staff_id = staff.id
            AND paid_in_month.payment_date BETWEEN ? AND ?
        )
     GROUP BY staff.id
     ORDER BY staff.full_name ASC`
  ).all(from, from, to, from, to, from, to);

  const workerRowsRaw = db.prepare(
    `SELECT users.id, users.full_name, users.start_date, users.monthly_salary,
            COALESCE(SUM(CASE WHEN worker_salary_payments.payment_date < ? THEN worker_salary_payments.amount ELSE 0 END), 0) as paid_before,
            COALESCE(SUM(CASE WHEN worker_salary_payments.payment_date BETWEEN ? AND ? AND worker_salary_payments.payment_type = 'SALARY' THEN worker_salary_payments.amount ELSE 0 END), 0) as salary_paid_month,
            COALESCE(SUM(CASE WHEN worker_salary_payments.payment_date BETWEEN ? AND ? AND worker_salary_payments.payment_type = 'ADVANCE' THEN worker_salary_payments.amount ELSE 0 END), 0) as advance_paid_month
     FROM users
     LEFT JOIN worker_salary_payments ON worker_salary_payments.worker_id = users.id
     WHERE users.role = 'WORKER'
       AND (
         users.is_active = 1
         OR EXISTS (
           SELECT 1
           FROM worker_salary_payments paid_in_month
           WHERE paid_in_month.worker_id = users.id
             AND paid_in_month.payment_date BETWEEN ? AND ?
         )
       )
     GROUP BY users.id
     ORDER BY users.full_name ASC`
  ).all(from, from, to, from, to, from, to);

  const staffAttendanceMap = new Map();
  db.prepare(
    `SELECT staff_id,
            SUM(CASE WHEN status = 'PRESENT' THEN 1 ELSE 0 END) as present_days,
            SUM(CASE WHEN status = 'ABSENT' THEN 1 ELSE 0 END) as absent_days
     FROM staff_attendance
     WHERE attendance_date BETWEEN ? AND ?
     GROUP BY staff_id`
  ).all(from, to).forEach((row) => {
    staffAttendanceMap.set(Number(row.staff_id), {
      present: Number(row.present_days || 0),
      absent: Number(row.absent_days || 0)
    });
  });

  const workerAttendanceMap = new Map();
  db.prepare(
    `SELECT user_id,
            SUM(CASE WHEN status = 'PRESENT' THEN 1 ELSE 0 END) as present_days,
            SUM(CASE WHEN status = 'ABSENT' THEN 1 ELSE 0 END) as absent_days
     FROM user_attendance
     WHERE attendance_date BETWEEN ? AND ?
     GROUP BY user_id`
  ).all(from, to).forEach((row) => {
    workerAttendanceMap.set(Number(row.user_id), {
      present: Number(row.present_days || 0),
      absent: Number(row.absent_days || 0)
    });
  });

  const staffRows = buildPayrollRows(staffRowsRaw, staffAttendanceMap, monthStart, prevMonthStart, "STAFF");
  const workerRows = buildPayrollRows(workerRowsRaw, workerAttendanceMap, monthStart, prevMonthStart, "WORKER");
  const staffTotals = summarizePayrollRows(staffRows);
  const workerTotals = summarizePayrollRows(workerRows);
  const grandTotals = {
    count: staffTotals.count + workerTotals.count,
    monthAccrued: roundMoney(staffTotals.monthAccrued + workerTotals.monthAccrued),
    salaryPaid: roundMoney(staffTotals.salaryPaid + workerTotals.salaryPaid),
    advancePaid: roundMoney(staffTotals.advancePaid + workerTotals.advancePaid),
    monthPaid: roundMoney(staffTotals.monthPaid + workerTotals.monthPaid),
    openingDue: roundMoney(staffTotals.openingDue + workerTotals.openingDue),
    closingDue: roundMoney(staffTotals.closingDue + workerTotals.closingDue),
    openingAdvance: roundMoney(staffTotals.openingAdvance + workerTotals.openingAdvance),
    closingAdvance: roundMoney(staffTotals.closingAdvance + workerTotals.closingAdvance),
    presentDays: staffTotals.presentDays + workerTotals.presentDays,
    absentDays: staffTotals.absentDays + workerTotals.absentDays
  };

  return {
    monthToken: safeMonth,
    from,
    to,
    staffRows,
    workerRows,
    staffTotals,
    workerTotals,
    grandTotals
  };
};

const humanizeRoleCode = (code) => String(code || "")
  .trim()
  .replace(/_/g, " ")
  .toLowerCase()
  .replace(/\b\w/g, (char) => char.toUpperCase());

const resolveStaffRoleLabel = (roleCode, roleName, t) => {
  const safeCode = String(roleCode || "").trim().toUpperCase();
  if (roleName && String(roleName).trim()) return String(roleName).trim();
  if (!safeCode) return "-";
  const key = `staffRole${safeCode}`;
  const translated = t(key);
  if (translated !== key) return translated;
  return humanizeRoleCode(safeCode);
};

const getStaffRoles = ({ includeInactive = false } = {}) => {
  const where = includeInactive ? "" : "WHERE is_active = 1";
  return db.prepare(
    `SELECT id, code, name, is_active
     FROM staff_roles
     ${where}
     ORDER BY name ASC, code ASC`
  ).all();
};

const getStaffRoleChoices = (t, selectedCode = null) => {
  const safeSelected = String(selectedCode || "").trim().toUpperCase();
  const rows = getStaffRoles();
  const choices = rows.map((row) => ({
    code: row.code,
    label: resolveStaffRoleLabel(row.code, row.name, t)
  }));
  if (!safeSelected) return choices;
  if (choices.find((row) => row.code === safeSelected)) return choices;
  const extra = db.prepare("SELECT code, name FROM staff_roles WHERE code = ?").get(safeSelected);
  if (extra) {
    return [...choices, { code: extra.code, label: resolveStaffRoleLabel(extra.code, extra.name, t) }];
  }
  return [...choices, { code: safeSelected, label: resolveStaffRoleLabel(safeSelected, null, t) }];
};

const buildStaffRoleCode = (name) => {
  const base = String(name || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "") || "STAFF_ROLE";
  let code = base;
  let suffix = 2;
  while (db.prepare("SELECT id FROM staff_roles WHERE code = ?").get(code)) {
    code = `${base}_${suffix}`;
    suffix += 1;
  }
  return code;
};

const normalizeStaffRole = (value, options = {}) => {
  const allowInactive = Boolean(options.allowInactive);
  const safe = String(value || "").trim().toUpperCase();
  if (!safe) return null;
  const sql = allowInactive
    ? "SELECT code FROM staff_roles WHERE code = ?"
    : "SELECT code FROM staff_roles WHERE code = ? AND is_active = 1";
  const exists = db.prepare(sql).get(safe);
  if (exists) return safe;
  if (defaultStaffRoleCodes.includes(safe)) return safe;
  return null;
};

const upsertWorkerDocument = ({ workerId, existingDocument, files, docTypeValue }) => {
  const docType = normalizeDocumentType(docTypeValue) || (existingDocument ? existingDocument.doc_type : null);
  const photoFile = files && files.photo ? files.photo[0] : null;
  const docFront = files && files.doc_front ? files.doc_front[0] : null;
  const docBack = files && files.doc_back ? files.doc_back[0] : null;
  const docSingle = files && files.doc_single ? files.doc_single[0] : null;

  const photoPath = photoFile ? `/uploads/${photoFile.filename}` : existingDocument ? existingDocument.photo_path : null;
  let frontPath = existingDocument ? existingDocument.front_path : null;
  let backPath = existingDocument ? existingDocument.back_path : null;

  if (docType === "CITIZENSHIP") {
    if (docFront) frontPath = `/uploads/${docFront.filename}`;
    if (docBack) backPath = `/uploads/${docBack.filename}`;
  } else if (docType) {
    if (docSingle) {
      frontPath = `/uploads/${docSingle.filename}`;
    } else if (docFront) {
      frontPath = `/uploads/${docFront.filename}`;
    }
    backPath = null;
  }

  const hasAnyContent = Boolean(docType || photoPath || frontPath || backPath);
  if (!hasAnyContent) return;

  if (existingDocument) {
    db.prepare(
      `UPDATE worker_documents
       SET doc_type = ?, photo_path = ?, front_path = ?, back_path = ?, updated_at = datetime('now')
       WHERE worker_id = ?`
    ).run(docType, photoPath, frontPath, backPath, workerId);
  } else {
    db.prepare(
      "INSERT INTO worker_documents (worker_id, doc_type, photo_path, front_path, back_path) VALUES (?, ?, ?, ?, ?)"
    ).run(workerId, docType, photoPath, frontPath, backPath);
  }
};

router.get("/staff-roles", (req, res) => {
  const rows = db.prepare(
    `SELECT staff_roles.*, COUNT(staff.id) as staff_count
     FROM staff_roles
     LEFT JOIN staff ON staff.staff_role = staff_roles.code
     GROUP BY staff_roles.id
     ORDER BY staff_roles.name ASC, staff_roles.code ASC`
  ).all();
  const roles = rows.map((row) => ({
    ...row,
    label: resolveStaffRoleLabel(row.code, row.name, req.t)
  }));
  const success = req.query.saved
    ? req.t("staffRoleSaved")
    : req.query.deleted
      ? req.t("staffRoleDeleted")
      : null;
  const error = req.query.error ? req.t(req.query.error) : null;
  res.render("admin/staff_roles", {
    title: req.t("staffRolesTitle"),
    roles,
    success,
    error
  });
});

router.post("/staff-roles", (req, res) => {
  const { name, is_active, show_in_exports } = req.body;
  const roleName = String(name || "").trim();
  if (!roleName) return res.redirect("/admin/staff-roles?error=staffRoleRequired");
  const roleCode = buildStaffRoleCode(roleName);
  const activeFlag = is_active === "on" ? 1 : 0;
  const showInExports = show_in_exports === "on" ? 1 : 0;
  const roleId = db.prepare(
    "INSERT INTO staff_roles (code, name, is_active, show_in_exports) VALUES (?, ?, ?, ?)"
  ).run(roleCode, roleName, activeFlag, showInExports).lastInsertRowid;
  logActivity({
    userId: req.session.userId,
    action: "create",
    entityType: "staff_role",
    entityId: roleId,
    details: `code=${roleCode}, name=${roleName}, active=${activeFlag}, show_in_exports=${showInExports}`
  });
  res.redirect("/admin/staff-roles?saved=1");
});

router.post("/staff-roles/:id", (req, res) => {
  const role = db.prepare("SELECT * FROM staff_roles WHERE id = ?").get(req.params.id);
  if (!role) return res.redirect("/admin/staff-roles?error=staffRoleNotFound");
  const roleName = String(req.body.name || "").trim();
  if (!roleName) return res.redirect("/admin/staff-roles?error=staffRoleRequired");
  const activeFlag = req.body.is_active === "on" ? 1 : 0;
  const showInExports = req.body.show_in_exports === "on" ? 1 : 0;
  db.prepare(
    "UPDATE staff_roles SET name = ?, is_active = ?, show_in_exports = ?, updated_at = datetime('now') WHERE id = ?"
  ).run(roleName, activeFlag, showInExports, req.params.id);
  logActivity({
    userId: req.session.userId,
    action: "update",
    entityType: "staff_role",
    entityId: req.params.id,
    details: `code=${role.code}, name=${roleName}, active=${activeFlag}, show_in_exports=${showInExports}`
  });
  res.redirect("/admin/staff-roles?saved=1");
});

router.post("/staff-roles/:id/delete", (req, res) => {
  const role = db.prepare("SELECT * FROM staff_roles WHERE id = ?").get(req.params.id);
  if (!role) return res.redirect("/admin/staff-roles?error=staffRoleNotFound");
  const usageCount = db.prepare("SELECT COUNT(*) as count FROM staff WHERE staff_role = ?").get(role.code).count || 0;
  if (usageCount > 0) {
    db.prepare(
      "UPDATE staff_roles SET is_active = 0, updated_at = datetime('now') WHERE id = ?"
    ).run(req.params.id);
    return res.redirect("/admin/staff-roles?error=staffRoleInUse");
  }
  db.prepare("DELETE FROM staff_roles WHERE id = ?").run(req.params.id);
  logActivity({
    userId: req.session.userId,
    action: "delete",
    entityType: "staff_role",
    entityId: req.params.id,
    details: `code=${role.code}`
  });
  res.redirect("/admin/staff-roles?deleted=1");
});

router.get("/staffs", (req, res) => {
  const includeInactive = String(req.query.include_inactive || "1") === "1";
  const q = String(req.query.q || "").trim();
  const whereParts = [];
  const params = [];
  if (!includeInactive) {
    whereParts.push("COALESCE(staff.is_active, 1) = 1");
  }
  if (q) {
    whereParts.push("(staff.full_name LIKE ? OR COALESCE(staff.phone, '') LIKE ? OR COALESCE(staff.staff_role, '') LIKE ?)");
    params.push(`%${q}%`, `%${q}%`, `%${q}%`);
  }
  const whereClause = whereParts.length > 0 ? `WHERE ${whereParts.join(" AND ")}` : "";
  const rows = db.prepare(
    `SELECT staff.*, staff_roles.name as role_name, COALESCE(SUM(staff_salary_payments.amount), 0) AS paid_total
     FROM staff
     LEFT JOIN staff_roles ON staff_roles.code = staff.staff_role
     LEFT JOIN staff_salary_payments ON staff_salary_payments.staff_id = staff.id
     ${whereClause}
     GROUP BY staff.id
     ORDER BY COALESCE(staff.is_active, 1) DESC, staff.created_at DESC`
  ).all(...params);

  const today = dayjs().format("YYYY-MM-DD");
  const staffs = rows.map((row) => ({
    ...row,
    role_label: resolveStaffRoleLabel(row.staff_role, row.role_name, req.t),
    due_salary: computeSalaryDue("STAFF", row, row.paid_total, today)
  }));

  const success = req.query.archived
    ? req.t("staffArchived")
    : req.query.unarchived || req.query.activated
      ? req.t("staffActivated")
      : null;

  res.render("admin/staffs", {
    title: req.t("staffsTitle"),
    staffs,
    basePath: "/admin/staffs",
    includeInactive,
    q,
    success
  });
});

router.get("/staffs/new", (req, res) => {
  res.render("admin/staff_form", {
    title: req.t("addStaffTitle"),
    staff: null,
    document: null,
    error: null,
    staffRoles: getStaffRoleChoices(req.t),
    basePath: "/admin/staffs"
  });
});

router.post("/staffs", (req, res) => {
  const upload = staffUpload.fields([
    { name: "photo", maxCount: 1 },
    { name: "doc_front", maxCount: 1 },
    { name: "doc_back", maxCount: 1 },
    { name: "doc_single", maxCount: 1 }
  ]);

  upload(req, res, (err) => {
    if (err) {
      return res.render("admin/staff_form", {
        title: req.t("addStaffTitle"),
        staff: null,
        document: null,
        error: err.message || req.t("uploadError"),
        staffRoles: getStaffRoleChoices(req.t, req.body ? req.body.staff_role : null),
        basePath: "/admin/staffs"
      });
    }

    const { full_name, phone, fingerprint_id, start_date, monthly_salary, doc_type, staff_role } = req.body;
    if (!full_name) {
      return res.render("admin/staff_form", {
        title: req.t("addStaffTitle"),
        staff: null,
        document: null,
        error: req.t("staffRequired"),
        staffRoles: getStaffRoleChoices(req.t, staff_role),
        basePath: "/admin/staffs"
      });
    }
    const salaryRaw = Number(monthly_salary || 0);
    if (Number.isNaN(salaryRaw) || salaryRaw < 0) {
      return res.render("admin/staff_form", {
        title: req.t("addStaffTitle"),
        staff: null,
        document: null,
        error: req.t("salaryInvalid"),
        staffRoles: getStaffRoleChoices(req.t, staff_role),
        basePath: "/admin/staffs"
      });
    }
    const salary = parseWholeMoney(salaryRaw);
    const fingerprintId = normalizeFingerprintId(fingerprint_id);
    const fpConflict = findFingerprintConflict({ fingerprintId });
    if (fpConflict) {
      return res.render("admin/staff_form", {
        title: req.t("addStaffTitle"),
        staff: null,
        document: null,
        error: req.t("fingerprintIdAlreadyUsed"),
        staffRoles: getStaffRoleChoices(req.t, staff_role),
        basePath: "/admin/staffs"
      });
    }
    const staffRole = normalizeStaffRole(staff_role);
    const safeDocType = normalizeDocumentType(doc_type);

    const photoFile = req.files && req.files.photo ? req.files.photo[0] : null;
    const photoPath = photoFile ? `/uploads/${photoFile.filename}` : null;

    const staffId = db.prepare(
      "INSERT INTO staff (full_name, staff_role, phone, fingerprint_id, photo_path, start_date, monthly_salary) VALUES (?, ?, ?, ?, ?, ?, ?)"
    ).run(full_name.trim(), staffRole, phone || null, fingerprintId, photoPath, start_date || null, salary).lastInsertRowid;

    if (safeDocType) {
      let frontPath = null;
      let backPath = null;
      if (safeDocType === "CITIZENSHIP") {
        const front = req.files && req.files.doc_front ? req.files.doc_front[0] : null;
        const back = req.files && req.files.doc_back ? req.files.doc_back[0] : null;
        if (front) frontPath = `/uploads/${front.filename}`;
        if (back) backPath = `/uploads/${back.filename}`;
      } else {
        const single = req.files && req.files.doc_single ? req.files.doc_single[0] : null;
        if (single) frontPath = `/uploads/${single.filename}`;
      }
      if (frontPath || backPath) {
        db.prepare(
          "INSERT INTO staff_documents (staff_id, doc_type, front_path, back_path) VALUES (?, ?, ?, ?)"
        ).run(staffId, safeDocType, frontPath, backPath);
      }
    }

    logActivity({
      userId: req.session.userId,
      action: "create",
      entityType: "staff",
      entityId: staffId,
      details: `name=${full_name}`
    });

    res.redirect("/admin/staffs");
  });
});

router.get("/staffs/:id", (req, res) => {
  const staffRow = db.prepare(
    `SELECT staff.*, staff_roles.name as role_name
     FROM staff
     LEFT JOIN staff_roles ON staff_roles.code = staff.staff_role
     WHERE staff.id = ?`
  ).get(req.params.id);
  const staff = staffRow
    ? { ...staffRow, role_label: resolveStaffRoleLabel(staffRow.staff_role, staffRow.role_name, req.t) }
    : null;
  if (!staff) return res.redirect("/admin/staffs");
  const document = db.prepare("SELECT * FROM staff_documents WHERE staff_id = ?").get(req.params.id);
  const payments = db.prepare(
    `SELECT staff_salary_payments.*, users.full_name as recorded_by
     FROM staff_salary_payments
     LEFT JOIN users ON staff_salary_payments.created_by = users.id
     WHERE staff_salary_payments.staff_id = ?
     ORDER BY staff_salary_payments.payment_date DESC, staff_salary_payments.id DESC`
  ).all(req.params.id);
  const totals = payments.reduce(
    (acc, row) => {
      const amt = Number(row.amount || 0);
      acc.total += amt;
      if (row.payment_type === "SALARY") acc.salary += amt;
      if (row.payment_type === "ADVANCE") acc.advance += amt;
      return acc;
    },
    { total: 0, salary: 0, advance: 0 }
  );
  const dueSalary = computeSalaryDue("STAFF", staff, totals.total, dayjs().format("YYYY-MM-DD"));
  const todayDate = dayjs().format("YYYY-MM-DD");
  const attendanceSummary = db.prepare(
    `SELECT
      COALESCE(SUM(CASE WHEN status = 'PRESENT' THEN 1 ELSE 0 END), 0) AS present_days,
      COALESCE(SUM(CASE WHEN status = 'ABSENT' THEN 1 ELSE 0 END), 0) AS absent_days,
      COUNT(*) AS marked_days
     FROM staff_attendance
     WHERE staff_id = ?`
  ).get(req.params.id);
  const absentDates = db.prepare(
    `SELECT attendance_date
     FROM staff_attendance
     WHERE staff_id = ?
       AND status = 'ABSENT'
     ORDER BY attendance_date DESC`
  ).all(req.params.id).map((row) => row.attendance_date);
  const errorKey = String(req.query.error || "").trim();
  const status = String(req.query.status || "").trim();
  const error = errorKey ? req.t(errorKey) : null;
  const success = status === "salary_adjusted" ? req.t("salaryAdjusted") : null;

  res.render("admin/staff_detail", {
    title: req.t("staffDetailTitle"),
    staff,
    document,
    payments,
    totals,
    dueSalary,
    todayDate,
    attendanceSummary,
    absentDates,
    error,
    success,
    basePath: "/admin/staffs"
  });
});

router.post("/staffs/:id/salary-adjust", (req, res) => {
  const staff = db.prepare("SELECT id, full_name, monthly_salary FROM staff WHERE id = ?").get(req.params.id);
  if (!staff) return res.redirect("/admin/staffs");

  const adjustType = String(req.body.adjust_type || "").trim().toUpperCase();
  const adjustAmountRaw = Number(req.body.adjust_amount || 0);
  const adjustAmount = parseWholeMoney(adjustAmountRaw);
  const note = String(req.body.note || "").trim();
  if (!["INCREASE", "DECREASE"].includes(adjustType) || Number.isNaN(adjustAmountRaw) || adjustAmount <= 0) {
    return res.redirect(`/admin/staffs/${req.params.id}?error=salaryAdjustmentInvalid`);
  }

  const currentSalary = parseWholeMoney(staff.monthly_salary || 0);
  const nextSalary = adjustType === "INCREASE"
    ? currentSalary + adjustAmount
    : currentSalary - adjustAmount;
  if (nextSalary < 0) {
    return res.redirect(`/admin/staffs/${req.params.id}?error=salaryAdjustmentWouldBeNegative`);
  }

  runInTransaction(() => {
    recordSalaryAdjustment(db, {
      personType: "STAFF",
      personId: req.params.id,
      previousSalary: currentSalary,
      newSalary: nextSalary,
      effectiveMonth: dayjs().startOf("month").format("YYYY-MM-DD"),
      note,
      createdBy: req.session.userId
    });
    db.prepare("UPDATE staff SET monthly_salary = ?, updated_at = datetime('now') WHERE id = ?").run(nextSalary, req.params.id);
  });
  logActivity({
    userId: req.session.userId,
    action: "update",
    entityType: "staff_salary_adjustment",
    entityId: req.params.id,
    details: `type=${adjustType}, amount=${adjustAmount}, from=${currentSalary}, to=${nextSalary}, note=${note || ""}`
  });

  return res.redirect(`/admin/staffs/${req.params.id}?status=salary_adjusted`);
});

router.get("/staffs/:id/print", (req, res) => {
  const staffRow = db.prepare(
    `SELECT staff.*, staff_roles.name as role_name
     FROM staff
     LEFT JOIN staff_roles ON staff_roles.code = staff.staff_role
     WHERE staff.id = ?`
  ).get(req.params.id);
  const staff = staffRow
    ? { ...staffRow, role_label: resolveStaffRoleLabel(staffRow.staff_role, staffRow.role_name, req.t) }
    : null;
  if (!staff) return res.redirect("/admin/staffs");
  const document = db.prepare("SELECT * FROM staff_documents WHERE staff_id = ?").get(req.params.id);
  const payments = db.prepare(
    `SELECT staff_salary_payments.*, users.full_name as recorded_by
     FROM staff_salary_payments
     LEFT JOIN users ON staff_salary_payments.created_by = users.id
     WHERE staff_salary_payments.staff_id = ?
     ORDER BY staff_salary_payments.payment_date DESC, staff_salary_payments.id DESC`
  ).all(req.params.id);
  const totals = payments.reduce(
    (acc, row) => {
      const amt = Number(row.amount || 0);
      acc.total += amt;
      if (row.payment_type === "SALARY") acc.salary += amt;
      if (row.payment_type === "ADVANCE") acc.advance += amt;
      return acc;
    },
    { total: 0, salary: 0, advance: 0 }
  );
  const dueSalary = computeSalaryDue("STAFF", staff, totals.total, dayjs().format("YYYY-MM-DD"));
  const attendanceSummary = db.prepare(
    `SELECT
      COALESCE(SUM(CASE WHEN status = 'PRESENT' THEN 1 ELSE 0 END), 0) AS present_days,
      COALESCE(SUM(CASE WHEN status = 'ABSENT' THEN 1 ELSE 0 END), 0) AS absent_days,
      COUNT(*) AS marked_days
     FROM staff_attendance
     WHERE staff_id = ?`
  ).get(req.params.id);
  const absentDates = db.prepare(
    `SELECT attendance_date
     FROM staff_attendance
     WHERE staff_id = ?
       AND status = 'ABSENT'
     ORDER BY attendance_date DESC`
  ).all(req.params.id).map((row) => row.attendance_date);

  res.render("admin/staff_detail_print", {
    title: req.t("staffDetailPrintTitle"),
    staff,
    document,
    payments,
    totals,
    dueSalary,
    attendanceSummary,
    absentDates
  });
});

router.get("/staffs/:id/edit", (req, res) => {
  const staff = db.prepare("SELECT * FROM staff WHERE id = ?").get(req.params.id);
  if (!staff) return res.redirect("/admin/staffs");
  const document = db.prepare("SELECT * FROM staff_documents WHERE staff_id = ?").get(req.params.id);
  res.render("admin/staff_form", {
    title: req.t("editStaffTitle"),
    staff,
    document,
    error: null,
    staffRoles: getStaffRoleChoices(req.t, staff.staff_role),
    basePath: "/admin/staffs"
  });
});

router.post("/staffs/:id", (req, res) => {
  const upload = staffUpload.fields([
    { name: "photo", maxCount: 1 },
    { name: "doc_front", maxCount: 1 },
    { name: "doc_back", maxCount: 1 },
    { name: "doc_single", maxCount: 1 }
  ]);

  upload(req, res, (err) => {
    const staff = db.prepare("SELECT * FROM staff WHERE id = ?").get(req.params.id);
    if (!staff) return res.redirect("/admin/staffs");
    const document = db.prepare("SELECT * FROM staff_documents WHERE staff_id = ?").get(req.params.id);
    if (err) {
      return res.render("admin/staff_form", {
        title: req.t("editStaffTitle"),
        staff,
        document,
        error: err.message || req.t("uploadError"),
        staffRoles: getStaffRoleChoices(req.t, req.body ? req.body.staff_role : staff.staff_role),
        basePath: "/admin/staffs"
      });
    }

    const { full_name, phone, fingerprint_id, start_date, monthly_salary, doc_type, staff_role } = req.body;
    if (!full_name) {
      return res.render("admin/staff_form", {
        title: req.t("editStaffTitle"),
        staff,
        document,
        error: req.t("staffRequired"),
        staffRoles: getStaffRoleChoices(req.t, staff_role || staff.staff_role),
        basePath: "/admin/staffs"
      });
    }
    const salaryRaw = Number(monthly_salary || 0);
    if (Number.isNaN(salaryRaw) || salaryRaw < 0) {
      return res.render("admin/staff_form", {
        title: req.t("editStaffTitle"),
        staff,
        document,
        error: req.t("salaryInvalid"),
        staffRoles: getStaffRoleChoices(req.t, staff_role || staff.staff_role),
        basePath: "/admin/staffs"
      });
    }
    const salary = parseWholeMoney(salaryRaw);
    const fingerprintId = normalizeFingerprintId(fingerprint_id);
    const fpConflict = findFingerprintConflict({ fingerprintId, skipStaffId: req.params.id });
    if (fpConflict) {
      return res.render("admin/staff_form", {
        title: req.t("editStaffTitle"),
        staff,
        document,
        error: req.t("fingerprintIdAlreadyUsed"),
        staffRoles: getStaffRoleChoices(req.t, staff_role || staff.staff_role),
        basePath: "/admin/staffs"
      });
    }
    const staffRole = normalizeStaffRole(staff_role, { allowInactive: true });
    const safeDocType = normalizeDocumentType(doc_type);

    let photoPath = staff.photo_path;
    const photoFile = req.files && req.files.photo ? req.files.photo[0] : null;
    if (photoFile) photoPath = `/uploads/${photoFile.filename}`;

    const currentSalary = parseWholeMoney(staff.monthly_salary || 0);
    runInTransaction(() => {
      if (currentSalary !== salary) {
        recordSalaryAdjustment(db, {
          personType: "STAFF",
          personId: req.params.id,
          previousSalary: currentSalary,
          newSalary: salary,
          effectiveMonth: dayjs().startOf("month").format("YYYY-MM-DD"),
          note: "Updated from staff profile",
          createdBy: req.session.userId
        });
      }
      db.prepare(
        "UPDATE staff SET full_name = ?, staff_role = ?, phone = ?, fingerprint_id = ?, photo_path = ?, start_date = ?, monthly_salary = ?, updated_at = datetime('now') WHERE id = ?"
      ).run(full_name.trim(), staffRole, phone || null, fingerprintId, photoPath, start_date || null, salary, req.params.id);
    });

    if (safeDocType) {
      let frontPath = document ? document.front_path : null;
      let backPath = document ? document.back_path : null;
      if (safeDocType === "CITIZENSHIP") {
        const front = req.files && req.files.doc_front ? req.files.doc_front[0] : null;
        const back = req.files && req.files.doc_back ? req.files.doc_back[0] : null;
        if (front) frontPath = `/uploads/${front.filename}`;
        if (back) backPath = `/uploads/${back.filename}`;
      } else {
        const single = req.files && req.files.doc_single ? req.files.doc_single[0] : null;
        if (single) {
          frontPath = `/uploads/${single.filename}`;
          backPath = null;
        }
      }

      if (document) {
        db.prepare(
          "UPDATE staff_documents SET doc_type = ?, front_path = ?, back_path = ? WHERE staff_id = ?"
        ).run(safeDocType, frontPath, backPath, req.params.id);
      } else if (frontPath || backPath) {
        db.prepare(
          "INSERT INTO staff_documents (staff_id, doc_type, front_path, back_path) VALUES (?, ?, ?, ?)"
        ).run(req.params.id, safeDocType, frontPath, backPath);
      }
    }

    logActivity({
      userId: req.session.userId,
      action: "update",
      entityType: "staff",
      entityId: req.params.id,
      details: `name=${full_name}`
    });

    res.redirect(`/admin/staffs/${req.params.id}`);
  });
});

router.post("/staffs/:id/archive", (req, res) => {
  const staff = db.prepare("SELECT id, full_name, is_active FROM staff WHERE id = ?").get(req.params.id);
  if (!staff) return res.redirect("/admin/staffs");
  setStaffActiveStatus(req.params.id, false, req.session.userId);
  logActivity({
    userId: req.session.userId,
    action: "update",
    entityType: "staff",
    entityId: req.params.id,
    details: `status=archived, name=${staff.full_name || ""}`
  });
  res.redirect("/admin/staffs?archived=1&include_inactive=1");
});

router.post("/staffs/:id/activate", (req, res) => {
  const staff = db.prepare("SELECT id, full_name FROM staff WHERE id = ?").get(req.params.id);
  if (!staff) return res.redirect("/admin/staffs");
  setStaffActiveStatus(req.params.id, true, req.session.userId);
  logActivity({
    userId: req.session.userId,
    action: "update",
    entityType: "staff",
    entityId: req.params.id,
    details: `status=active, name=${staff.full_name || ""}`
  });
  res.redirect("/admin/staffs?activated=1&include_inactive=1");
});

router.post("/staffs/:id/delete", (req, res) => {
  res.redirect(307, `/admin/staffs/${req.params.id}/archive`);
});

router.post("/staffs/:id/payments", (req, res) => {
  const staff = db.prepare("SELECT * FROM staff WHERE id = ?").get(req.params.id);
  if (!staff) return res.redirect("/admin/staffs");
  const { payment_date, amount, payment_type, payment_source, note, print } = req.body;
  const amt = Number(amount || 0);
  if (!payment_date || Number.isNaN(amt) || amt <= 0) {
    return res.redirect(`/admin/staffs/${req.params.id}`);
  }
  const type = payment_type === "ADVANCE" ? "ADVANCE" : "SALARY";
  const source = normalizeSalaryPaymentSource(payment_source);
  if (source === "DAILY_COLLECTION") {
    const collectionBalance = getDailyCollectionBalance(db, payment_date);
    if (amt > roundMoney(collectionBalance) + 0.01) {
      return res.redirect(`/admin/staffs/${req.params.id}?error=dailyCollectionInsufficient`);
    }
  }
  if (type === "SALARY") {
    const paidTotal = getStaffPaidTotal(req.params.id);
    const due = computeSalaryDue("STAFF", staff, paidTotal, payment_date || dayjs().format("YYYY-MM-DD"));
    if (amt > roundMoney(due) + 0.01) {
      return res.redirect(`/admin/staffs/${req.params.id}?error=salaryOverpaymentBlocked`);
    }
  }

  const paymentResult = db.prepare(
    "INSERT INTO staff_salary_payments (staff_id, payment_date, amount, payment_type, payment_source, note, created_by) VALUES (?, ?, ?, ?, ?, ?, ?)"
  ).run(req.params.id, payment_date, amt, type, source, note || null, req.session.userId);
  const paymentId = Number(paymentResult.lastInsertRowid);
  const receiptNo = createReceiptNo(db, "STF", payment_date || dayjs().format("YYYY-MM-DD"));
  db.prepare("UPDATE staff_salary_payments SET receipt_no = ? WHERE id = ?").run(receiptNo, paymentId);

  logActivity({
    userId: req.session.userId,
    action: "payment",
    entityType: "staff_salary",
    entityId: paymentId,
    details: `receipt=${receiptNo}, type=${type}, source=${source}, amount=${amt}`
  });

  if (print) {
    return res.redirect(`/admin/staffs/payments/${paymentId}/print`);
  }
  res.redirect(`/admin/staffs/${req.params.id}`);
});

router.get("/staffs/payments/:id/edit", (req, res) => {
  const payment = db.prepare(
    `SELECT staff_salary_payments.*, staff.full_name
     FROM staff_salary_payments
     JOIN staff ON staff_salary_payments.staff_id = staff.id
     WHERE staff_salary_payments.id = ?`
  ).get(req.params.id);
  if (!payment) return res.redirect("/admin/staffs");
  const errorKey = String(req.query.error || "").trim();
  const error = errorKey ? req.t(errorKey) : null;
  res.render("admin/staff_payment_form", {
    title: req.t("editSalaryPaymentTitle"),
    payment,
    staff: { id: payment.staff_id, full_name: payment.full_name },
    error,
    basePath: "/admin/staffs"
  });
});

router.post("/staffs/payments/:id", (req, res) => {
  const payment = db.prepare("SELECT * FROM staff_salary_payments WHERE id = ?").get(req.params.id);
  if (!payment) return res.redirect("/admin/staffs");
  const staff = db.prepare("SELECT * FROM staff WHERE id = ?").get(payment.staff_id);
  if (!staff) return res.redirect("/admin/staffs");
  const { payment_date, amount, payment_type, payment_source, note, print } = req.body;
  const amt = Number(amount || 0);
  if (!payment_date || Number.isNaN(amt) || amt <= 0) {
    return res.redirect(`/admin/staffs/payments/${req.params.id}/edit`);
  }
  const type = payment_type === "ADVANCE" ? "ADVANCE" : "SALARY";
  const source = normalizeSalaryPaymentSource(payment_source);
  if (source === "DAILY_COLLECTION") {
    const collectionBalance = getDailyCollectionBalance(db, payment_date, { excludeStaffPaymentId: req.params.id });
    if (amt > roundMoney(collectionBalance) + 0.01) {
      return res.redirect(`/admin/staffs/payments/${req.params.id}/edit?error=dailyCollectionInsufficient`);
    }
  }
  if (type === "SALARY") {
    const paidTotal = getStaffPaidTotal(payment.staff_id, { excludePaymentId: req.params.id });
    const due = computeSalaryDue("STAFF", staff, paidTotal, payment_date || dayjs().format("YYYY-MM-DD"));
    if (amt > roundMoney(due) + 0.01) {
      return res.redirect(`/admin/staffs/payments/${req.params.id}/edit?error=salaryOverpaymentBlocked`);
    }
  }

  db.prepare(
    "UPDATE staff_salary_payments SET payment_date = ?, amount = ?, payment_type = ?, payment_source = ?, note = ? WHERE id = ?"
  ).run(payment_date, amt, type, source, note || null, req.params.id);

  logActivity({
    userId: req.session.userId,
    action: "update",
    entityType: "staff_salary",
    entityId: req.params.id,
    details: buildDiffDetails(
      payment,
      {
        payment_date,
        amount: amt,
        payment_type: type,
        payment_source: source,
        note: note || null
      },
      ["payment_date", "amount", "payment_type", "payment_source", "note"]
    )
  });

  if (print) {
    return res.redirect(`/admin/staffs/payments/${req.params.id}/print`);
  }
  res.redirect(`/admin/staffs/${payment.staff_id}`);
});

router.post("/staffs/payments/:id/delete", (req, res) => {
  const payment = db.prepare("SELECT * FROM staff_salary_payments WHERE id = ?").get(req.params.id);
  if (!payment) return res.redirect("/admin/staffs");
  const recycleId = createRecycleEntry({
    entityType: "staff_salary_payment",
    entityId: req.params.id,
    payload: { staff_salary_payment: payment },
    deletedBy: req.session.userId,
    note: `staff_id=${payment.staff_id}; receipt=${payment.receipt_no || ""}`
  });
  logActivity({
    userId: req.session.userId,
    action: "delete",
    entityType: "staff_salary",
    entityId: req.params.id,
    details: `recycle_id=${recycleId}`
  });
  db.prepare("DELETE FROM staff_salary_payments WHERE id = ?").run(req.params.id);
  res.redirect(`/admin/staffs/${payment.staff_id}`);
});

router.get("/staffs/payments/:id/print", (req, res) => {
  const payment = db.prepare(
    `SELECT staff_salary_payments.*, staff.full_name, staff.phone, staff.photo_path
     FROM staff_salary_payments
     JOIN staff ON staff_salary_payments.staff_id = staff.id
     WHERE staff_salary_payments.id = ?`
  ).get(req.params.id);
  if (!payment) return res.redirect("/admin/staffs");
  res.render("admin/staff_payment_print", { title: req.t("staffPaySlipTitle"), payment });
});

router.get("/jar-types", (req, res) => {
  const types = db.prepare("SELECT * FROM jar_types ORDER BY created_at DESC").all();
  res.render("admin/jar_types", { title: req.t("jarTypesTitle"), types });
});

// Jar cap type management was deprecated in favor of generic import item types.
router.all("/jar-cap-types*", (req, res) => {
  return res.redirect("/admin/import-item-types");
});

router.get("/jar-cap-types", (req, res) => {
  const types = db.prepare("SELECT * FROM jar_cap_types ORDER BY created_at DESC").all();
  res.render("admin/jar_cap_types", { title: req.t("jarCapTypesTitle"), types });
});

router.get("/jar-types/new", (req, res) => {
  res.render("admin/jar_type_form", { title: req.t("addJarTypeTitle"), type: null, error: null });
});

router.get("/jar-cap-types/new", (req, res) => {
  res.render("admin/jar_cap_type_form", { title: req.t("addJarCapTypeTitle"), type: null, error: null });
});

router.post("/jar-types", (req, res) => {
  const { name, default_qty } = req.body;
  const defaultQtyNum = Number(default_qty || 0);
  if (!name || Number.isNaN(defaultQtyNum) || defaultQtyNum < 0) {
    return res.render("admin/jar_type_form", { title: req.t("addJarTypeTitle"), type: null, error: req.t("jarTypeRequired") });
  }
  db.prepare("INSERT INTO jar_types (name, price, default_qty, active) VALUES (?, 0, ?, 1)")
    .run(name.trim(), defaultQtyNum);
  logActivity({
    userId: req.session.userId,
    action: "create",
    entityType: "jar_type",
    entityId: name.trim(),
    details: `default_qty=${defaultQtyNum}`
  });
  res.redirect("/admin/jar-types");
});

router.post("/jar-cap-types", (req, res) => {
  const { name, default_qty } = req.body;
  const defaultQtyNum = Number(default_qty || 0);
  if (!name || Number.isNaN(defaultQtyNum) || defaultQtyNum < 0) {
    return res.render("admin/jar_cap_type_form", { title: req.t("addJarCapTypeTitle"), type: null, error: req.t("jarCapTypeRequired") });
  }
  db.prepare("INSERT INTO jar_cap_types (name, default_qty, active) VALUES (?, ?, 1)")
    .run(name.trim(), defaultQtyNum);
  logActivity({
    userId: req.session.userId,
    action: "create",
    entityType: "jar_cap_type",
    entityId: name.trim(),
    details: `default_qty=${defaultQtyNum}`
  });
  res.redirect("/admin/jar-cap-types");
});

router.get("/jar-types/:id/edit", (req, res) => {
  const type = db.prepare("SELECT * FROM jar_types WHERE id = ?").get(req.params.id);
  if (!type) return res.redirect("/admin/jar-types");
  res.render("admin/jar_type_form", { title: req.t("editJarTypeTitle"), type, error: null });
});

router.get("/jar-cap-types/:id/edit", (req, res) => {
  const type = db.prepare("SELECT * FROM jar_cap_types WHERE id = ?").get(req.params.id);
  if (!type) return res.redirect("/admin/jar-cap-types");
  res.render("admin/jar_cap_type_form", { title: req.t("editJarCapTypeTitle"), type, error: null });
});

router.post("/jar-types/:id", (req, res) => {
  const type = db.prepare("SELECT * FROM jar_types WHERE id = ?").get(req.params.id);
  if (!type) return res.redirect("/admin/jar-types");
  const { name, active, default_qty } = req.body;
  const defaultQtyNum = Number(default_qty || 0);
  if (!name || Number.isNaN(defaultQtyNum) || defaultQtyNum < 0) {
    return res.render("admin/jar_type_form", { title: req.t("editJarTypeTitle"), type, error: req.t("jarTypeRequired") });
  }
  const activeFlag = active === "on" ? 1 : 0;
  db.prepare("UPDATE jar_types SET name = ?, price = 0, default_qty = ?, active = ?, updated_at = datetime('now') WHERE id = ?")
    .run(name.trim(), defaultQtyNum, activeFlag, req.params.id);
  logActivity({
    userId: req.session.userId,
    action: "update",
    entityType: "jar_type",
    entityId: req.params.id,
    details: `name=${name.trim()}, default_qty=${defaultQtyNum}, active=${activeFlag}`
  });
  res.redirect("/admin/jar-types");
});

router.post("/jar-cap-types/:id", (req, res) => {
  const type = db.prepare("SELECT * FROM jar_cap_types WHERE id = ?").get(req.params.id);
  if (!type) return res.redirect("/admin/jar-cap-types");
  const { name, active, default_qty } = req.body;
  const defaultQtyNum = Number(default_qty || 0);
  if (!name || Number.isNaN(defaultQtyNum) || defaultQtyNum < 0) {
    return res.render("admin/jar_cap_type_form", { title: req.t("editJarCapTypeTitle"), type, error: req.t("jarCapTypeRequired") });
  }
  const activeFlag = active === "on" ? 1 : 0;
  db.prepare("UPDATE jar_cap_types SET name = ?, default_qty = ?, active = ?, updated_at = datetime('now') WHERE id = ?")
    .run(name.trim(), defaultQtyNum, activeFlag, req.params.id);
  logActivity({
    userId: req.session.userId,
    action: "update",
    entityType: "jar_cap_type",
    entityId: req.params.id,
    details: `name=${name.trim()}, default_qty=${defaultQtyNum}, active=${activeFlag}`
  });
  res.redirect("/admin/jar-cap-types");
});

router.post("/jar-types/:id/delete", (req, res) => {
  logActivity({
    userId: req.session.userId,
    action: "delete",
    entityType: "jar_type",
    entityId: req.params.id
  });
  db.prepare("DELETE FROM jar_types WHERE id = ?").run(req.params.id);
  res.redirect("/admin/jar-types");
});

router.post("/jar-cap-types/:id/delete", (req, res) => {
  logActivity({
    userId: req.session.userId,
    action: "delete",
    entityType: "jar_cap_type",
    entityId: req.params.id
  });
  db.prepare("DELETE FROM jar_cap_types WHERE id = ?").run(req.params.id);
  res.redirect("/admin/jar-cap-types");
});

router.get("/import-item-types", (req, res) => {
  const types = db.prepare(
    "SELECT * FROM import_item_types ORDER BY is_predefined DESC, name ASC"
  ).all().map((row) => ({
    ...row,
    label: resolveImportItemLabel(row.code, row.name, req.t)
  }));
  const jarTypes = db.prepare("SELECT id, name, default_qty, active FROM jar_types ORDER BY name ASC").all();
  const success = req.query.saved ? req.t("itemTypeSaved") : req.query.deleted ? req.t("itemTypeDeleted") : null;
  const error = req.query.error ? req.t(req.query.error) : null;
  res.render("admin/import_item_types", {
    title: req.t("importItemTypesTitle"),
    types,
    jarTypes,
    success,
    error
  });
});

router.get("/import-item-types/new", (req, res) => {
  res.render("admin/import_item_type_form", {
    title: req.t("addImportItemTypeTitle"),
    type: null,
    error: null
  });
});

router.post("/import-item-types", (req, res) => {
  const { name, unit_label, uses_direction, is_active } = req.body;
  if (!name || !String(name).trim()) {
    return res.render("admin/import_item_type_form", {
      title: req.t("addImportItemTypeTitle"),
      type: null,
      error: req.t("importItemTypeRequired")
    });
  }
  const code = buildImportItemCode(name);
  const directionFlag = uses_direction === "on" ? 1 : 0;
  const activeFlag = is_active === "on" ? 1 : 0;
  const id = db.prepare(
    `INSERT INTO import_item_types (code, name, unit_label, uses_direction, is_predefined, is_active)
     VALUES (?, ?, ?, ?, 0, ?)`
  ).run(
    code,
    String(name).trim(),
    unit_label ? String(unit_label).trim() : null,
    directionFlag,
    activeFlag
  ).lastInsertRowid;
  logActivity({
    userId: req.session.userId,
    action: "create",
    entityType: "import_item_type",
    entityId: id,
    details: `code=${code}, name=${String(name).trim()}`
  });
  res.redirect("/admin/import-item-types?saved=1");
});

router.get("/import-item-types/:id/edit", (req, res) => {
  const type = db.prepare("SELECT * FROM import_item_types WHERE id = ?").get(req.params.id);
  if (!type) return res.redirect("/admin/import-item-types");
  res.render("admin/import_item_type_form", {
    title: req.t("editImportItemTypeTitle"),
    type,
    error: null
  });
});

router.post("/import-item-types/:id", (req, res) => {
  const type = db.prepare("SELECT * FROM import_item_types WHERE id = ?").get(req.params.id);
  if (!type) return res.redirect("/admin/import-item-types");
  const { name, unit_label, uses_direction, is_active } = req.body;
  if (!name || !String(name).trim()) {
    return res.render("admin/import_item_type_form", {
      title: req.t("editImportItemTypeTitle"),
      type: { ...type, name, unit_label, uses_direction: uses_direction === "on" ? 1 : 0, is_active: is_active === "on" ? 1 : 0 },
      error: req.t("importItemTypeRequired")
    });
  }
  const directionFlag = type.code === "JAR_CONTAINER" ? 0 : (uses_direction === "on" ? 1 : 0);
  const activeFlag = is_active === "on" ? 1 : 0;
  db.prepare(
    "UPDATE import_item_types SET name = ?, unit_label = ?, uses_direction = ?, is_active = ?, updated_at = datetime('now') WHERE id = ?"
  ).run(
    String(name).trim(),
    unit_label ? String(unit_label).trim() : null,
    directionFlag,
    activeFlag,
    req.params.id
  );
  logActivity({
    userId: req.session.userId,
    action: "update",
    entityType: "import_item_type",
    entityId: req.params.id,
    details: `name=${String(name).trim()}, active=${activeFlag}, uses_direction=${directionFlag}`
  });
  res.redirect("/admin/import-item-types?saved=1");
});

router.post("/import-item-types/:id/delete", (req, res) => {
  const type = db.prepare("SELECT * FROM import_item_types WHERE id = ?").get(req.params.id);
  if (!type) return res.redirect("/admin/import-item-types");
  const usageCount = db.prepare("SELECT COUNT(*) as count FROM import_entries WHERE item_type = ?").get(type.code).count;
  if (Number(type.is_predefined) === 1 || usageCount > 0) {
    db.prepare("UPDATE import_item_types SET is_active = 0, updated_at = datetime('now') WHERE id = ?").run(req.params.id);
  } else {
    db.prepare("DELETE FROM import_item_types WHERE id = ?").run(req.params.id);
  }
  logActivity({
    userId: req.session.userId,
    action: "delete",
    entityType: "import_item_type",
    entityId: req.params.id,
    details: `code=${type.code}`
  });
  res.redirect("/admin/import-item-types?deleted=1");
});

router.get("/vehicles/new", (req, res) => {
  res.render("admin/vehicle_form", {
    title: req.t("addVehicleTitle"),
    vehicle: null,
    vehicleCompliance: {},
    error: null
  });
});

router.post("/vehicles", upload.single("profile_pic"), (req, res) => {
  const {
    vehicle_number,
    owner_name,
    phone,
    is_company,
    insurance_details,
    insurance_expiry,
    tax_expiry,
    permit_details,
    permit_expiry,
    compliance_note
  } = req.body;
  if (!vehicle_number || !owner_name) {
    return res.render("admin/vehicle_form", {
      title: req.t("addVehicleTitle"),
      vehicle: null,
      vehicleCompliance: {
        insurance_details,
        insurance_expiry,
        tax_expiry,
        permit_details,
        permit_expiry,
        note: compliance_note
      },
      error: req.t("vehicleRequired")
    });
  }

  const profilePath = req.file ? `/uploads/${req.file.filename}` : null;
  const companyFlag = is_company === "on" ? 1 : 0;

  try {
    const result = db.prepare(
      "INSERT INTO vehicles (vehicle_number, owner_name, phone, is_company, profile_pic_path) VALUES (?, ?, ?, ?, ?)"
    ).run(vehicle_number.trim(), owner_name.trim(), phone ? phone.trim() : null, companyFlag, profilePath);
    saveVehicleCompliance(db, {
      vehicleId: result.lastInsertRowid,
      isCompany: companyFlag === 1,
      insuranceDetails: insurance_details,
      insuranceExpiry: insurance_expiry,
      taxExpiry: tax_expiry,
      permitDetails: permit_details,
      permitExpiry: permit_expiry,
      note: compliance_note,
      userId: req.session.userId
    });
    logActivity({
      userId: req.session.userId,
      action: "create",
      entityType: "vehicle",
      entityId: vehicle_number.trim(),
      details: `owner=${owner_name.trim()}, company=${companyFlag}`
    });
    res.redirect("/admin/vehicles");
  } catch (err) {
    res.render("admin/vehicle_form", {
      title: req.t("addVehicleTitle"),
      vehicle: null,
      vehicleCompliance: {
        insurance_details,
        insurance_expiry,
        tax_expiry,
        permit_details,
        permit_expiry,
        note: compliance_note
      },
      error: req.t("vehicleExists")
    });
  }
});

router.get("/vehicles/:id/edit", (req, res) => {
  const vehicle = db.prepare("SELECT * FROM vehicles WHERE id = ?").get(req.params.id);
  if (!vehicle) return res.redirect("/admin/vehicles");
  res.render("admin/vehicle_form", {
    title: req.t("editVehicleTitle"),
    vehicle,
    vehicleCompliance: getVehicleCompliance(db, vehicle.id),
    error: null
  });
});

router.post("/vehicles/:id", upload.single("profile_pic"), (req, res) => {
  const vehicle = db.prepare("SELECT * FROM vehicles WHERE id = ?").get(req.params.id);
  if (!vehicle) return res.redirect("/admin/vehicles");

  const {
    vehicle_number,
    owner_name,
    phone,
    is_company,
    insurance_details,
    insurance_expiry,
    tax_expiry,
    permit_details,
    permit_expiry,
    compliance_note
  } = req.body;
  if (!vehicle_number || !owner_name) {
    return res.render("admin/vehicle_form", {
      title: req.t("editVehicleTitle"),
      vehicle,
      vehicleCompliance: {
        insurance_details,
        insurance_expiry,
        tax_expiry,
        permit_details,
        permit_expiry,
        note: compliance_note
      },
      error: req.t("vehicleRequired")
    });
  }

  const profilePath = req.file ? `/uploads/${req.file.filename}` : vehicle.profile_pic_path;
  const companyFlag = is_company === "on" ? 1 : 0;

  try {
    db.prepare(
      "UPDATE vehicles SET vehicle_number = ?, owner_name = ?, phone = ?, is_company = ?, profile_pic_path = ?, updated_at = datetime('now') WHERE id = ?"
    ).run(vehicle_number.trim(), owner_name.trim(), phone ? phone.trim() : null, companyFlag, profilePath, req.params.id);
    saveVehicleCompliance(db, {
      vehicleId: req.params.id,
      isCompany: companyFlag === 1,
      insuranceDetails: insurance_details,
      insuranceExpiry: insurance_expiry,
      taxExpiry: tax_expiry,
      permitDetails: permit_details,
      permitExpiry: permit_expiry,
      note: compliance_note,
      userId: req.session.userId
    });
    logActivity({
      userId: req.session.userId,
      action: "update",
      entityType: "vehicle",
      entityId: req.params.id,
      details: `vehicle_number=${vehicle_number.trim()}, company=${companyFlag}`
    });
    res.redirect("/admin/vehicles");
  } catch (err) {
    res.render("admin/vehicle_form", {
      title: req.t("editVehicleTitle"),
      vehicle,
      vehicleCompliance: {
        insurance_details,
        insurance_expiry,
        tax_expiry,
        permit_details,
        permit_expiry,
        note: compliance_note
      },
      error: req.t("vehicleExists")
    });
  }
});

router.post("/vehicles/:id/archive", (req, res) => {
  const vehicle = db.prepare("SELECT id, vehicle_number, is_active FROM vehicles WHERE id = ?").get(req.params.id);
  if (!vehicle) return res.redirect("/admin/vehicles");
  setVehicleActiveStatus(req.params.id, false, req.session.userId);
  logActivity({
    userId: req.session.userId,
    action: "update",
    entityType: "vehicle",
    entityId: req.params.id,
    details: `status=archived, vehicle_number=${vehicle.vehicle_number || ""}`
  });
  res.redirect("/admin/vehicles?archived=1&include_inactive=1");
});

router.post("/vehicles/:id/activate", (req, res) => {
  const vehicle = db.prepare("SELECT id, vehicle_number FROM vehicles WHERE id = ?").get(req.params.id);
  if (!vehicle) return res.redirect("/admin/vehicles");
  setVehicleActiveStatus(req.params.id, true, req.session.userId);
  logActivity({
    userId: req.session.userId,
    action: "update",
    entityType: "vehicle",
    entityId: req.params.id,
    details: `status=active, vehicle_number=${vehicle.vehicle_number || ""}`
  });
  res.redirect("/admin/vehicles?activated=1&include_inactive=1");
});

router.post("/vehicles/:id/delete", (req, res) => {
  res.redirect(307, `/admin/vehicles/${req.params.id}/archive`);
});

router.get("/sales", (req, res) => {
  const date = req.query.date || dayjs().format("YYYY-MM-DD");
  const mode = "exports";
  const sales = db.prepare(
    `SELECT exports.export_date as sale_date, vehicles.vehicle_number, vehicles.owner_name,
            COALESCE(SUM(exports.total_amount), 0) as total_sales
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     WHERE exports.export_date = ?
     GROUP BY exports.vehicle_id
     ORDER BY total_sales DESC`
  ).all(date);
  const total = sales.reduce((sum, sale) => sum + Number(sale.total_sales || 0), 0);
  const counts = db.prepare(
    "SELECT COALESCE(SUM(jar_count), 0) as total_jars, COALESCE(SUM(bottle_case_count), 0) as total_bottles, COALESCE(SUM(total_amount), 0) as total_amount FROM exports WHERE export_date = ?"
  ).get(date);
  res.render("admin/sales", {
    title: req.t("dailySalesTitle"),
    sales,
    date,
    total,
    mode,
    counts
  });
});

router.get("/sales/new", (req, res) => {
  res.redirect("/admin/sales");
});

router.post("/sales", (req, res) => {
  res.redirect("/admin/sales");
});

router.get("/sales/:id/edit", (req, res) => {
  res.redirect("/admin/sales");
});

router.post("/sales/:id", (req, res) => {
  res.redirect("/admin/sales");
});

router.post("/sales/:id/delete", (req, res) => {
  res.redirect("/admin/sales");
});

router.get("/sales/export", (req, res) => {
  const date = req.query.date || dayjs().format("YYYY-MM-DD");
  const mode = "exports";
  const rows = db.prepare(
    `SELECT exports.export_date as sale_date, vehicles.vehicle_number, vehicles.owner_name,
            COALESCE(SUM(exports.total_amount), 0) as total_sales
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     WHERE export_date = ?
     GROUP BY exports.vehicle_id
     ORDER BY vehicles.vehicle_number ASC`
  ).all(date);

  const header = "Date,Vehicle Number,Owner Name,Total Sales";
  const lines = rows.map((row) => {
    const safe = [row.sale_date, row.vehicle_number, row.owner_name, row.total_sales].map((val) => {
      const str = String(val ?? "").replace(/\"/g, "\"\"");
      return `"${str}"`;
    });
    return safe.join(",");
  });

  const csv = [header, ...lines].join("\n");
  res.setHeader("Content-Type", "text/csv");
  res.setHeader("Content-Disposition", `attachment; filename="daily_sales_${mode}_${date}.csv"`);
  res.send(csv);
});

router.get("/reports/sales", (req, res) => {
  const from = req.query.from || dayjs().subtract(7, "day").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const rows = db.prepare(
    `SELECT exports.export_date as sale_date, vehicles.vehicle_number, vehicles.owner_name,
            COALESCE(SUM(exports.total_amount), 0) as total_sales
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     WHERE export_date BETWEEN ? AND ?
     GROUP BY exports.export_date, exports.vehicle_id
     ORDER BY export_date ASC`
  ).all(from, to);

  const total = rows.reduce((sum, row) => sum + Number(row.total_sales || 0), 0);

  res.render("admin/report_sales", {
    title: req.t("salesReportTitle"),
    from,
    to,
    rows,
    total
  });
});

router.get("/reports/summary", (req, res) => {
  const from = req.query.from || dayjs().subtract(30, "day").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const group = ["day", "month", "year"].includes(req.query.group) ? req.query.group : "day";

  let labelExpr = "export_date";
  if (group === "month") labelExpr = "strftime('%Y-%m', export_date)";
  if (group === "year") labelExpr = "strftime('%Y', export_date)";

  const rows = db.prepare(
    `SELECT ${labelExpr} as label, COALESCE(SUM(total_amount), 0) as total
     FROM exports
     WHERE export_date BETWEEN ? AND ?
     GROUP BY label
     ORDER BY label ASC`
  ).all(from, to);

  const total = rows.reduce((sum, row) => sum + Number(row.total || 0), 0);

  res.render("admin/report_summary", {
    title: req.t("salesSummaryTitle"),
    from,
    to,
    group,
    rows,
    total
  });
});

router.get("/reports/credit-aging", (req, res) => {
  const asOf = req.query.asOf || dayjs().format("YYYY-MM-DD");
  const rawRows = db.prepare(
    `SELECT credits.*, vehicles.vehicle_number, vehicles.owner_name
     FROM credits
     JOIN vehicles ON credits.vehicle_id = vehicles.id
     WHERE credit_date <= ?
       AND (credits.amount - credits.paid_amount) > 0
     ORDER BY credit_date ASC`
  ).all(asOf);

  const rows = rawRows.map((row) => {
    const days = dayjs(asOf).diff(dayjs(row.credit_date), "day");
    const remaining = Math.max(0, Number(row.amount || 0) - Number(row.paid_amount || 0));
    return { ...row, days, remaining };
  });

  const buckets = [
    { key: "0_7", label: req.t("aging0to7"), min: 0, max: 7 },
    { key: "8_30", label: req.t("aging8to30"), min: 8, max: 30 },
    { key: "31_60", label: req.t("aging31to60"), min: 31, max: 60 },
    { key: "61_90", label: req.t("aging61to90"), min: 61, max: 90 },
    { key: "90_plus", label: req.t("aging90plus"), min: 91, max: Infinity }
  ];

  const bucketRows = buckets.map((bucket) => {
    const items = rows.filter((row) => row.days >= bucket.min && row.days <= bucket.max);
    const totalRemaining = items.reduce((sum, row) => sum + row.remaining, 0);
    return { ...bucket, count: items.length, totalRemaining };
  });

  const totalOutstanding = rows.reduce((sum, row) => sum + row.remaining, 0);

  res.render("admin/report_credit_aging", {
    title: req.t("creditAgingTitle"),
    asOf,
    rows,
    bucketRows,
    totalOutstanding
  });
});

router.get("/reports/vehicle-balances", (req, res) => {
  const from = req.query.from || dayjs().startOf("month").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");

  const rows = db.prepare(
    `SELECT vehicles.id, vehicles.vehicle_number, vehicles.owner_name,
            COALESCE(exports.total_amount, 0) as export_total,
            COALESCE(exports.paid_amount, 0) as export_paid,
            COALESCE(exports.credit_amount, 0) as export_credit,
            COALESCE(credits.total_amount, 0) as credit_total,
            COALESCE(credits.paid_amount, 0) as credit_paid,
            COALESCE(credits.remaining_amount, 0) as credit_remaining
     FROM vehicles
     LEFT JOIN (
       SELECT vehicle_id,
              COALESCE(SUM(total_amount), 0) as total_amount,
              COALESCE(SUM(paid_amount), 0) as paid_amount,
              COALESCE(SUM(credit_amount), 0) as credit_amount
       FROM exports
       WHERE export_date BETWEEN ? AND ?
       GROUP BY vehicle_id
     ) exports ON exports.vehicle_id = vehicles.id
     LEFT JOIN (
       SELECT vehicle_id,
              COALESCE(SUM(amount), 0) as total_amount,
              COALESCE(SUM(paid_amount), 0) as paid_amount,
              COALESCE(SUM(CASE WHEN amount - paid_amount < 0 THEN 0 ELSE amount - paid_amount END), 0) as remaining_amount
       FROM credits
       WHERE credit_date BETWEEN ? AND ?
       GROUP BY vehicle_id
     ) credits ON credits.vehicle_id = vehicles.id
     ORDER BY vehicles.vehicle_number ASC`
  ).all(from, to, from, to);

  const totals = rows.reduce(
    (acc, row) => {
      acc.export_total += Number(row.export_total || 0);
      acc.export_paid += Number(row.export_paid || 0);
      acc.export_credit += Number(row.export_credit || 0);
      acc.credit_total += Number(row.credit_total || 0);
      acc.credit_paid += Number(row.credit_paid || 0);
      acc.credit_remaining += Number(row.credit_remaining || 0);
      return acc;
    },
    {
      export_total: 0,
      export_paid: 0,
      export_credit: 0,
      credit_total: 0,
      credit_paid: 0,
      credit_remaining: 0
    }
  );

  res.render("admin/report_vehicle_balances", {
    title: req.t("vehicleBalancesTitle"),
    from,
    to,
    rows,
    totals
  });
});

router.get("/reports/customer-invoice", (req, res) => {
  const from = req.query.from || dayjs().startOf("month").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const customerId = parseOptionalId(req.query.customer_id);
  const customerName = normalizeCustomerText(req.query.customer);
  const customers = getCustomerDirectoryForSelect({ includeInactive: true });
  const selectedCustomer = customerId ? getCustomerById(customerId) : findCustomerByName(customerName);
  const customer = selectedCustomer ? selectedCustomer.name : customerName;

  let rows = [];
  let totals = { total_amount: 0, total_paid: 0, total_remaining: 0 };
  if (selectedCustomer) {
    rows = db.prepare(
      `SELECT credits.*, vehicles.vehicle_number, vehicles.owner_name
       FROM credits
       JOIN vehicles ON credits.vehicle_id = vehicles.id
       WHERE (credits.customer_id = ?
          OR (credits.customer_id IS NULL AND lower(trim(credits.customer_name)) = lower(trim(?))))
         AND credits.credit_date BETWEEN ? AND ?
       ORDER BY credits.credit_date ASC`
    ).all(selectedCustomer.id, selectedCustomer.name, from, to);
    totals = rows.reduce(
      (acc, row) => {
        const amount = Number(row.amount || 0);
        const paidAmount = Number(row.paid_amount || 0);
        const remaining = Math.max(0, amount - paidAmount);
        acc.total_amount += amount;
        acc.total_paid += paidAmount;
        acc.total_remaining += remaining;
        return acc;
      },
      { total_amount: 0, total_paid: 0, total_remaining: 0 }
    );
  }

  res.render("admin/report_customer_invoice", {
    title: req.t("customerInvoiceTitle"),
    from,
    to,
    customerId: selectedCustomer ? selectedCustomer.id : 0,
    customer,
    customers,
    rows,
    totals
  });
});

router.get("/reports/customer-invoice/export", (req, res) => {
  const from = req.query.from || dayjs().startOf("month").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const customerId = parseOptionalId(req.query.customer_id);
  const customerName = normalizeCustomerText(req.query.customer);
  const selectedCustomer = customerId ? getCustomerById(customerId) : findCustomerByName(customerName);
  if (!selectedCustomer) {
    return res.redirect(`/admin/reports/customer-invoice?from=${from}&to=${to}`);
  }
  const customer = selectedCustomer.name;
  const rows = db.prepare(
    `SELECT credits.credit_date, vehicles.vehicle_number, vehicles.owner_name,
            credits.amount, credits.paid_amount
     FROM credits
     JOIN vehicles ON credits.vehicle_id = vehicles.id
     WHERE (credits.customer_id = ?
        OR (credits.customer_id IS NULL AND lower(trim(credits.customer_name)) = lower(trim(?))))
       AND credits.credit_date BETWEEN ? AND ?
     ORDER BY credits.credit_date ASC`
  ).all(selectedCustomer.id, selectedCustomer.name, from, to);
  const header = ["Date (AD)", "Date (BS)", "Vehicle Number", "Owner Name", "Amount", "Paid Amount", "Remaining Amount"];
  const lines = rows.map((row) => {
    const amount = Number(row.amount || 0);
    const paid = Number(row.paid_amount || 0);
    const remaining = Math.max(0, amount - paid);
    return [
      row.credit_date,
      adToBs(row.credit_date) || "",
      row.vehicle_number || "",
      row.owner_name || "",
      amount,
      paid,
      remaining
    ].map((value) => `"${String(value ?? "").replace(/"/g, '""')}"`).join(",");
  });
  res.setHeader("Content-Type", "text/csv");
  res.setHeader("Content-Disposition", `attachment; filename="customer_invoice_${customer.replace(/[^a-zA-Z0-9_-]/g, "_")}_${from}_to_${to}.csv"`);
  return res.send([header.join(","), ...lines].join("\n"));
});

router.post("/reports/customer-invoice/print", (req, res) => {
  const from = req.body.from || dayjs().startOf("month").format("YYYY-MM-DD");
  const to = req.body.to || dayjs().format("YYYY-MM-DD");
  const customerId = parseOptionalId(req.body.customer_id);
  const customerName = normalizeCustomerText(req.body.customer);
  const selectedCustomer = customerId ? getCustomerById(customerId) : findCustomerByName(customerName);
  if (!selectedCustomer) {
    return res.redirect(`/admin/reports/customer-invoice?from=${from}&to=${to}`);
  }
  const invoiceNo = createInvoiceNo(db, to);
  logActivity({
    userId: req.session.userId,
    action: "create",
    entityType: "invoice",
    entityId: selectedCustomer.name,
    details: `invoice=${invoiceNo}, from=${from}, to=${to}`
  });
  const query = new URLSearchParams({
    from,
    to,
    customer_id: String(selectedCustomer.id),
    customer: selectedCustomer.name,
    invoice_no: invoiceNo,
    autoprint: "1"
  });
  return res.redirect(`/admin/reports/customer-invoice/print?${query.toString()}`);
});

router.get("/reports/customer-invoice/print", (req, res) => {
  const from = req.query.from || dayjs().startOf("month").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const customerId = parseOptionalId(req.query.customer_id);
  const customerName = normalizeCustomerText(req.query.customer);
  const selectedCustomer = customerId ? getCustomerById(customerId) : findCustomerByName(customerName);
  if (!selectedCustomer) {
    return res.redirect("/admin/reports/customer-invoice");
  }
  const customer = selectedCustomer.name;

  const rows = db.prepare(
    `SELECT credits.*, vehicles.vehicle_number, vehicles.owner_name, vehicles.phone
     FROM credits
     JOIN vehicles ON credits.vehicle_id = vehicles.id
     WHERE (credits.customer_id = ?
        OR (credits.customer_id IS NULL AND lower(trim(credits.customer_name)) = lower(trim(?))))
       AND credits.credit_date BETWEEN ? AND ?
     ORDER BY credits.credit_date ASC`
  ).all(selectedCustomer.id, selectedCustomer.name, from, to);

  const totals = rows.reduce(
    (acc, row) => {
      const amount = Number(row.amount || 0);
      const paidAmount = Number(row.paid_amount || 0);
      const remaining = Math.max(0, amount - paidAmount);
      acc.total_amount += amount;
      acc.total_paid += paidAmount;
      acc.total_remaining += remaining;
      return acc;
    },
    { total_amount: 0, total_paid: 0, total_remaining: 0 }
  );

  const invoiceNo = String(req.query.invoice_no || "").trim();
  const autoPrint = Boolean(invoiceNo) && req.query.autoprint === "1";

  res.render("admin/report_customer_invoice_print", {
    title: req.t("customerInvoiceTitle"),
    from,
    to,
    customerId: selectedCustomer.id,
    customer,
    invoiceNo,
    autoPrint,
    rows,
    totals
  });
});

router.get("/reports/payroll-summary", (req, res) => {
  const monthToken = parseMonthToken(req.query.month);
  const summary = getPayrollSummaryPayload(monthToken);
  const monthLabel = dayjs(`${summary.monthToken}-01`).format("MMMM YYYY");
  res.render("admin/report_payroll_summary", {
    title: req.t("payrollSummaryTitle"),
    monthToken: summary.monthToken,
    monthLabel,
    ...summary
  });
});

router.get("/reports/payroll-summary/print", (req, res) => {
  const monthToken = parseMonthToken(req.query.month);
  const summary = getPayrollSummaryPayload(monthToken);
  const monthLabel = dayjs(`${summary.monthToken}-01`).format("MMMM YYYY");
  const autoPrint = req.query.autoprint === "1";
  res.render("admin/report_payroll_summary_print", {
    title: req.t("payrollSummaryTitle"),
    monthToken: summary.monthToken,
    monthLabel,
    autoPrint,
    ...summary
  });
});

router.get("/reports/payroll-summary/export", (req, res) => {
  const monthToken = parseMonthToken(req.query.month);
  const summary = getPayrollSummaryPayload(monthToken);
  const escapeCsv = (value) => `\"${String(value ?? "").replace(/\"/g, "\"\"")}\"`;
  const lines = [
    "TYPE,Name,Role,Monthly Salary,Opening Due,Opening Advance,Month Accrued,Salary Paid,Advance Paid,Month Paid,Closing Due,Closing Advance,Present Days,Absent Days"
  ];
  summary.staffRows.forEach((row) => {
    lines.push([
      "STAFF",
      row.full_name,
      resolveStaffRoleLabel(row.staff_role, null, req.t),
      row.monthly_salary,
      row.opening_due,
      row.opening_advance,
      row.month_accrued,
      row.salary_paid_month,
      row.advance_paid_month,
      row.month_paid,
      row.closing_due,
      row.closing_advance,
      row.present_days,
      row.absent_days
    ].map(escapeCsv).join(","));
  });
  summary.workerRows.forEach((row) => {
    lines.push([
      "WORKER",
      row.full_name,
      req.t("workersTitle"),
      row.monthly_salary,
      row.opening_due,
      row.opening_advance,
      row.month_accrued,
      row.salary_paid_month,
      row.advance_paid_month,
      row.month_paid,
      row.closing_due,
      row.closing_advance,
      row.present_days,
      row.absent_days
    ].map(escapeCsv).join(","));
  });
  res.setHeader("Content-Type", "text/csv");
  res.setHeader("Content-Disposition", `attachment; filename=\"payroll_summary_${summary.monthToken}.csv\"`);
  res.send(lines.join("\n"));
});

router.get("/reports/returns", (req, res) => {
  const date = req.query.date || dayjs().format("YYYY-MM-DD");
  const rows = db.prepare(
    `SELECT vehicles.vehicle_number, vehicles.owner_name,
            COALESCE(SUM(exports.return_jar_count), 0) as return_jars,
            COALESCE(SUM(exports.leakage_jar_count), 0) as leakage_jars,
            COALESCE(SUM(exports.return_bottle_case_count), 0) as return_bottles
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     WHERE exports.export_date = ?
     GROUP BY exports.vehicle_id
     ORDER BY vehicles.vehicle_number ASC`
  ).all(date);

  const totals = rows.reduce(
    (acc, row) => {
      acc.return_jars += Number(row.return_jars || 0);
      acc.leakage_jars += Number(row.leakage_jars || 0);
      acc.return_bottles += Number(row.return_bottles || 0);
      return acc;
    },
    { return_jars: 0, leakage_jars: 0, return_bottles: 0 }
  );

  res.render("admin/report_returns", {
    title: req.t("returnsReportTitle"),
    date,
    rows,
    totals
  });
});

router.get("/reports/returns/print", (req, res) => {
  const date = req.query.date || dayjs().format("YYYY-MM-DD");
  const rows = db.prepare(
    `SELECT vehicles.vehicle_number, vehicles.owner_name,
            COALESCE(SUM(exports.return_jar_count), 0) as return_jars,
            COALESCE(SUM(exports.leakage_jar_count), 0) as leakage_jars,
            COALESCE(SUM(exports.return_bottle_case_count), 0) as return_bottles
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     WHERE exports.export_date = ?
     GROUP BY exports.vehicle_id
     ORDER BY vehicles.vehicle_number ASC`
  ).all(date);
  const totals = rows.reduce(
    (acc, row) => {
      acc.return_jars += Number(row.return_jars || 0);
      acc.leakage_jars += Number(row.leakage_jars || 0);
      acc.return_bottles += Number(row.return_bottles || 0);
      return acc;
    },
    { return_jars: 0, leakage_jars: 0, return_bottles: 0 }
  );
  res.render("admin/report_returns_print", {
    title: req.t("returnsReportTitle"),
    date,
    rows,
    totals
  });
});

router.get("/reports/returns/export", (req, res) => {
  const date = req.query.date || dayjs().format("YYYY-MM-DD");
  const rows = db.prepare(
    `SELECT vehicles.vehicle_number, vehicles.owner_name,
            COALESCE(SUM(exports.return_jar_count), 0) as return_jars,
            COALESCE(SUM(exports.leakage_jar_count), 0) as leakage_jars,
            COALESCE(SUM(exports.return_bottle_case_count), 0) as return_bottles
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     WHERE exports.export_date = ?
     GROUP BY exports.vehicle_id
     ORDER BY vehicles.vehicle_number ASC`
  ).all(date);

  const header = "Date,Vehicle Number,Owner Name,Return Jars,Leakage Jars,Return Bottle Cases";
  const lines = rows.map((row) => {
    const safe = [date, row.vehicle_number, row.owner_name, row.return_jars, row.leakage_jars, row.return_bottles]
      .map((val) => `"${String(val ?? "").replace(/\"/g, "\"\"")}"`);
    return safe.join(",");
  });
  res.setHeader("Content-Type", "text/csv");
  res.setHeader("Content-Disposition", `attachment; filename="returns_${date}.csv"`);
  res.send([header, ...lines].join("\n"));
});

router.get("/reports/export-summary", (req, res) => {
  const from = req.query.from || dayjs().subtract(7, "day").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const vehicleId = req.query.vehicle_id || "all";
  const vehicles = db.prepare("SELECT id, vehicle_number, owner_name FROM vehicles ORDER BY vehicle_number ASC").all();

  const params = [from, to];
  let vehicleClause = "";
  if (vehicleId !== "all") {
    vehicleClause = "AND exports.vehicle_id = ?";
    params.push(vehicleId);
  }

  const rows = db.prepare(
    `SELECT exports.export_date, vehicles.vehicle_number, vehicles.owner_name,
            COALESCE(SUM(exports.jar_count), 0) as jars,
            COALESCE(SUM(exports.bottle_case_count), 0) as bottles,
            COALESCE(SUM(exports.return_jar_count), 0) as return_jars,
            COALESCE(SUM(exports.leakage_jar_count), 0) as leakage_jars,
            COALESCE(SUM(exports.return_bottle_case_count), 0) as return_bottles,
            COALESCE(SUM(exports.total_amount), 0) as total_amount,
            COALESCE(SUM(exports.paid_amount), 0) as paid_amount,
            COALESCE(SUM(exports.credit_amount), 0) as credit_amount
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     WHERE exports.export_date BETWEEN ? AND ?
     ${vehicleClause}
     GROUP BY exports.export_date, exports.vehicle_id
     ORDER BY exports.export_date ASC, vehicles.vehicle_number ASC`
  ).all(...params);

  const totals = rows.reduce(
    (acc, row) => {
      acc.jars += Number(row.jars || 0);
      acc.bottles += Number(row.bottles || 0);
      acc.return_jars += Number(row.return_jars || 0);
      acc.leakage_jars += Number(row.leakage_jars || 0);
      acc.return_bottles += Number(row.return_bottles || 0);
      acc.total_amount += Number(row.total_amount || 0);
      acc.paid_amount += Number(row.paid_amount || 0);
      acc.credit_amount += Number(row.credit_amount || 0);
      return acc;
    },
    { jars: 0, bottles: 0, return_jars: 0, leakage_jars: 0, return_bottles: 0, total_amount: 0, paid_amount: 0, credit_amount: 0 }
  );

  res.render("admin/report_export_summary", {
    title: req.t("exportSummaryTitle"),
    from,
    to,
    vehicleId,
    vehicles,
    rows,
    totals
  });
});

router.get("/reports/export-summary/print", (req, res) => {
  const from = req.query.from || dayjs().subtract(7, "day").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const vehicleId = req.query.vehicle_id || "all";
  const params = [from, to];
  let vehicleClause = "";
  if (vehicleId !== "all") {
    vehicleClause = "AND exports.vehicle_id = ?";
    params.push(vehicleId);
  }

  const rows = db.prepare(
    `SELECT exports.export_date, vehicles.vehicle_number, vehicles.owner_name,
            COALESCE(SUM(exports.jar_count), 0) as jars,
            COALESCE(SUM(exports.bottle_case_count), 0) as bottles,
            COALESCE(SUM(exports.return_jar_count), 0) as return_jars,
            COALESCE(SUM(exports.leakage_jar_count), 0) as leakage_jars,
            COALESCE(SUM(exports.return_bottle_case_count), 0) as return_bottles,
            COALESCE(SUM(exports.total_amount), 0) as total_amount,
            COALESCE(SUM(exports.paid_amount), 0) as paid_amount,
            COALESCE(SUM(exports.credit_amount), 0) as credit_amount
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     WHERE exports.export_date BETWEEN ? AND ?
     ${vehicleClause}
     GROUP BY exports.export_date, exports.vehicle_id
     ORDER BY exports.export_date ASC, vehicles.vehicle_number ASC`
  ).all(...params);

  const totals = rows.reduce(
    (acc, row) => {
      acc.jars += Number(row.jars || 0);
      acc.bottles += Number(row.bottles || 0);
      acc.return_jars += Number(row.return_jars || 0);
      acc.leakage_jars += Number(row.leakage_jars || 0);
      acc.return_bottles += Number(row.return_bottles || 0);
      acc.total_amount += Number(row.total_amount || 0);
      acc.paid_amount += Number(row.paid_amount || 0);
      acc.credit_amount += Number(row.credit_amount || 0);
      return acc;
    },
    { jars: 0, bottles: 0, return_jars: 0, leakage_jars: 0, return_bottles: 0, total_amount: 0, paid_amount: 0, credit_amount: 0 }
  );

  res.render("admin/report_export_summary_print", {
    title: req.t("exportSummaryTitle"),
    from,
    to,
    vehicleId,
    rows,
    totals
  });
});

router.get("/reports/export-summary/export", (req, res) => {
  const from = req.query.from || dayjs().subtract(7, "day").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const vehicleId = req.query.vehicle_id || "all";
  const params = [from, to];
  let vehicleClause = "";
  if (vehicleId !== "all") {
    vehicleClause = "AND exports.vehicle_id = ?";
    params.push(vehicleId);
  }

  const rows = db.prepare(
    `SELECT exports.export_date, vehicles.vehicle_number, vehicles.owner_name,
            COALESCE(SUM(exports.jar_count), 0) as jars,
            COALESCE(SUM(exports.bottle_case_count), 0) as bottles,
            COALESCE(SUM(exports.return_jar_count), 0) as return_jars,
            COALESCE(SUM(exports.leakage_jar_count), 0) as leakage_jars,
            COALESCE(SUM(exports.return_bottle_case_count), 0) as return_bottles,
            COALESCE(SUM(exports.total_amount), 0) as total_amount,
            COALESCE(SUM(exports.paid_amount), 0) as paid_amount,
            COALESCE(SUM(exports.credit_amount), 0) as credit_amount
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     WHERE exports.export_date BETWEEN ? AND ?
     ${vehicleClause}
     GROUP BY exports.export_date, exports.vehicle_id
     ORDER BY exports.export_date ASC, vehicles.vehicle_number ASC`
  ).all(...params);

  const header = "Date,Vehicle,Owner,Jars,Bottles,Return Jars,Leakage Jars,Return Bottle Cases,Total,Paid,Credit";
  const lines = rows.map((row) => {
    const safe = [
      row.export_date,
      row.vehicle_number,
      row.owner_name,
      row.jars,
      row.bottles,
      row.return_jars,
      row.leakage_jars,
      row.return_bottles,
      row.total_amount,
      row.paid_amount,
      row.credit_amount
    ].map((val) => `"${String(val ?? "").replace(/\"/g, "\"\"")}"`);
    return safe.join(",");
  });
  res.setHeader("Content-Type", "text/csv");
  res.setHeader("Content-Disposition", `attachment; filename="export_summary_${from}_to_${to}.csv"`);
  res.send([header, ...lines].join("\n"));
});

router.get("/reports/inventory", (req, res) => {
  const from = req.query.from || dayjs().startOf("month").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");

  const totals = db.prepare(
    `SELECT
        COALESCE(SUM(jar_count), 0) as jars,
        COALESCE(SUM(return_jar_count), 0) as return_jars,
        COALESCE(SUM(leakage_jar_count), 0) as leakage_jars,
        COALESCE(SUM(bottle_case_count), 0) as bottles,
        COALESCE(SUM(return_bottle_case_count), 0) as return_bottles
     FROM exports
     WHERE export_date BETWEEN ? AND ?`
  ).get(from, to);

  const rows = db.prepare(
    `SELECT vehicles.vehicle_number, vehicles.owner_name,
            COALESCE(SUM(exports.jar_count), 0) as jars,
            COALESCE(SUM(exports.return_jar_count), 0) as return_jars,
            COALESCE(SUM(exports.leakage_jar_count), 0) as leakage_jars,
            COALESCE(SUM(exports.bottle_case_count), 0) as bottles,
            COALESCE(SUM(exports.return_bottle_case_count), 0) as return_bottles
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     WHERE export_date BETWEEN ? AND ?
     GROUP BY exports.vehicle_id
     ORDER BY vehicles.vehicle_number ASC`
  ).all(from, to);

  res.render("admin/report_inventory", {
    title: req.t("inventorySummaryTitle"),
    from,
    to,
    totals,
    rows
  });
});

router.get("/reports/inventory/print", (req, res) => {
  const from = req.query.from || dayjs().startOf("month").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");

  const totals = db.prepare(
    `SELECT
        COALESCE(SUM(jar_count), 0) as jars,
        COALESCE(SUM(return_jar_count), 0) as return_jars,
        COALESCE(SUM(leakage_jar_count), 0) as leakage_jars,
        COALESCE(SUM(bottle_case_count), 0) as bottles,
        COALESCE(SUM(return_bottle_case_count), 0) as return_bottles
     FROM exports
     WHERE export_date BETWEEN ? AND ?`
  ).get(from, to);

  const rows = db.prepare(
    `SELECT vehicles.vehicle_number, vehicles.owner_name,
            COALESCE(SUM(exports.jar_count), 0) as jars,
            COALESCE(SUM(exports.return_jar_count), 0) as return_jars,
            COALESCE(SUM(exports.leakage_jar_count), 0) as leakage_jars,
            COALESCE(SUM(exports.bottle_case_count), 0) as bottles,
            COALESCE(SUM(exports.return_bottle_case_count), 0) as return_bottles
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     WHERE export_date BETWEEN ? AND ?
     GROUP BY exports.vehicle_id
     ORDER BY vehicles.vehicle_number ASC`
  ).all(from, to);

  res.render("admin/report_inventory_print", {
    title: req.t("inventorySummaryTitle"),
    from,
    to,
    totals,
    rows
  });
});

router.get("/reports/daily-close", (req, res) => {
  const date = req.query.date || dayjs().format("YYYY-MM-DD");
  const summary = db.prepare(
    `SELECT
        COALESCE(SUM(jar_count), 0) as jars,
        COALESCE(SUM(bottle_case_count), 0) as bottles,
        COALESCE(SUM(return_jar_count), 0) as return_jars,
        COALESCE(SUM(leakage_jar_count), 0) as leakage_jars,
        COALESCE(SUM(return_bottle_case_count), 0) as return_bottles,
        COALESCE(SUM(total_amount), 0) as total_amount,
        COALESCE(SUM(paid_amount), 0) as paid_amount,
        ${buildExportRemainingCreditAggregateSql("exports")} as remaining_credit
     FROM exports
     WHERE export_date = ?`
  ).get(date);

  const rows = db.prepare(
    `SELECT exports.*, vehicles.vehicle_number, vehicles.owner_name, users.full_name as recorded_by
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     LEFT JOIN users ON exports.created_by = users.id
     WHERE exports.export_date = ?
     ORDER BY exports.created_at DESC`
  ).all(date);

  res.render("admin/report_daily_close", {
    title: req.t("dailyCloseTitle"),
    date,
    summary,
    rows
  });
});

router.get("/reports/daily-close/print", (req, res) => {
  const date = req.query.date || dayjs().format("YYYY-MM-DD");
  const summary = db.prepare(
    `SELECT
        COALESCE(SUM(jar_count), 0) as jars,
        COALESCE(SUM(bottle_case_count), 0) as bottles,
        COALESCE(SUM(return_jar_count), 0) as return_jars,
        COALESCE(SUM(leakage_jar_count), 0) as leakage_jars,
        COALESCE(SUM(return_bottle_case_count), 0) as return_bottles,
        COALESCE(SUM(total_amount), 0) as total_amount,
        COALESCE(SUM(paid_amount), 0) as paid_amount,
        ${buildExportRemainingCreditAggregateSql("exports")} as remaining_credit
     FROM exports
     WHERE export_date = ?`
  ).get(date);
  const rows = db.prepare(
    `SELECT exports.*, vehicles.vehicle_number, vehicles.owner_name
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     WHERE exports.export_date = ?
     ORDER BY exports.created_at DESC`
  ).all(date);
  res.render("admin/report_daily_close_print", {
    title: req.t("dailyCloseTitle"),
    date,
    summary,
    rows
  });
});

router.get("/reports/daily-close/export", (req, res) => {
  const date = req.query.date || dayjs().format("YYYY-MM-DD");
  const rows = db.prepare(
    `SELECT exports.export_date, vehicles.vehicle_number, vehicles.owner_name,
            exports.jar_count, exports.bottle_case_count, exports.return_jar_count,
            exports.return_bottle_case_count, exports.leakage_jar_count,
            exports.total_amount, exports.paid_amount, exports.credit_amount
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     WHERE exports.export_date = ?
     ORDER BY exports.created_at DESC`
  ).all(date);

  const header = "Date,Vehicle,Owner,Jars,Bottles,Return Jars,Return Bottle Cases,Leakage Jars,Total,Paid,Credit";
  const lines = rows.map((row) => {
    const safe = [
      row.export_date,
      row.vehicle_number,
      row.owner_name,
      row.jar_count,
      row.bottle_case_count,
      row.return_jar_count,
      row.return_bottle_case_count,
      row.leakage_jar_count,
      row.total_amount,
      row.paid_amount,
      row.credit_amount
    ].map((val) => `"${String(val ?? "").replace(/\"/g, "\"\"")}"`);
    return safe.join(",");
  });
  res.setHeader("Content-Type", "text/csv");
  res.setHeader("Content-Disposition", `attachment; filename="daily_close_${date}.csv"`);
  res.send([header, ...lines].join("\n"));
});

router.get("/reports/customer-ledger", (req, res) => {
  const from = req.query.from || dayjs().startOf("month").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const customers = getCustomerReportOptions();
  const { selectedCustomer, customer, customerId, customerRef } = resolveCustomerReportSelection(req.query);

  let entries = [];
  let totals = { credit_total: 0, payment_total: 0, balance: 0 };

  if (customer) {
    ({ entries, totals } = buildCustomerLedgerEntries({
      selectedCustomer,
      customer,
      from,
      to,
      t: req.t
    }));
  }

  res.render("admin/report_customer_ledger", {
    title: req.t("customerLedgerTitle"),
    from,
    to,
    customerId,
    customer,
    customerRef,
    customers,
    entries,
    totals
  });
});

router.get("/reports/customer-ledger/print", (req, res) => {
  const from = req.query.from || dayjs().startOf("month").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const { selectedCustomer, customer, customerId } = resolveCustomerReportSelection(req.query);
  if (!customer) {
    return res.redirect("/admin/reports/customer-ledger");
  }

  const { entries, totals } = buildCustomerLedgerEntries({
    selectedCustomer,
    customer,
    from,
    to,
    t: req.t
  });

  res.render("admin/report_customer_ledger_print", {
    title: req.t("customerLedgerTitle"),
    from,
    to,
    customerId,
    customer,
    entries,
    totals
  });
});

router.get("/reports/customer-statement", (req, res) => {
  const from = req.query.from || dayjs().startOf("month").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const customers = getCustomerReportOptions();
  const { selectedCustomer, customer, customerId, customerRef } = resolveCustomerReportSelection(req.query);

  let entries = [];
  let totals = { credit_total: 0, payment_total: 0, balance: 0 };
  if (customer) {
    ({ entries, totals } = buildCustomerLedgerEntries({
      selectedCustomer,
      customer,
      from,
      to,
      t: req.t
    }));
  }

  res.render("admin/report_customer_statement", {
    title: req.t("customerStatementTitle"),
    from,
    to,
    customerId,
    customer,
    customerRef,
    customers,
    entries,
    totals
  });
});

router.get("/reports/customer-statement/print", (req, res) => {
  const from = req.query.from || dayjs().startOf("month").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const { selectedCustomer, customer, customerId } = resolveCustomerReportSelection(req.query);
  if (!customer) {
    return res.redirect("/admin/reports/customer-statement");
  }

  const { entries, totals } = buildCustomerLedgerEntries({
    selectedCustomer,
    customer,
    from,
    to,
    t: req.t
  });

  res.render("admin/report_customer_statement_print", {
    title: req.t("customerStatementTitle"),
    from,
    to,
    customerId,
    customer,
    entries,
    totals
  });
});

router.get("/reports/vehicle-trips", (req, res) => {
  const from = req.query.from || dayjs().subtract(7, "day").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");

  const rows = db.prepare(
    `SELECT exports.export_date, vehicles.vehicle_number, vehicles.owner_name,
            COUNT(exports.id) as trip_count,
            COALESCE(SUM(exports.jar_count), 0) as jars,
            COALESCE(SUM(exports.bottle_case_count), 0) as bottles
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     WHERE exports.export_date BETWEEN ? AND ?
     GROUP BY exports.export_date, exports.vehicle_id
     ORDER BY exports.export_date ASC, vehicles.vehicle_number ASC`
  ).all(from, to);

  const routes = db.prepare(
    `SELECT exports.export_date, vehicles.vehicle_number, exports.route
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     WHERE exports.export_date BETWEEN ? AND ?
       AND exports.route IS NOT NULL AND exports.route != ''
     ORDER BY exports.export_date DESC`
  ).all(from, to);

  res.render("admin/report_vehicle_trips", {
    title: req.t("vehicleTripsTitle"),
    from,
    to,
    rows,
    routes
  });
});

router.get("/reports/vehicle-trips/export", (req, res) => {
  const from = req.query.from || dayjs().subtract(7, "day").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const rows = db.prepare(
    `SELECT exports.export_date, vehicles.vehicle_number, vehicles.owner_name,
            COUNT(exports.id) as trip_count,
            COALESCE(SUM(exports.jar_count), 0) as jars,
            COALESCE(SUM(exports.bottle_case_count), 0) as bottles
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     WHERE exports.export_date BETWEEN ? AND ?
     GROUP BY exports.export_date, exports.vehicle_id
     ORDER BY exports.export_date ASC, vehicles.vehicle_number ASC`
  ).all(from, to);

  const header = "Date,Vehicle,Owner,Trips,Jars,Bottles";
  const lines = rows.map((row) => {
    const safe = [row.export_date, row.vehicle_number, row.owner_name, row.trip_count, row.jars, row.bottles]
      .map((val) => `"${String(val ?? "").replace(/\"/g, "\"\"")}"`);
    return safe.join(",");
  });
  res.setHeader("Content-Type", "text/csv");
  res.setHeader("Content-Disposition", `attachment; filename="vehicle_trips_${from}_to_${to}.csv"`);
  res.send([header, ...lines].join("\n"));
});

router.get("/reports/vehicle-trips/print", (req, res) => {
  const from = req.query.from || dayjs().subtract(7, "day").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");

  const rows = db.prepare(
    `SELECT exports.export_date, vehicles.vehicle_number, vehicles.owner_name,
            COUNT(exports.id) as trip_count,
            COALESCE(SUM(exports.jar_count), 0) as jars,
            COALESCE(SUM(exports.bottle_case_count), 0) as bottles
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     WHERE exports.export_date BETWEEN ? AND ?
     GROUP BY exports.export_date, exports.vehicle_id
     ORDER BY exports.export_date ASC, vehicles.vehicle_number ASC`
  ).all(from, to);

  const routes = db.prepare(
    `SELECT exports.export_date, vehicles.vehicle_number, exports.route
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     WHERE exports.export_date BETWEEN ? AND ?
       AND exports.route IS NOT NULL AND exports.route != ''
     ORDER BY exports.export_date DESC`
  ).all(from, to);

  res.render("admin/report_vehicle_trips_print", {
    title: req.t("vehicleTripsTitle"),
    from,
    to,
    rows,
    routes
  });
});

router.get("/reports/all", (req, res) => {
  const from = req.query.from || dayjs().startOf("month").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const today = dayjs().format("YYYY-MM-DD");
  const weekStart = dayjs().subtract(6, "day").format("YYYY-MM-DD");
  const monthStart = dayjs().startOf("month").format("YYYY-MM-DD");
  const yearStart = dayjs().startOf("year").format("YYYY-MM-DD");
  const collectionPrintSummary = getPrintableCollectionSummary(from, to);

  const exportsRows = db.prepare(
    `SELECT exports.*, vehicles.vehicle_number, vehicles.owner_name, users.full_name as recorded_by
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     LEFT JOIN users ON exports.created_by = users.id
     WHERE export_date BETWEEN ? AND ?
     ORDER BY export_date DESC, exports.created_at DESC`
  ).all(from, to);
  const exportTotals = db.prepare(
    `SELECT
        COUNT(exports.id) AS total_trips,
        COALESCE(SUM(exports.jar_count), 0) AS total_jars,
        COALESCE(SUM(exports.bottle_case_count), 0) AS total_bottles,
        COALESCE(SUM(exports.return_jar_count), 0) AS total_return_jars,
        COALESCE(SUM(exports.return_bottle_case_count), 0) AS total_return_bottles,
        COALESCE(SUM(exports.leakage_jar_count), 0) AS total_leakage_jars,
        COALESCE(SUM(exports.sold_jar_count), 0) AS total_sold_jars,
        COALESCE(SUM(exports.sold_jar_amount), 0) AS total_sold_amount,
        COALESCE(SUM(exports.total_amount), 0) AS total_amount,
        COALESCE(SUM(exports.paid_amount), 0) AS total_paid,
        COALESCE(SUM(exports.credit_amount), 0) AS total_credit
     FROM exports
     WHERE export_date BETWEEN ? AND ?`
  ).get(from, to);

  const creditsRows = db.prepare(
    `SELECT credits.*, vehicles.vehicle_number, vehicles.owner_name, users.full_name as recorded_by,
            CASE WHEN credits.amount - credits.paid_amount < 0 THEN 0 ELSE credits.amount - credits.paid_amount END AS remaining_amount
     FROM credits
     JOIN vehicles ON credits.vehicle_id = vehicles.id
     LEFT JOIN users ON credits.created_by = users.id
     WHERE credit_date BETWEEN ? AND ?
     ORDER BY credit_date DESC, credits.created_at DESC`
  ).all(from, to);
  const creditCreatedTotals = db.prepare(
    `SELECT
        COALESCE(SUM(amount), 0) AS total_amount,
        COALESCE(SUM(CASE WHEN amount - paid_amount < 0 THEN 0 ELSE amount - paid_amount END), 0) AS total_remaining,
        COALESCE(SUM(credit_jars), 0) AS total_jars,
        COALESCE(SUM(credit_bottle_cases), 0) AS total_bottles
     FROM credits
     WHERE credit_date BETWEEN ? AND ?`
  ).get(from, to);
  const creditPaidTotals = db.prepare(
    `SELECT COALESCE(SUM(amount), 0) AS total_paid
     FROM credit_payments
     WHERE date(paid_at) BETWEEN ? AND ?`
  ).get(from, to);
  const creditTotals = {
    ...creditCreatedTotals,
    total_paid: Number(creditPaidTotals?.total_paid || 0)
  };
  const exportCreditPaymentRows = getExportCreditPaymentReportRows({ from, to });
  const creditPaymentRows = getCustomerCreditPaymentReportRows({ from, to });

  const jarSalesRows = db.prepare(
    `SELECT jar_sales.*, jar_types.name as jar_name, users.full_name as recorded_by,
            vehicles.vehicle_number as vehicle_number_ref, vehicles.owner_name
     FROM jar_sales
     JOIN jar_types ON jar_sales.jar_type_id = jar_types.id
     LEFT JOIN vehicles ON jar_sales.vehicle_id = vehicles.id
     LEFT JOIN users ON jar_sales.created_by = users.id
     WHERE jar_sales.sale_date BETWEEN ? AND ?
     ORDER BY jar_sales.sale_date DESC, jar_sales.created_at DESC`
  ).all(from, to);
  const jarSalesTotals = jarSalesRows.reduce(
    (acc, row) => {
      acc.total += Number(row.total_amount || 0);
      acc.paid += Number(row.paid_amount || 0);
      acc.credit += Number(row.credit_amount || 0);
      acc.qty += Number(row.quantity || 0);
      return acc;
    },
    { total: 0, paid: 0, credit: 0, qty: 0 }
  );

  const leakageSalesRows = db.prepare(
    `SELECT leakage_jar_sales.*, users.full_name AS recorded_by
     FROM leakage_jar_sales
     LEFT JOIN users ON users.id = leakage_jar_sales.created_by
     WHERE leakage_jar_sales.sale_date BETWEEN ? AND ?
     ORDER BY leakage_jar_sales.sale_date DESC, leakage_jar_sales.created_at DESC`
  ).all(from, to);
  const leakageSalesTotals = leakageSalesRows.reduce(
    (acc, row) => {
      acc.qty += Number(row.quantity || 0);
      acc.total += Number(row.total_amount || 0);
      acc.paid += Number(row.paid_amount || 0);
      acc.credit += Number(row.credit_amount || 0);
      return acc;
    },
    { qty: 0, total: 0, paid: 0, credit: 0 }
  );

  const rangeExportMethod = db.prepare(
    `SELECT
        ${buildExportOpeningPaymentAggregateSql("exports", "export_credit_totals")},
        COALESCE(SUM(${buildCompanyMissingAmountSql("paid_cash_amount", "exports", "vehicles")}), 0) AS company_cash_adjustment
     FROM exports
     JOIN vehicles ON vehicles.id = exports.vehicle_id
     ${buildExportCreditTotalsJoin("exports", "export_credit_totals")}
     WHERE export_date BETWEEN ? AND ?`
  ).get(from, to);
  const rangeExportCreditMethod = db.prepare(
    `SELECT
        COALESCE(SUM(cash_amount), 0) AS cash_paid,
        COALESCE(SUM(bank_amount), 0) AS bank_paid,
        COALESCE(SUM(ewallet_amount), 0) AS ewallet_paid
     FROM export_credit_payments
     WHERE payment_date BETWEEN ? AND ?`
  ).get(from, to);
  const rangeCustomerMethod = db.prepare(
    `SELECT
        COALESCE(SUM(CASE WHEN payment_method = 'CASH' THEN amount ELSE 0 END), 0) AS cash_paid,
        COALESCE(SUM(CASE WHEN payment_method = 'BANK' THEN amount ELSE 0 END), 0) AS bank_paid,
        COALESCE(SUM(CASE WHEN payment_method = 'E_WALLET' THEN amount ELSE 0 END), 0) AS ewallet_paid
     FROM credit_payments
     WHERE date(paid_at) BETWEEN ? AND ?`
  ).get(from, to);
  const rangeRentMethod = db.prepare(
    `SELECT
        COALESCE(SUM(CASE WHEN add_to_collection = 1 AND payment_method = 'CASH' THEN amount ELSE 0 END), 0) AS cash_paid,
        COALESCE(SUM(CASE WHEN add_to_collection = 1 AND payment_method = 'BANK' THEN amount ELSE 0 END), 0) AS bank_paid,
        COALESCE(SUM(CASE WHEN add_to_collection = 1 AND payment_method = 'E_WALLET' THEN amount ELSE 0 END), 0) AS ewallet_paid
     FROM rent_entries
     WHERE rent_date BETWEEN ? AND ?`
  ).get(from, to);
  const rangeLeakageMethod = db.prepare(
    `SELECT
        COALESCE(SUM(cash_amount), 0) AS cash_paid,
        COALESCE(SUM(bank_amount), 0) AS bank_paid,
        COALESCE(SUM(ewallet_amount), 0) AS ewallet_paid
     FROM leakage_jar_sale_payments
     WHERE payment_date BETWEEN ? AND ?`
  ).get(from, to);
  const rangeJarSalesMethod = db.prepare(
    `SELECT
        COALESCE(SUM(amount), 0) AS amount,
        COALESCE(SUM(cash_amount), 0) AS cash_paid,
        COALESCE(SUM(bank_amount), 0) AS bank_paid,
        COALESCE(SUM(ewallet_amount), 0) AS ewallet_paid
     FROM jar_sale_payments
     WHERE payment_date BETWEEN ? AND ?`
  ).get(from, to);
  const rangeContainerLendingMethod = db.prepare(
    `SELECT
        COALESCE(SUM(amount), 0) AS amount,
        COALESCE(SUM(cash_amount), 0) AS cash_paid,
        COALESCE(SUM(bank_amount), 0) AS bank_paid,
        COALESCE(SUM(ewallet_amount), 0) AS ewallet_paid
     FROM jar_container_lending_payments
     WHERE payment_date BETWEEN ? AND ?`
  ).get(from, to);
  const rangeSavingsDeposits = db.prepare(
    `SELECT COALESCE(SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END), 0) AS amount
     FROM vehicle_savings
     WHERE entry_date BETWEEN ? AND ?`
  ).get(from, to);

  const rangeImportsFromCollection = db.prepare(
    `SELECT COALESCE(SUM(amount), 0) AS amount
     FROM import_payments
     WHERE payment_date BETWEEN ? AND ? AND payment_source = 'DAILY_COLLECTION'`
  ).get(from, to);
  const rangePurchasesFromCollection = db.prepare(
    `SELECT COALESCE(SUM(amount), 0) AS amount
     FROM company_purchase_payments
     WHERE payment_date BETWEEN ? AND ? AND payment_source = 'DAILY_COLLECTION'`
  ).get(from, to);
  const rangeVehicleExpensesFromCollection = db.prepare(
    `SELECT COALESCE(SUM(amount), 0) AS amount
     FROM vehicle_expense_payments
     WHERE payment_date BETWEEN ? AND ? AND payment_source = 'DAILY_COLLECTION'`
  ).get(from, to);
  const rangeStaffSalaryFromCollection = db.prepare(
    `SELECT COALESCE(SUM(amount), 0) AS amount
     FROM staff_salary_payments
     WHERE payment_date BETWEEN ? AND ? AND payment_source = 'DAILY_COLLECTION'`
  ).get(from, to);
  const rangeWorkerSalaryFromCollection = db.prepare(
    `SELECT COALESCE(SUM(amount), 0) AS amount
     FROM worker_salary_payments
     WHERE payment_date BETWEEN ? AND ? AND payment_source = 'DAILY_COLLECTION'`
  ).get(from, to);
  const rangeSavingsWithdrawFromCollection = db.prepare(
    `SELECT COALESCE(SUM(CASE WHEN amount < 0 AND payment_source = 'DAILY_COLLECTION' THEN ABS(amount) ELSE 0 END), 0) AS amount
     FROM vehicle_savings
     WHERE entry_date BETWEEN ? AND ?`
  ).get(from, to);
  const rangeContainerLendingRefunds = db.prepare(
    `SELECT COALESCE(SUM(refund_amount), 0) AS amount
     FROM jar_container_lending_returns
     WHERE return_date BETWEEN ? AND ?`
  ).get(from, to);

  const snapshotExportCash = parseMoneyValue(
    Number(rangeExportMethod.cash_paid || 0) +
    Number(rangeExportMethod.company_cash_adjustment || 0)
  );
  const snapshotCash = parseMoneyValue(
    snapshotExportCash +
    Number(rangeExportCreditMethod.cash_paid || 0) +
    Number(rangeCustomerMethod.cash_paid || 0) +
    Number(rangeRentMethod.cash_paid || 0) +
    Number(rangeLeakageMethod.cash_paid || 0) +
    Number(rangeJarSalesMethod.cash_paid || 0) +
    Number(rangeContainerLendingMethod.cash_paid || 0) +
    Number(rangeSavingsDeposits.amount || 0)
  );
  const snapshotBank = parseMoneyValue(
    Number(rangeExportMethod.bank_paid || 0) +
    Number(rangeExportCreditMethod.bank_paid || 0) +
    Number(rangeCustomerMethod.bank_paid || 0) +
    Number(rangeRentMethod.bank_paid || 0) +
    Number(rangeLeakageMethod.bank_paid || 0) +
    Number(rangeJarSalesMethod.bank_paid || 0) +
    Number(rangeContainerLendingMethod.bank_paid || 0)
  );
  const snapshotEWallet = parseMoneyValue(
    Number(rangeExportMethod.ewallet_paid || 0) +
    Number(rangeExportCreditMethod.ewallet_paid || 0) +
    Number(rangeCustomerMethod.ewallet_paid || 0) +
    Number(rangeRentMethod.ewallet_paid || 0) +
    Number(rangeLeakageMethod.ewallet_paid || 0) +
    Number(rangeJarSalesMethod.ewallet_paid || 0) +
    Number(rangeContainerLendingMethod.ewallet_paid || 0)
  );
  const snapshotCollection = parseMoneyValue(snapshotCash + snapshotBank + snapshotEWallet);
  const snapshotDeductions = parseMoneyValue(
    Number(rangeImportsFromCollection.amount || 0) +
    Number(rangePurchasesFromCollection.amount || 0) +
    Number(rangeVehicleExpensesFromCollection.amount || 0) +
    Number(rangeStaffSalaryFromCollection.amount || 0) +
    Number(rangeWorkerSalaryFromCollection.amount || 0) +
    Number(rangeSavingsWithdrawFromCollection.amount || 0) +
    Number(rangeContainerLendingRefunds.amount || 0)
  );
  const snapshotNet = parseMoneyValue(snapshotCollection - snapshotDeductions);

  const importsRows = db.prepare(
    `SELECT import_entries.*, jar_types.name as jar_type_name, jar_cap_types.name as jar_cap_type_name,
            users.full_name as recorded_by
     FROM import_entries
     LEFT JOIN jar_types ON import_entries.jar_type_id = jar_types.id
     LEFT JOIN jar_cap_types ON import_entries.jar_cap_type_id = jar_cap_types.id
     LEFT JOIN users ON import_entries.created_by = users.id
     WHERE entry_date BETWEEN ? AND ?
     ORDER BY entry_date DESC, created_at DESC`
  ).all(from, to);
  const importTotals = importsRows.reduce((acc, row) => {
    const qty = Number(row.quantity || 0);
    const sign = row.direction === "OUT" ? -1 : 1;
    acc.incoming += row.direction === "OUT" ? 0 : qty;
    acc.outgoing += row.direction === "OUT" ? qty : 0;
    acc.total += sign * qty;
    acc.byItem[row.item_type] = (acc.byItem[row.item_type] || 0) + (sign * qty);
    return acc;
  }, { total: 0, incoming: 0, outgoing: 0, byItem: {} });

  const savingsRows = db.prepare(
    `SELECT vehicle_savings.*, vehicles.vehicle_number, vehicles.owner_name, users.full_name as recorded_by
     FROM vehicle_savings
     JOIN vehicles ON vehicle_savings.vehicle_id = vehicles.id
     LEFT JOIN users ON vehicle_savings.created_by = users.id
     WHERE entry_date BETWEEN ? AND ?
     ORDER BY entry_date DESC, created_at DESC`
  ).all(from, to);
  const savingsTotals = savingsRows.reduce(
    (acc, row) => {
      const amt = Number(row.amount || 0);
      acc.total += amt;
      acc.deposits += amt > 0 ? amt : 0;
      acc.withdraws += amt < 0 ? Math.abs(amt) : 0;
      return acc;
    },
    { total: 0, deposits: 0, withdraws: 0 }
  );

  res.render("admin/report_all", {
    title: req.t("allReportsTitle"),
    from,
    to,
    today,
    weekStart,
    monthStart,
    yearStart,
    exportsRows,
    exportTotals,
    exportCreditPaymentRows,
    creditsRows,
    creditPaymentRows,
    creditTotals,
    jarSalesRows,
    jarSalesTotals,
    leakageSalesRows,
    leakageSalesTotals,
    importsRows,
    importTotals,
    savingsRows,
    savingsTotals,
    collectionPrintSummary,
    snapshotSummary: {
      collection: snapshotCollection,
      cash: snapshotCash,
      bank: snapshotBank,
      eWallet: snapshotEWallet,
      deductions: snapshotDeductions,
      net: snapshotNet,
      exports: {
        cash: snapshotExportCash,
        bank: parseMoneyValue(rangeExportMethod.bank_paid || 0),
        eWallet: parseMoneyValue(rangeExportMethod.ewallet_paid || 0)
      },
      exportCreditPayments: {
        cash: parseMoneyValue(rangeExportCreditMethod.cash_paid || 0),
        bank: parseMoneyValue(rangeExportCreditMethod.bank_paid || 0),
        eWallet: parseMoneyValue(rangeExportCreditMethod.ewallet_paid || 0)
      },
      customerCredits: {
        cash: parseMoneyValue(rangeCustomerMethod.cash_paid || 0),
        bank: parseMoneyValue(rangeCustomerMethod.bank_paid || 0),
        eWallet: parseMoneyValue(rangeCustomerMethod.ewallet_paid || 0)
      },
      containerDeposits: {
        cash: parseMoneyValue(rangeContainerLendingMethod.cash_paid || 0),
        bank: parseMoneyValue(rangeContainerLendingMethod.bank_paid || 0),
        eWallet: parseMoneyValue(rangeContainerLendingMethod.ewallet_paid || 0)
      },
      rentIncome: {
        cash: parseMoneyValue(rangeRentMethod.cash_paid || 0),
        bank: parseMoneyValue(rangeRentMethod.bank_paid || 0),
        eWallet: parseMoneyValue(rangeRentMethod.ewallet_paid || 0)
      },
      leakageJarSales: {
        cash: parseMoneyValue(rangeLeakageMethod.cash_paid || 0),
        bank: parseMoneyValue(rangeLeakageMethod.bank_paid || 0),
        eWallet: parseMoneyValue(rangeLeakageMethod.ewallet_paid || 0)
      },
      jarSales: {
        cash: parseMoneyValue(rangeJarSalesMethod.cash_paid || 0),
        bank: parseMoneyValue(rangeJarSalesMethod.bank_paid || 0),
        eWallet: parseMoneyValue(rangeJarSalesMethod.ewallet_paid || 0)
      },
      savingsDepositsCash: parseMoneyValue(rangeSavingsDeposits.amount || 0),
      deductionRows: {
        imports: parseMoneyValue(rangeImportsFromCollection.amount || 0),
        companyPurchases: parseMoneyValue(rangePurchasesFromCollection.amount || 0),
        vehicleExpenses: parseMoneyValue(rangeVehicleExpensesFromCollection.amount || 0),
        staffSalaries: parseMoneyValue(rangeStaffSalaryFromCollection.amount || 0),
        workerSalaries: parseMoneyValue(rangeWorkerSalaryFromCollection.amount || 0),
        savingsWithdrawals: parseMoneyValue(rangeSavingsWithdrawFromCollection.amount || 0),
        containerDepositRefunds: parseMoneyValue(rangeContainerLendingRefunds.amount || 0)
      }
    }
  });
});

router.get("/reports/collection-summary/print", (req, res) => {
  const from = req.query.from || dayjs().startOf("month").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const summary = getPrintableCollectionSummary(from, to);

  res.render("admin/report_collection_summary_print", {
    title: req.t("collectionSummaryTitle"),
    from,
    to,
    summary
  });
});

router.get("/reports/all/export", (req, res) => {
  const from = req.query.from || dayjs().startOf("month").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");

  const exportsRows = db.prepare(
    `SELECT exports.export_date, vehicles.vehicle_number, vehicles.owner_name,
            exports.jar_count, exports.bottle_case_count, exports.return_jar_count, exports.return_bottle_case_count, exports.leakage_jar_count,
            exports.sold_jar_count, exports.sold_jar_amount, exports.total_amount, exports.paid_amount, exports.credit_amount
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     WHERE export_date BETWEEN ? AND ?
     ORDER BY export_date ASC`
  ).all(from, to);

  const creditsRows = db.prepare(
    `SELECT credits.credit_date, vehicles.vehicle_number, vehicles.owner_name, credits.customer_name,
            credits.amount, credits.paid_amount,
            CASE WHEN credits.amount - credits.paid_amount < 0 THEN 0 ELSE credits.amount - credits.paid_amount END AS remaining_amount
     FROM credits
     JOIN vehicles ON credits.vehicle_id = vehicles.id
     WHERE credit_date BETWEEN ? AND ?
     ORDER BY credit_date ASC`
  ).all(from, to);
  const exportCreditPaymentRows = getExportCreditPaymentReportRows({ from, to });
  const creditPaymentRows = getCustomerCreditPaymentReportRows({ from, to });

  const jarSalesRows = db.prepare(
    `SELECT jar_sales.sale_date, jar_types.name as jar_name, jar_sales.customer_name,
            COALESCE(vehicles.vehicle_number, jar_sales.vehicle_number) as vehicle_number,
            vehicles.owner_name as owner_name,
            jar_sales.quantity, jar_sales.unit_price, jar_sales.total_amount, jar_sales.paid_amount, jar_sales.credit_amount
     FROM jar_sales
     JOIN jar_types ON jar_sales.jar_type_id = jar_types.id
     LEFT JOIN vehicles ON jar_sales.vehicle_id = vehicles.id
     WHERE jar_sales.sale_date BETWEEN ? AND ?
     ORDER BY jar_sales.sale_date ASC`
  ).all(from, to);

  const leakageSalesRows = db.prepare(
    `SELECT leakage_jar_sales.sale_date, leakage_jar_sales.quantity, leakage_jar_sales.unit_price,
            leakage_jar_sales.total_amount, leakage_jar_sales.paid_amount, leakage_jar_sales.credit_amount,
            leakage_jar_sales.payment_method, leakage_jar_sales.note
     FROM leakage_jar_sales
     WHERE leakage_jar_sales.sale_date BETWEEN ? AND ?
     ORDER BY leakage_jar_sales.sale_date ASC`
  ).all(from, to);

  const importsRows = db.prepare(
    `SELECT import_entries.entry_date, import_entries.item_type, import_entries.quantity,
            import_entries.direction, import_entries.note, jar_types.name as jar_type_name,
            jar_cap_types.name as jar_cap_type_name
     FROM import_entries
     LEFT JOIN jar_types ON import_entries.jar_type_id = jar_types.id
     LEFT JOIN jar_cap_types ON import_entries.jar_cap_type_id = jar_cap_types.id
     WHERE entry_date BETWEEN ? AND ?
     ORDER BY entry_date ASC`
  ).all(from, to);

  const savingsRows = db.prepare(
    `SELECT entry_date, vehicles.vehicle_number, vehicles.owner_name, amount, payment_source, note
     FROM vehicle_savings
     JOIN vehicles ON vehicle_savings.vehicle_id = vehicles.id
     WHERE entry_date BETWEEN ? AND ?
     ORDER BY entry_date ASC`
  ).all(from, to);

  const sections = [];
  sections.push("EXPORTS");
  sections.push("Date,Vehicle Number,Owner,Jar Count,Bottle Cases,Return Jars,Return Bottles,Leakage Jars,Sold Jars,Sold Amount,Total, Paid, Credit");
  exportsRows.forEach((row) => {
    sections.push([
      row.export_date,
      row.vehicle_number,
      row.owner_name,
      row.jar_count,
      row.bottle_case_count,
      row.return_jar_count,
      row.return_bottle_case_count,
      row.leakage_jar_count,
      row.sold_jar_count,
      row.sold_jar_amount,
      row.total_amount,
      row.paid_amount,
      row.credit_amount
    ].map((val) => `"${String(val ?? "").replace(/\"/g, "\"\"")}"`).join(","));
  });

  sections.push("");
  sections.push("EXPORT_CREDIT_PAYMENTS");
  sections.push("Payment Date,Receipt No,Vehicle Number,Owner,Export Date,Export Receipt,Amount,Payment Method,Note");
  exportCreditPaymentRows.forEach((row) => {
    sections.push([
      row.payment_date,
      row.receipt_no || "",
      row.vehicle_number || "",
      row.owner_name || "",
      row.export_date || "",
      row.export_receipt_no || "",
      row.amount,
      row.payment_method || "",
      row.note || ""
    ].map((val) => `"${String(val ?? "").replace(/\"/g, "\"\"")}"`).join(","));
  });

  sections.push("");
  sections.push("CREDITS");
  sections.push("Date,Vehicle Number,Owner,Customer,Amount,Remaining");
  creditsRows.forEach((row) => {
    sections.push([
      row.credit_date,
      row.vehicle_number,
      row.owner_name,
      row.customer_name || "",
      row.amount,
      row.remaining_amount
    ].map((val) => `"${String(val ?? "").replace(/\"/g, "\"\"")}"`).join(","));
  });

  sections.push("");
  sections.push("CUSTOMER_CREDIT_PAYMENTS");
  sections.push("Payment Date,Receipt No,Customer,Vehicle Number,Owner,Credit Date,Amount,Payment Method,Note");
  creditPaymentRows.forEach((row) => {
    sections.push([
      row.payment_date,
      row.receipt_no || "",
      row.customer_name || "",
      row.vehicle_number || "",
      row.owner_name || "",
      row.credit_date || "",
      row.amount,
      row.payment_method || "",
      row.note || ""
    ].map((val) => `"${String(val ?? "").replace(/\"/g, "\"\"")}"`).join(","));
  });

  sections.push("");
  sections.push("JAR_CONTAINER_SALES");
  sections.push("Date,Jar Type,Person Name,Vehicle Number,Owner,Quantity,Unit Price,Total,Paid,Credit");
  jarSalesRows.forEach((row) => {
    sections.push([
      row.sale_date,
      row.jar_name,
      row.customer_name || "",
      row.vehicle_number || "",
      row.owner_name || "",
      row.quantity,
      row.unit_price,
      row.total_amount,
      row.paid_amount,
      row.credit_amount
    ].map((val) => `"${String(val ?? "").replace(/\"/g, "\"\"")}"`).join(","));
  });

  sections.push("");
  sections.push("LEAKAGE_JAR_SALES");
  sections.push("Date,Quantity,Unit Price,Total,Paid,Credit,Payment Method,Note");
  leakageSalesRows.forEach((row) => {
    sections.push([
      row.sale_date,
      row.quantity,
      row.unit_price,
      row.total_amount,
      row.paid_amount,
      row.credit_amount,
      row.payment_method || "",
      row.note || ""
    ].map((val) => `"${String(val ?? "").replace(/\"/g, "\"\"")}"`).join(","));
  });

  sections.push("");
  sections.push("IMPORTS");
  sections.push("Date,Item Type,Jar Container Type,Jar Cap Type,Direction,Quantity,Note");
  importsRows.forEach((row) => {
    sections.push([
      row.entry_date,
      row.item_type,
      row.item_type === "JAR_CONTAINER" ? (row.jar_type_name || "") : "",
      row.item_type === "JAR_CAP" ? (row.jar_cap_type_name || "") : "",
      row.direction || "IN",
      row.quantity,
      row.note || ""
    ].map((val) => `"${String(val ?? "").replace(/\"/g, "\"\"")}"`).join(","));
  });

  sections.push("");
  sections.push("SAVINGS");
  sections.push("Date,Vehicle Number,Owner,Type,Source,Amount,Note");
  savingsRows.forEach((row) => {
    const entryType = Number(row.amount || 0) < 0 ? "withdraw" : "deposit";
    sections.push([
      row.entry_date,
      row.vehicle_number,
      row.owner_name,
      entryType,
      row.payment_source || "DAILY_COLLECTION",
      Math.abs(Number(row.amount || 0)),
      row.note || ""
    ].map((val) => `"${String(val ?? "").replace(/\"/g, "\"\"")}"`).join(","));
  });

  const csv = sections.join("\n");
  res.setHeader("Content-Type", "text/csv");
  res.setHeader("Content-Disposition", `attachment; filename="all_reports_${from}_to_${to}.csv"`);
  res.send(csv);
});

router.post("/imports/:id/delete", (req, res) => {
  const entry = db.prepare("SELECT * FROM import_entries WHERE id = ?").get(req.params.id);
  if (!entry) return res.redirect("/admin/reports/all");
  const recycleId = createRecycleEntry({
    entityType: "import_entry",
    entityId: req.params.id,
    payload: { import_entry: entry },
    deletedBy: req.session.userId,
    note: `date=${entry.entry_date || ""}; item=${entry.item_type || ""}`
  });
  logActivity({
    userId: req.session.userId,
    action: "delete",
    entityType: "import_entry",
    entityId: req.params.id,
    details: `recycle_id=${recycleId}`
  });
  db.prepare("DELETE FROM import_entries WHERE id = ?").run(req.params.id);
  const back = req.get("referer");
  res.redirect(back || "/admin/reports/all");
});

router.get("/reports/stock-ledger", (req, res) => {
  const from = req.query.from || dayjs().startOf("month").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const item = req.query.item || "all";

  const itemClause = item === "all" ? "" : "AND item_type = ?";
  const params = item === "all" ? [from, to] : [from, to, item];

  const entries = db.prepare(
    `SELECT stock_ledger.*, users.full_name as recorded_by
     FROM stock_ledger
     LEFT JOIN users ON stock_ledger.created_by = users.id
     WHERE entry_date BETWEEN ? AND ?
     ${itemClause}
     ORDER BY entry_date DESC, created_at DESC`
  ).all(...params);

  const totals = entries.reduce(
    (acc, row) => {
      const qty = Number(row.quantity || 0);
      if (row.item_type === "JAR") {
        row.direction === "IN" ? acc.jar_in += qty : acc.jar_out += qty;
      } else {
        row.direction === "IN" ? acc.bottle_in += qty : acc.bottle_out += qty;
      }
      return acc;
    },
    { jar_in: 0, jar_out: 0, bottle_in: 0, bottle_out: 0 }
  );

  const exportsTotals = db.prepare(
    `SELECT
        COALESCE(SUM(CASE WHEN jar_count - return_jar_count - leakage_jar_count < 0 THEN 0 ELSE jar_count - return_jar_count - leakage_jar_count END), 0) as net_jars,
        COALESCE(SUM(CASE WHEN bottle_case_count - return_bottle_case_count < 0 THEN 0 ELSE bottle_case_count - return_bottle_case_count END), 0) as net_bottles,
        COALESCE(SUM(leakage_jar_count), 0) as leakage_jars,
        COALESCE(SUM(sold_jar_count), 0) as sold_jars
     FROM exports
     WHERE export_date BETWEEN ? AND ?`
  ).get(from, to);

  const jarSalesTotals = db.prepare(
    `SELECT
        COALESCE(SUM(jar_sales.quantity), 0) as sold_jars,
        COALESCE(SUM(CASE WHEN vehicles.is_company = 1 THEN jar_sales.quantity ELSE 0 END), 0) as company_jars
     FROM jar_sales
     LEFT JOIN vehicles ON jar_sales.vehicle_id = vehicles.id
     WHERE jar_sales.sale_date BETWEEN ? AND ?`
  ).get(from, to);

  res.render("admin/report_stock_ledger", {
    title: req.t("stockLedgerTitle"),
    from,
    to,
    item,
    entries,
    totals,
    exportsTotals,
    jarSalesTotals
  });
});

router.post("/reports/stock-ledger", (req, res) => {
  const { item_type, direction, quantity, entry_date, note } = req.body;
  if (!item_type || !direction || !entry_date) {
    return res.redirect("/admin/reports/stock-ledger");
  }
  const qty = Number(quantity || 0);
  if (Number.isNaN(qty) || qty <= 0) {
    return res.redirect("/admin/reports/stock-ledger");
  }
  db.prepare(
    "INSERT INTO stock_ledger (item_type, direction, quantity, entry_date, note, created_by) VALUES (?, ?, ?, ?, ?, ?)"
  ).run(item_type, direction, qty, entry_date, note || null, req.session.userId);
  logActivity({
    userId: req.session.userId,
    action: "create",
    entityType: "stock_ledger",
    entityId: `${item_type}_${entry_date}`,
    details: `direction=${direction}, qty=${qty}`
  });
  res.redirect(`/admin/reports/stock-ledger?from=${entry_date}&to=${entry_date}`);
});

router.get("/reports/stock-ledger/export", (req, res) => {
  const from = req.query.from || dayjs().startOf("month").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const item = req.query.item || "all";
  const itemClause = item === "all" ? "" : "AND item_type = ?";
  const params = item === "all" ? [from, to] : [from, to, item];

  const entries = db.prepare(
    `SELECT entry_date, item_type, direction, quantity, note
     FROM stock_ledger
     WHERE entry_date BETWEEN ? AND ?
     ${itemClause}
     ORDER BY entry_date ASC`
  ).all(...params);

  const header = "Date,Item,Direction,Quantity,Note";
  const lines = entries.map((row) => {
    const safe = [row.entry_date, row.item_type, row.direction, row.quantity, row.note || ""]
      .map((val) => `"${String(val ?? "").replace(/\"/g, "\"\"")}"`);
    return safe.join(",");
  });
  res.setHeader("Content-Type", "text/csv");
  res.setHeader("Content-Disposition", `attachment; filename="stock_ledger_${from}_to_${to}.csv"`);
  res.send([header, ...lines].join("\n"));
});

router.get("/reports/stock-ledger/print", (req, res) => {
  const from = req.query.from || dayjs().startOf("month").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const item = req.query.item || "all";
  const itemClause = item === "all" ? "" : "AND item_type = ?";
  const params = item === "all" ? [from, to] : [from, to, item];

  const entries = db.prepare(
    `SELECT entry_date, item_type, direction, quantity, note
     FROM stock_ledger
     WHERE entry_date BETWEEN ? AND ?
     ${itemClause}
     ORDER BY entry_date ASC`
  ).all(...params);

  res.render("admin/report_stock_ledger_print", {
    title: req.t("stockLedgerTitle"),
    from,
    to,
    item,
    entries
  });
});

router.get("/reports/profit", (req, res) => {
  const from = req.query.from || dayjs().startOf("month").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");

  const rows = db.prepare(
    `SELECT export_date,
            COALESCE(SUM(total_amount), 0) as total_amount,
            COALESCE(SUM(leakage_jar_count), 0) as leakage_jars,
            COALESCE(SUM(leakage_jar_count * jar_unit_price), 0) as leakage_cost
     FROM exports
     WHERE export_date BETWEEN ? AND ?
     GROUP BY export_date
     ORDER BY export_date ASC`
  ).all(from, to);

  const summary = rows.reduce(
    (acc, row) => {
      acc.total_sales += Number(row.total_amount || 0);
      acc.leakage_cost += Number(row.leakage_cost || 0);
      return acc;
    },
    { total_sales: 0, leakage_cost: 0 }
  );
  summary.profit = summary.total_sales - summary.leakage_cost;

  res.render("admin/report_profit", {
    title: req.t("profitSummaryTitle"),
    from,
    to,
    rows,
    summary
  });
});

router.get("/reports/profit/print", (req, res) => {
  const from = req.query.from || dayjs().startOf("month").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");

  const rows = db.prepare(
    `SELECT export_date,
            COALESCE(SUM(total_amount), 0) as total_amount,
            COALESCE(SUM(leakage_jar_count), 0) as leakage_jars,
            COALESCE(SUM(leakage_jar_count * jar_unit_price), 0) as leakage_cost
     FROM exports
     WHERE export_date BETWEEN ? AND ?
     GROUP BY export_date
     ORDER BY export_date ASC`
  ).all(from, to);

  const summary = rows.reduce(
    (acc, row) => {
      acc.total_sales += Number(row.total_amount || 0);
      acc.leakage_cost += Number(row.leakage_cost || 0);
      return acc;
    },
    { total_sales: 0, leakage_cost: 0 }
  );
  summary.profit = summary.total_sales - summary.leakage_cost;

  res.render("admin/report_profit_print", {
    title: req.t("profitSummaryTitle"),
    from,
    to,
    rows,
    summary
  });
});

router.get("/reports/staff-scorecard", (req, res) => {
  const from = req.query.from || dayjs().startOf("month").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");

  const users = db.prepare("SELECT id, full_name, role FROM users ORDER BY full_name").all();

  const rows = users.map((user) => {
    const exportStats = db.prepare(
      `SELECT COUNT(*) as count, COALESCE(SUM(total_amount), 0) as total
       FROM exports
       WHERE created_by = ?
         AND export_date BETWEEN ? AND ?`
    ).get(user.id, from, to);
    const creditStats = db.prepare(
      `SELECT COUNT(*) as count, COALESCE(SUM(amount), 0) as total
       FROM credits
       WHERE created_by = ?
         AND credit_date BETWEEN ? AND ?`
    ).get(user.id, from, to);
    const paymentStats = db.prepare(
      `SELECT COUNT(*) as count, COALESCE(SUM(amount), 0) as total
       FROM credit_payments
       WHERE created_by = ?
         AND date(paid_at) BETWEEN ? AND ?`
    ).get(user.id, from, to);

    return {
      full_name: user.full_name,
      role: user.role,
      export_count: exportStats.count || 0,
      export_total: exportStats.total || 0,
      credit_count: creditStats.count || 0,
      credit_total: creditStats.total || 0,
      payment_count: paymentStats.count || 0,
      payment_total: paymentStats.total || 0
    };
  });

  res.render("admin/report_staff_scorecard", {
    title: req.t("staffScorecardTitle"),
    from,
    to,
    rows
  });
});

router.get("/reports/staff-scorecard/print", (req, res) => {
  const from = req.query.from || dayjs().startOf("month").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const users = db.prepare("SELECT id, full_name, role FROM users ORDER BY full_name").all();
  const rows = users.map((user) => {
    const exportStats = db.prepare(
      `SELECT COUNT(*) as count, COALESCE(SUM(total_amount), 0) as total
       FROM exports
       WHERE created_by = ?
         AND export_date BETWEEN ? AND ?`
    ).get(user.id, from, to);
    const creditStats = db.prepare(
      `SELECT COUNT(*) as count, COALESCE(SUM(amount), 0) as total
       FROM credits
       WHERE created_by = ?
         AND credit_date BETWEEN ? AND ?`
    ).get(user.id, from, to);
    const paymentStats = db.prepare(
      `SELECT COUNT(*) as count, COALESCE(SUM(amount), 0) as total
       FROM credit_payments
       WHERE created_by = ?
         AND date(paid_at) BETWEEN ? AND ?`
    ).get(user.id, from, to);

    return {
      full_name: user.full_name,
      role: user.role,
      export_count: exportStats.count || 0,
      export_total: exportStats.total || 0,
      credit_count: creditStats.count || 0,
      credit_total: creditStats.total || 0,
      payment_count: paymentStats.count || 0,
      payment_total: paymentStats.total || 0
    };
  });

  res.render("admin/report_staff_scorecard_print", {
    title: req.t("staffScorecardTitle"),
    from,
    to,
    rows
  });
});

router.get("/reports/worker-activity", (req, res) => {
  const from = req.query.from || dayjs().subtract(7, "day").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");

  const rows = db.prepare(
    `SELECT users.id, users.full_name, users.username, users.role,
            COALESCE(exports.export_count, 0) as export_count,
            COALESCE(exports.export_total, 0) as export_total,
            COALESCE(credits.credit_count, 0) as credit_count,
            COALESCE(credits.credit_total, 0) as credit_total
     FROM users
     LEFT JOIN (
       SELECT created_by,
              COUNT(*) as export_count,
              COALESCE(SUM(total_amount), 0) as export_total
       FROM exports
       WHERE export_date BETWEEN ? AND ?
       GROUP BY created_by
     ) exports ON exports.created_by = users.id
     LEFT JOIN (
       SELECT created_by,
              COUNT(*) as credit_count,
              COALESCE(SUM(amount), 0) as credit_total
       FROM credits
       WHERE credit_date BETWEEN ? AND ?
       GROUP BY created_by
     ) credits ON credits.created_by = users.id
     WHERE users.role = 'WORKER'
     ORDER BY export_total DESC, credit_total DESC`
  ).all(from, to, from, to);

  res.render("admin/report_worker_activity", {
    title: req.t("workerActivityTitle"),
    from,
    to,
    rows
  });
});

router.get("/reports/worker-activity/:id", (req, res) => {
  const from = req.query.from || dayjs().subtract(7, "day").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const vehicle = (req.query.vehicle || "").trim();
  const customer = (req.query.customer || "").trim();
  const worker = db.prepare("SELECT id, full_name, username FROM users WHERE id = ? AND role = 'WORKER'").get(req.params.id);
  if (!worker) return res.redirect("/admin/reports/worker-activity");

  const vehicles = db.prepare("SELECT id, vehicle_number, owner_name FROM vehicles ORDER BY vehicle_number").all();
  const customers = db.prepare("SELECT DISTINCT customer_name FROM credits ORDER BY customer_name").all()
    .map((row) => row.customer_name);

  const exportSearchClause = vehicle ? "AND exports.vehicle_id = ?" : "";
  const exportParams = vehicle ? [worker.id, from, to, vehicle] : [worker.id, from, to];
  const exportsRows = db.prepare(
    `SELECT exports.*, vehicles.vehicle_number, vehicles.owner_name
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     WHERE exports.created_by = ?
       AND exports.export_date BETWEEN ? AND ?
       ${exportSearchClause}
     ORDER BY exports.export_date DESC, exports.created_at DESC`
  ).all(...exportParams);

  const creditSearchClause = customer ? "AND credits.customer_name = ?" : "";
  const creditParams = customer ? [worker.id, from, to, customer] : [worker.id, from, to];
  const creditsRows = db.prepare(
    `SELECT credits.*, vehicles.vehicle_number, vehicles.owner_name,
            CASE WHEN credits.amount - credits.paid_amount < 0 THEN 0 ELSE credits.amount - credits.paid_amount END AS remaining_amount
     FROM credits
     JOIN vehicles ON credits.vehicle_id = vehicles.id
     WHERE credits.created_by = ?
       AND credits.credit_date BETWEEN ? AND ?
       ${creditSearchClause}
     ORDER BY credits.credit_date DESC, credits.created_at DESC`
  ).all(...creditParams);

  const exportTotals = exportsRows.reduce(
    (acc, row) => {
      acc.count += 1;
      acc.jars += Number(row.jar_count || 0);
      acc.bottles += Number(row.bottle_case_count || 0);
      acc.total += Number(row.total_amount || 0);
      return acc;
    },
    { count: 0, jars: 0, bottles: 0, total: 0 }
  );
  const creditTotals = creditsRows.reduce(
    (acc, row) => {
      acc.count += 1;
      acc.amount += Number(row.amount || 0);
      acc.remaining += Number(row.remaining_amount || 0);
      return acc;
    },
    { count: 0, amount: 0, remaining: 0 }
  );

  res.render("admin/report_worker_history", {
    title: req.t("workerHistoryTitle"),
    from,
    to,
    vehicle,
    customer,
    vehicles,
    customers,
    worker,
    exportsRows,
    creditsRows,
    exportTotals,
    creditTotals
  });
});

router.get("/reports/worker-activity/:id/export-csv", (req, res) => {
  const from = req.query.from || dayjs().subtract(7, "day").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const vehicle = (req.query.vehicle || "").trim();
  const worker = db.prepare("SELECT id, full_name FROM users WHERE id = ? AND role = 'WORKER'").get(req.params.id);
  if (!worker) return res.redirect("/admin/reports/worker-activity");

  const exportSearchClause = vehicle ? "AND exports.vehicle_id = ?" : "";
  const exportParams = vehicle ? [worker.id, from, to, vehicle] : [worker.id, from, to];

  const exportsRows = db.prepare(
    `SELECT exports.export_date, vehicles.vehicle_number, vehicles.owner_name,
            exports.jar_count, exports.bottle_case_count, exports.total_amount
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     WHERE exports.created_by = ?
       AND exports.export_date BETWEEN ? AND ?
       ${exportSearchClause}
     ORDER BY exports.export_date ASC`
  ).all(...exportParams);

  const header = "Date,Vehicle Number,Owner Name,Jars,Bottle Cases,Total Amount";
  const lines = exportsRows.map((row) => {
    const safe = [
      row.export_date,
      row.vehicle_number,
      row.owner_name,
      row.jar_count,
      row.bottle_case_count,
      row.total_amount
    ].map((val) => {
      const str = String(val ?? "").replace(/\"/g, "\"\"");
      return `"${str}"`;
    });
    return safe.join(",");
  });

  const csv = [header, ...lines].join("\n");
  res.setHeader("Content-Type", "text/csv");
  res.setHeader("Content-Disposition", `attachment; filename="worker_${worker.id}_exports_${from}_to_${to}.csv"`);
  res.send(csv);
});

router.get("/reports/worker-activity/:id/credits-csv", (req, res) => {
  const from = req.query.from || dayjs().subtract(7, "day").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const customer = (req.query.customer || "").trim();
  const worker = db.prepare("SELECT id, full_name FROM users WHERE id = ? AND role = 'WORKER'").get(req.params.id);
  if (!worker) return res.redirect("/admin/reports/worker-activity");

  const creditSearchClause = customer ? "AND credits.customer_name = ?" : "";
  const creditParams = customer ? [worker.id, from, to, customer] : [worker.id, from, to];

  const creditsRows = db.prepare(
    `SELECT credits.credit_date, vehicles.vehicle_number, vehicles.owner_name, credits.customer_name,
            credits.amount, credits.paid_amount,
            CASE WHEN credits.amount - credits.paid_amount < 0 THEN 0 ELSE credits.amount - credits.paid_amount END AS remaining_amount
     FROM credits
     JOIN vehicles ON credits.vehicle_id = vehicles.id
     WHERE credits.created_by = ?
       AND credits.credit_date BETWEEN ? AND ?
       ${creditSearchClause}
     ORDER BY credits.credit_date ASC`
  ).all(...creditParams);

  const header = "Date,Vehicle Number,Owner Name,Customer,Amount,Paid Amount,Remaining Amount";
  const lines = creditsRows.map((row) => {
    const safe = [
      row.credit_date,
      row.vehicle_number,
      row.owner_name,
      row.customer_name,
      row.amount,
      row.paid_amount,
      row.remaining_amount
    ].map((val) => {
      const str = String(val ?? "").replace(/\"/g, "\"\"");
      return `"${str}"`;
    });
    return safe.join(",");
  });

  const csv = [header, ...lines].join("\n");
  res.setHeader("Content-Type", "text/csv");
  res.setHeader("Content-Disposition", `attachment; filename="worker_${worker.id}_credits_${from}_to_${to}.csv"`);
  res.send(csv);
});

router.get("/export/sales", (req, res) => {
  const from = req.query.from || dayjs().subtract(7, "day").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const rows = db.prepare(
    `SELECT exports.export_date as sale_date, vehicles.vehicle_number, vehicles.owner_name,
            COALESCE(SUM(exports.total_amount), 0) as total_sales
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     WHERE export_date BETWEEN ? AND ?
     GROUP BY exports.export_date, exports.vehicle_id
     ORDER BY export_date ASC`
  ).all(from, to);

  const header = "Date,Vehicle Number,Owner Name,Total Sales";
  const lines = rows.map((row) => {
    const safe = [row.sale_date, row.vehicle_number, row.owner_name, row.total_sales].map((val) => {
      const str = String(val ?? "").replace(/"/g, '""');
      return `"${str}"`;
    });
    return safe.join(",");
  });

  const csv = [header, ...lines].join("\n");
  res.setHeader("Content-Type", "text/csv");
  res.setHeader("Content-Disposition", `attachment; filename="sales_${from}_to_${to}.csv"`);
  res.send(csv);
});

router.get("/workers", (req, res) => {
  const includeInactive = String(req.query.include_inactive || "1") === "1";
  const q = String(req.query.q || "").trim();
  const whereParts = ["users.role = 'WORKER'"];
  const params = [];
  if (!includeInactive) {
    whereParts.push("users.is_active = 1");
  }
  if (q) {
    whereParts.push("(users.full_name LIKE ? OR users.username LIKE ? OR COALESCE(users.phone, '') LIKE ?)");
    params.push(`%${q}%`, `%${q}%`, `%${q}%`);
  }
  const whereClause = whereParts.join(" AND ");
  const rows = db.prepare(
    `SELECT users.id, users.full_name, users.username, users.phone, users.start_date, users.monthly_salary,
            users.is_active, users.deactivated_at,
            COALESCE(SUM(worker_salary_payments.amount), 0) AS paid_total
     FROM users
     LEFT JOIN worker_salary_payments ON worker_salary_payments.worker_id = users.id
     WHERE ${whereClause}
     GROUP BY users.id
     ORDER BY users.is_active DESC, users.created_at DESC`
  ).all(...params);
  const today = dayjs().format("YYYY-MM-DD");
  const workers = rows.map((row) => ({
    ...row,
    due_salary: computeSalaryDue("WORKER", row, row.paid_total, today)
  }));
  const success = req.query.archived || req.query.deleted
    ? req.t("workerDeactivated")
    : req.query.unarchived || req.query.activated
      ? req.t("workerActivated")
      : null;
  const error = req.query.error === "workerDeleteBlocked" ? req.t("workerDeleteBlocked") : null;
  res.render("admin/workers", { title: req.t("workersTitle"), workers, includeInactive, q, success, error });
});

router.get("/workers/new", (req, res) => {
  res.render("admin/worker_form", { title: req.t("addWorkerTitle"), user: null, document: null, error: null });
});

router.post("/workers", (req, res) => {
  const upload = staffUpload.fields(workerDocumentFields);
  upload(req, res, (err) => {
    const { full_name, username, phone, fingerprint_id, password, start_date, monthly_salary, doc_type } = req.body;
    const formUser = { full_name, username, phone, fingerprint_id, start_date, monthly_salary };
    const formDocument = { doc_type: normalizeDocumentType(doc_type) };

    if (err) {
      return res.render("admin/worker_form", {
        title: req.t("addWorkerTitle"),
        user: formUser,
        document: formDocument,
        error: err.message || req.t("uploadError")
      });
    }
    if (!full_name || !username || !password) {
      return res.render("admin/worker_form", {
        title: req.t("addWorkerTitle"),
        user: formUser,
        document: formDocument,
        error: req.t("workerRequired")
      });
    }
    const salaryRaw = Number(monthly_salary || 0);
    if (Number.isNaN(salaryRaw) || salaryRaw < 0) {
      return res.render("admin/worker_form", {
        title: req.t("addWorkerTitle"),
        user: formUser,
        document: formDocument,
        error: req.t("salaryInvalid")
      });
    }
    const salary = parseWholeMoney(salaryRaw);
    const fingerprintId = normalizeFingerprintId(fingerprint_id);
    const fpConflict = findFingerprintConflict({ fingerprintId });
    if (fpConflict) {
      return res.render("admin/worker_form", {
        title: req.t("addWorkerTitle"),
        user: formUser,
        document: formDocument,
        error: req.t("fingerprintIdAlreadyUsed")
      });
    }

    const hash = bcrypt.hashSync(password, 10);

    try {
      const workerId = db.prepare(
        "INSERT INTO users (full_name, username, phone, fingerprint_id, password_hash, role, start_date, monthly_salary) VALUES (?, ?, ?, ?, ?, 'WORKER', ?, ?)"
      ).run(
        full_name.trim(),
        username.trim(),
        phone ? phone.trim() : null,
        fingerprintId,
        hash,
        start_date || null,
        salary
      ).lastInsertRowid;

      upsertWorkerDocument({
        workerId,
        existingDocument: null,
        files: req.files,
        docTypeValue: doc_type
      });

      logActivity({
        userId: req.session.userId,
        action: "create",
        entityType: "worker",
        entityId: username.trim(),
        details: `name=${full_name.trim()}`
      });
      res.redirect("/admin/workers");
    } catch (insertErr) {
      res.render("admin/worker_form", {
        title: req.t("addWorkerTitle"),
        user: formUser,
        document: formDocument,
        error: req.t("usernameExists")
      });
    }
  });
});

router.get("/workers/:id", (req, res) => {
  const worker = db.prepare(
    "SELECT id, full_name, username, phone, fingerprint_id, start_date, monthly_salary, is_active, deactivated_at FROM users WHERE id = ? AND role = 'WORKER'"
  ).get(req.params.id);
  if (!worker) return res.redirect("/admin/workers");
  const document = db.prepare("SELECT * FROM worker_documents WHERE worker_id = ?").get(req.params.id);

  const payments = db.prepare(
    `SELECT worker_salary_payments.*, users.full_name as recorded_by
     FROM worker_salary_payments
     LEFT JOIN users ON worker_salary_payments.created_by = users.id
     WHERE worker_salary_payments.worker_id = ?
     ORDER BY worker_salary_payments.payment_date DESC, worker_salary_payments.id DESC`
  ).all(req.params.id);

  const totals = payments.reduce(
    (acc, row) => {
      const amt = Number(row.amount || 0);
      acc.total += amt;
      if (row.payment_type === "SALARY") acc.salary += amt;
      if (row.payment_type === "ADVANCE") acc.advance += amt;
      return acc;
    },
    { total: 0, salary: 0, advance: 0 }
  );
  const dueSalary = computeSalaryDue("WORKER", worker, totals.total, dayjs().format("YYYY-MM-DD"));
  const todayDate = dayjs().format("YYYY-MM-DD");
  const attendanceSummary = db.prepare(
    `SELECT
      COALESCE(SUM(CASE WHEN status = 'PRESENT' THEN 1 ELSE 0 END), 0) AS present_days,
      COALESCE(SUM(CASE WHEN status = 'ABSENT' THEN 1 ELSE 0 END), 0) AS absent_days,
      COUNT(*) AS marked_days
     FROM user_attendance
     WHERE user_id = ?`
  ).get(req.params.id);
  const absentDates = db.prepare(
    `SELECT attendance_date
     FROM user_attendance
     WHERE user_id = ?
       AND status = 'ABSENT'
     ORDER BY attendance_date DESC`
  ).all(req.params.id).map((row) => row.attendance_date);
  const errorKey = String(req.query.error || "").trim();
  const status = String(req.query.status || "").trim();
  const error = errorKey ? req.t(errorKey) : null;
  const success = status === "salary_adjusted" ? req.t("salaryAdjusted") : null;

  res.render("admin/worker_detail", {
    title: req.t("workerDetailTitle"),
    worker,
    document,
    payments,
    totals,
    dueSalary,
    todayDate,
    attendanceSummary,
    absentDates,
    error,
    success
  });
});

router.post("/workers/:id/salary-adjust", (req, res) => {
  const worker = db.prepare(
    "SELECT id, full_name, monthly_salary FROM users WHERE id = ? AND role = 'WORKER'"
  ).get(req.params.id);
  if (!worker) return res.redirect("/admin/workers");

  const adjustType = String(req.body.adjust_type || "").trim().toUpperCase();
  const adjustAmountRaw = Number(req.body.adjust_amount || 0);
  const adjustAmount = parseWholeMoney(adjustAmountRaw);
  const note = String(req.body.note || "").trim();
  if (!["INCREASE", "DECREASE"].includes(adjustType) || Number.isNaN(adjustAmountRaw) || adjustAmount <= 0) {
    return res.redirect(`/admin/workers/${req.params.id}?error=salaryAdjustmentInvalid`);
  }

  const currentSalary = parseWholeMoney(worker.monthly_salary || 0);
  const nextSalary = adjustType === "INCREASE"
    ? currentSalary + adjustAmount
    : currentSalary - adjustAmount;
  if (nextSalary < 0) {
    return res.redirect(`/admin/workers/${req.params.id}?error=salaryAdjustmentWouldBeNegative`);
  }

  runInTransaction(() => {
    recordSalaryAdjustment(db, {
      personType: "WORKER",
      personId: req.params.id,
      previousSalary: currentSalary,
      newSalary: nextSalary,
      effectiveMonth: dayjs().startOf("month").format("YYYY-MM-DD"),
      note,
      createdBy: req.session.userId
    });
    db.prepare("UPDATE users SET monthly_salary = ?, updated_at = datetime('now') WHERE id = ?").run(nextSalary, req.params.id);
  });
  logActivity({
    userId: req.session.userId,
    action: "update",
    entityType: "worker_salary_adjustment",
    entityId: req.params.id,
    details: `type=${adjustType}, amount=${adjustAmount}, from=${currentSalary}, to=${nextSalary}, note=${note || ""}`
  });

  return res.redirect(`/admin/workers/${req.params.id}?status=salary_adjusted`);
});

router.get("/workers/:id/print", (req, res) => {
  const worker = db.prepare(
    "SELECT id, full_name, username, phone, fingerprint_id, start_date, monthly_salary, is_active, deactivated_at FROM users WHERE id = ? AND role = 'WORKER'"
  ).get(req.params.id);
  if (!worker) return res.redirect("/admin/workers");
  const document = db.prepare("SELECT * FROM worker_documents WHERE worker_id = ?").get(req.params.id);

  const payments = db.prepare(
    `SELECT worker_salary_payments.*, users.full_name as recorded_by
     FROM worker_salary_payments
     LEFT JOIN users ON worker_salary_payments.created_by = users.id
     WHERE worker_salary_payments.worker_id = ?
     ORDER BY worker_salary_payments.payment_date DESC, worker_salary_payments.id DESC`
  ).all(req.params.id);
  const totals = payments.reduce(
    (acc, row) => {
      const amt = Number(row.amount || 0);
      acc.total += amt;
      if (row.payment_type === "SALARY") acc.salary += amt;
      if (row.payment_type === "ADVANCE") acc.advance += amt;
      return acc;
    },
    { total: 0, salary: 0, advance: 0 }
  );
  const dueSalary = computeSalaryDue("WORKER", worker, totals.total, dayjs().format("YYYY-MM-DD"));
  const attendanceSummary = db.prepare(
    `SELECT
      COALESCE(SUM(CASE WHEN status = 'PRESENT' THEN 1 ELSE 0 END), 0) AS present_days,
      COALESCE(SUM(CASE WHEN status = 'ABSENT' THEN 1 ELSE 0 END), 0) AS absent_days,
      COUNT(*) AS marked_days
     FROM user_attendance
     WHERE user_id = ?`
  ).get(req.params.id);
  const absentDates = db.prepare(
    `SELECT attendance_date
     FROM user_attendance
     WHERE user_id = ?
       AND status = 'ABSENT'
     ORDER BY attendance_date DESC`
  ).all(req.params.id).map((row) => row.attendance_date);

  res.render("admin/worker_detail_print", {
    title: req.t("workerDetailPrintTitle"),
    worker,
    document,
    payments,
    totals,
    dueSalary,
    attendanceSummary,
    absentDates
  });
});

router.get("/workers/:id/edit", (req, res) => {
  const user = db.prepare(
    "SELECT id, full_name, username, phone, fingerprint_id, start_date, monthly_salary, is_active, deactivated_at FROM users WHERE id = ? AND role = 'WORKER'"
  ).get(req.params.id);
  if (!user) return res.redirect("/admin/workers");
  const document = db.prepare("SELECT * FROM worker_documents WHERE worker_id = ?").get(req.params.id);
  res.render("admin/worker_form", { title: req.t("editWorkerTitle"), user, document, error: null });
});

router.post("/workers/:id", (req, res) => {
  const upload = staffUpload.fields(workerDocumentFields);
  upload(req, res, (err) => {
    const { full_name, username, phone, fingerprint_id, password, start_date, monthly_salary, doc_type } = req.body;
    const user = db.prepare(
      "SELECT id FROM users WHERE id = ? AND role = 'WORKER'"
    ).get(req.params.id);
    if (!user) return res.redirect("/admin/workers");
    const existingDocument = db.prepare("SELECT * FROM worker_documents WHERE worker_id = ?").get(req.params.id);

    const formUser = { id: req.params.id, full_name, username, phone, fingerprint_id, start_date, monthly_salary };
    const formDocument = existingDocument
      ? { ...existingDocument, doc_type: normalizeDocumentType(doc_type) || existingDocument.doc_type }
      : { doc_type: normalizeDocumentType(doc_type) };

    if (err) {
      return res.render("admin/worker_form", {
        title: req.t("editWorkerTitle"),
        user: formUser,
        document: formDocument,
        error: err.message || req.t("uploadError")
      });
    }
    if (!full_name || !username) {
      return res.render("admin/worker_form", {
        title: req.t("editWorkerTitle"),
        user: formUser,
        document: formDocument,
        error: req.t("nameUsernameRequired")
      });
    }
    const salaryRaw = Number(monthly_salary || 0);
    if (Number.isNaN(salaryRaw) || salaryRaw < 0) {
      return res.render("admin/worker_form", {
        title: req.t("editWorkerTitle"),
        user: formUser,
        document: formDocument,
        error: req.t("salaryInvalid")
      });
    }
    const salary = parseWholeMoney(salaryRaw);
    const fingerprintId = normalizeFingerprintId(fingerprint_id);
    const fpConflict = findFingerprintConflict({ fingerprintId, skipWorkerId: req.params.id });
    if (fpConflict) {
      return res.render("admin/worker_form", {
        title: req.t("editWorkerTitle"),
        user: formUser,
        document: formDocument,
        error: req.t("fingerprintIdAlreadyUsed")
      });
    }

    try {
      const currentSalary = parseWholeMoney(
        db.prepare("SELECT monthly_salary FROM users WHERE id = ? AND role = 'WORKER'").get(req.params.id)?.monthly_salary || 0
      );
      runInTransaction(() => {
        if (currentSalary !== salary) {
          recordSalaryAdjustment(db, {
            personType: "WORKER",
            personId: req.params.id,
            previousSalary: currentSalary,
            newSalary: salary,
            effectiveMonth: dayjs().startOf("month").format("YYYY-MM-DD"),
            note: "Updated from worker profile",
            createdBy: req.session.userId
          });
        }
        db.prepare(
          "UPDATE users SET full_name = ?, username = ?, phone = ?, fingerprint_id = ?, start_date = ?, monthly_salary = ?, updated_at = datetime('now') WHERE id = ?"
        ).run(
          full_name.trim(),
          username.trim(),
          phone ? phone.trim() : null,
          fingerprintId,
          start_date || null,
          salary,
          req.params.id
        );
      });

      if (password) {
        const hash = bcrypt.hashSync(password, 10);
        db.prepare("UPDATE users SET password_hash = ? WHERE id = ?").run(hash, req.params.id);
      }

      upsertWorkerDocument({
        workerId: req.params.id,
        existingDocument,
        files: req.files,
        docTypeValue: doc_type
      });

      logActivity({
        userId: req.session.userId,
        action: "update",
        entityType: "worker",
        entityId: req.params.id,
        details: `username=${username.trim()}`
      });

      res.redirect(`/admin/workers/${req.params.id}`);
    } catch (updateErr) {
      res.render("admin/worker_form", {
        title: req.t("editWorkerTitle"),
        user: formUser,
        document: formDocument,
        error: req.t("usernameExists")
      });
    }
  });
});

router.post("/workers/:id/delete", (req, res) => {
  const worker = db.prepare("SELECT id, is_active FROM users WHERE id = ? AND role = 'WORKER'").get(req.params.id);
  if (!worker) return res.redirect("/admin/workers");
  db.prepare(
    "UPDATE users SET is_active = 0, deactivated_at = datetime('now'), deactivated_by = ?, updated_at = datetime('now') WHERE id = ?"
  ).run(req.session.userId || null, req.params.id);
  logActivity({
    userId: req.session.userId,
    action: "update",
    entityType: "worker",
    entityId: req.params.id,
    details: "status=archived"
  });
  res.redirect("/admin/workers?archived=1&include_inactive=1");
});

router.post("/workers/:id/activate", (req, res) => {
  const worker = db.prepare("SELECT id FROM users WHERE id = ? AND role = 'WORKER'").get(req.params.id);
  if (!worker) return res.redirect("/admin/workers");
  db.prepare(
    "UPDATE users SET is_active = 1, deactivated_at = NULL, deactivated_by = NULL, updated_at = datetime('now') WHERE id = ?"
  ).run(req.params.id);
  logActivity({
    userId: req.session.userId,
    action: "update",
    entityType: "worker",
    entityId: req.params.id,
    details: "status=active"
  });
  res.redirect("/admin/workers?activated=1&include_inactive=1");
});

router.post("/workers/:id/payments", (req, res) => {
  const worker = db.prepare("SELECT id FROM users WHERE id = ? AND role = 'WORKER'").get(req.params.id);
  if (!worker) return res.redirect("/admin/workers");
  const { payment_date, amount, payment_type, payment_source, note, print } = req.body;
  const amt = Number(amount || 0);
  if (!payment_date || Number.isNaN(amt) || amt <= 0) {
    return res.redirect(`/admin/workers/${req.params.id}`);
  }
  const type = payment_type === "ADVANCE" ? "ADVANCE" : "SALARY";
  const source = normalizeSalaryPaymentSource(payment_source);
  if (source === "DAILY_COLLECTION") {
    const collectionBalance = getDailyCollectionBalance(db, payment_date);
    if (amt > roundMoney(collectionBalance) + 0.01) {
      return res.redirect(`/admin/workers/${req.params.id}?error=dailyCollectionInsufficient`);
    }
  }
  if (type === "SALARY") {
    const workerRow = db.prepare("SELECT * FROM users WHERE id = ? AND role = 'WORKER'").get(req.params.id);
    if (!workerRow) return res.redirect("/admin/workers");
    const paidTotal = getWorkerPaidTotal(req.params.id);
    const due = computeSalaryDue("WORKER", workerRow, paidTotal, payment_date || dayjs().format("YYYY-MM-DD"));
    if (amt > roundMoney(due) + 0.01) {
      return res.redirect(`/admin/workers/${req.params.id}?error=salaryOverpaymentBlocked`);
    }
  }

  const paymentResult = db.prepare(
    "INSERT INTO worker_salary_payments (worker_id, payment_date, amount, payment_type, payment_source, note, created_by) VALUES (?, ?, ?, ?, ?, ?, ?)"
  ).run(req.params.id, payment_date, amt, type, source, note || null, req.session.userId);
  const paymentId = Number(paymentResult.lastInsertRowid);
  const receiptNo = createReceiptNo(db, "WRK", payment_date || dayjs().format("YYYY-MM-DD"));
  db.prepare("UPDATE worker_salary_payments SET receipt_no = ? WHERE id = ?").run(receiptNo, paymentId);

  logActivity({
    userId: req.session.userId,
    action: "payment",
    entityType: "worker_salary",
    entityId: paymentId,
    details: `receipt=${receiptNo}, type=${type}, source=${source}, amount=${amt}`
  });

  if (print) {
    return res.redirect(`/admin/workers/payments/${paymentId}/print`);
  }
  res.redirect(`/admin/workers/${req.params.id}`);
});

router.get("/workers/payments/:id/edit", (req, res) => {
  const payment = db.prepare(
    `SELECT worker_salary_payments.*, users.full_name
     FROM worker_salary_payments
     JOIN users ON worker_salary_payments.worker_id = users.id
     WHERE worker_salary_payments.id = ?`
  ).get(req.params.id);
  if (!payment) return res.redirect("/admin/workers");
  const errorKey = String(req.query.error || "").trim();
  const error = errorKey ? req.t(errorKey) : null;
  res.render("admin/staff_payment_form", {
    title: req.t("editSalaryPaymentTitle"),
    payment,
    staff: { id: payment.worker_id, full_name: payment.full_name },
    error,
    basePath: "/admin/workers"
  });
});

router.post("/workers/payments/:id", (req, res) => {
  const payment = db.prepare("SELECT * FROM worker_salary_payments WHERE id = ?").get(req.params.id);
  if (!payment) return res.redirect("/admin/workers");
  const worker = db.prepare("SELECT * FROM users WHERE id = ? AND role = 'WORKER'").get(payment.worker_id);
  if (!worker) return res.redirect("/admin/workers");
  const { payment_date, amount, payment_type, payment_source, note, print } = req.body;
  const amt = Number(amount || 0);
  if (!payment_date || Number.isNaN(amt) || amt <= 0) {
    return res.redirect(`/admin/workers/payments/${req.params.id}/edit`);
  }
  const type = payment_type === "ADVANCE" ? "ADVANCE" : "SALARY";
  const source = normalizeSalaryPaymentSource(payment_source);
  if (source === "DAILY_COLLECTION") {
    const collectionBalance = getDailyCollectionBalance(db, payment_date, { excludeWorkerPaymentId: req.params.id });
    if (amt > roundMoney(collectionBalance) + 0.01) {
      return res.redirect(`/admin/workers/payments/${req.params.id}/edit?error=dailyCollectionInsufficient`);
    }
  }
  if (type === "SALARY") {
    const paidTotal = getWorkerPaidTotal(payment.worker_id, { excludePaymentId: req.params.id });
    const due = computeSalaryDue("WORKER", worker, paidTotal, payment_date || dayjs().format("YYYY-MM-DD"));
    if (amt > roundMoney(due) + 0.01) {
      return res.redirect(`/admin/workers/payments/${req.params.id}/edit?error=salaryOverpaymentBlocked`);
    }
  }

  db.prepare(
    "UPDATE worker_salary_payments SET payment_date = ?, amount = ?, payment_type = ?, payment_source = ?, note = ? WHERE id = ?"
  ).run(payment_date, amt, type, source, note || null, req.params.id);

  logActivity({
    userId: req.session.userId,
    action: "update",
    entityType: "worker_salary",
    entityId: req.params.id,
    details: buildDiffDetails(
      payment,
      {
        payment_date,
        amount: amt,
        payment_type: type,
        payment_source: source,
        note: note || null
      },
      ["payment_date", "amount", "payment_type", "payment_source", "note"]
    )
  });

  if (print) {
    return res.redirect(`/admin/workers/payments/${req.params.id}/print`);
  }
  res.redirect(`/admin/workers/${payment.worker_id}`);
});

router.post("/workers/payments/:id/delete", (req, res) => {
  const payment = db.prepare("SELECT * FROM worker_salary_payments WHERE id = ?").get(req.params.id);
  if (!payment) return res.redirect("/admin/workers");
  const recycleId = createRecycleEntry({
    entityType: "worker_salary_payment",
    entityId: req.params.id,
    payload: { worker_salary_payment: payment },
    deletedBy: req.session.userId,
    note: `worker_id=${payment.worker_id}; receipt=${payment.receipt_no || ""}`
  });
  logActivity({
    userId: req.session.userId,
    action: "delete",
    entityType: "worker_salary",
    entityId: req.params.id,
    details: `recycle_id=${recycleId}`
  });
  db.prepare("DELETE FROM worker_salary_payments WHERE id = ?").run(req.params.id);
  res.redirect(`/admin/workers/${payment.worker_id}`);
});

router.get("/workers/payments/:id/print", (req, res) => {
  const payment = db.prepare(
    `SELECT worker_salary_payments.*, users.full_name, users.phone, worker_documents.photo_path
     FROM worker_salary_payments
     JOIN users ON worker_salary_payments.worker_id = users.id
     LEFT JOIN worker_documents ON worker_documents.worker_id = users.id
     WHERE worker_salary_payments.id = ?`
  ).get(req.params.id);
  if (!payment) return res.redirect("/admin/workers");
  res.render("admin/staff_payment_print", { title: req.t("workerPaySlipTitle"), payment });
});

router.get("/admins", (req, res) => {
  const currentUser = res.locals.currentUser;
  if (currentUser.role !== "SUPER_ADMIN") {
    return res.status(403).render("unauthorized", { title: req.t("notAllowedTitle") });
  }

  const admins = db.prepare(
    "SELECT id, full_name, username, phone, role, is_active, deactivated_at FROM users WHERE role IN ('ADMIN','SUPER_ADMIN') ORDER BY created_at DESC"
  ).all();
  const success = req.query.deactivated ? req.t("adminDeactivated") : req.query.activated ? req.t("adminActivated") : null;
  res.render("admin/admins", { title: req.t("adminsTitle"), admins, currentUser, success });
});

router.get("/admins/new", (req, res) => {
  const currentUser = res.locals.currentUser;
  if (currentUser.role !== "SUPER_ADMIN") {
    return res.status(403).render("unauthorized", { title: req.t("notAllowedTitle") });
  }
  res.render("admin/admin_form", { title: req.t("addAdminTitle"), user: null, error: null });
});

router.post("/admins", (req, res) => {
  const currentUser = res.locals.currentUser;
  if (currentUser.role !== "SUPER_ADMIN") {
    return res.status(403).render("unauthorized", { title: req.t("notAllowedTitle") });
  }

  const { full_name, username, phone, password, role } = req.body;
  if (!full_name || !username || !password) {
    return res.render("admin/admin_form", { title: req.t("addAdminTitle"), user: null, error: req.t("adminRequired") });
  }

  const hash = bcrypt.hashSync(password, 10);
  const chosenRole = role === "SUPER_ADMIN" ? "SUPER_ADMIN" : "ADMIN";

  try {
    db.prepare("INSERT INTO users (full_name, username, phone, password_hash, role) VALUES (?, ?, ?, ?, ?)")
      .run(full_name.trim(), username.trim(), phone ? phone.trim() : null, hash, chosenRole);
    logActivity({
      userId: req.session.userId,
      action: "create",
      entityType: "admin",
      entityId: username.trim(),
      details: `role=${chosenRole}`
    });
    res.redirect("/admin/admins");
  } catch (err) {
    res.render("admin/admin_form", { title: req.t("addAdminTitle"), user: null, error: req.t("usernameExists") });
  }
});

router.post("/admins/:id/role", (req, res) => {
  const currentUser = res.locals.currentUser;
  if (currentUser.role !== "SUPER_ADMIN") {
    return res.status(403).render("unauthorized", { title: req.t("notAllowedTitle") });
  }

  const admin = db.prepare("SELECT id, role FROM users WHERE id = ? AND role IN ('ADMIN','SUPER_ADMIN')").get(req.params.id);
  if (!admin) return res.redirect("/admin/admins");

  const newRole = admin.role === "SUPER_ADMIN" ? "ADMIN" : "SUPER_ADMIN";

  if (admin.id === currentUser.id && newRole !== "SUPER_ADMIN") {
    return res.redirect("/admin/admins");
  }

  db.prepare("UPDATE users SET role = ? WHERE id = ?").run(newRole, admin.id);
  logActivity({
    userId: req.session.userId,
    action: "update",
    entityType: "admin",
    entityId: admin.id,
    details: `role=${newRole}`
  });
  res.redirect("/admin/admins");
});

router.post("/admins/:id/delete", (req, res) => {
  const currentUser = res.locals.currentUser;
  if (currentUser.role !== "SUPER_ADMIN") {
    return res.status(403).render("unauthorized", { title: req.t("notAllowedTitle") });
  }

  if (String(currentUser.id) === String(req.params.id)) {
    return res.redirect("/admin/admins");
  }

  const activeSupers = db.prepare(
    "SELECT COUNT(*) as count FROM users WHERE role = 'SUPER_ADMIN' AND is_active = 1"
  ).get().count;
  const target = db.prepare("SELECT role, is_active FROM users WHERE id = ?").get(req.params.id);
  if (target && target.role === "SUPER_ADMIN" && Number(target.is_active) === 1 && activeSupers <= 1) {
    return res.redirect("/admin/admins");
  }

  db.prepare(
    "UPDATE users SET is_active = 0, deactivated_at = datetime('now'), deactivated_by = ?, updated_at = datetime('now') WHERE id = ? AND role IN ('ADMIN','SUPER_ADMIN')"
  ).run(req.session.userId || null, req.params.id);
  logActivity({
    userId: req.session.userId,
    action: "update",
    entityType: "admin",
    entityId: req.params.id,
    details: "status=deactivated"
  });
  res.redirect("/admin/admins?deactivated=1");
});

router.post("/admins/:id/activate", (req, res) => {
  const currentUser = res.locals.currentUser;
  if (currentUser.role !== "SUPER_ADMIN") {
    return res.status(403).render("unauthorized", { title: req.t("notAllowedTitle") });
  }
  const target = db.prepare("SELECT id FROM users WHERE id = ? AND role IN ('ADMIN','SUPER_ADMIN')").get(req.params.id);
  if (!target) return res.redirect("/admin/admins");
  db.prepare(
    "UPDATE users SET is_active = 1, deactivated_at = NULL, deactivated_by = NULL, updated_at = datetime('now') WHERE id = ?"
  ).run(req.params.id);
  logActivity({
    userId: req.session.userId,
    action: "update",
    entityType: "admin",
    entityId: req.params.id,
    details: "status=active"
  });
  res.redirect("/admin/admins?activated=1");
});

router.get("/settings", (req, res) => {
  return renderSettingsPage(req, res);
});

router.post("/settings", (req, res) => {
  const currentUser = res.locals.currentUser;
  const autoBackupEnabled = req.body.auto_backup_enabled === "on";
  const autoBackupHour = Number(req.body.auto_backup_hour || 18);
  const autoBackupKeep = Number(req.body.auto_backup_keep || 30);
  const retentionEnabled = req.body.retention_enabled === "on";
  const retentionDays = Number(req.body.retention_days || 365);
  const retentionBatchSize = Number(req.body.retention_batch_size || 500);
  const numberingFiscalStartMonth = Number(req.body.numbering_fiscal_start_month || 7);
  const numberingSequencePad = Number(req.body.numbering_sequence_pad || 5);
  const jarLowThreshold = Number(req.body.alert_low_stock_jars || 20);
  const itemLowThreshold = Number(req.body.alert_low_stock_items || 10);
  const overdueCreditDays = Number(req.body.alert_overdue_credit_days || 7);
  const vehicleComplianceDays = Number(req.body.alert_vehicle_compliance_days || 30);
  const hybridSyncEnabled = req.body.hybrid_sync_enabled === "on";
  const hybridSyncPostgresUrl = String(req.body.hybrid_sync_pg_url || "").trim();
  const hybridSyncSiteId = normalizeSiteId(req.body.hybrid_sync_site_id || "aqua-msk-main");
  const hybridSyncIntervalMin = Number(req.body.hybrid_sync_interval_min || 15);
  const hybridSyncSslEnabled = req.body.hybrid_sync_ssl_enabled === "on";
  const iotAttendanceEnabled = req.body.iot_attendance_enabled === "on";
  const iotAttendanceToken = String(req.body.iot_attendance_token || "").trim();
  if (Number.isNaN(autoBackupHour) || autoBackupHour < 0 || autoBackupHour > 23) {
    return renderSettingsPage(req, res, {
      error: req.t("backupHourInvalid")
    });
  }
  if (Number.isNaN(autoBackupKeep) || autoBackupKeep < 3 || autoBackupKeep > 180) {
    return renderSettingsPage(req, res, {
      error: req.t("backupKeepInvalid")
    });
  }
  if (Number.isNaN(retentionDays) || retentionDays < 30 || retentionDays > 3650) {
    return renderSettingsPage(req, res, {
      error: req.t("retentionDaysInvalid")
    });
  }
  if (Number.isNaN(retentionBatchSize) || retentionBatchSize < 50 || retentionBatchSize > 5000) {
    return renderSettingsPage(req, res, {
      error: req.t("retentionBatchInvalid")
    });
  }
  if (Number.isNaN(numberingFiscalStartMonth) || numberingFiscalStartMonth < 1 || numberingFiscalStartMonth > 12) {
    return renderSettingsPage(req, res, {
      error: req.t("numberingFiscalMonthInvalid")
    });
  }
  if (Number.isNaN(numberingSequencePad) || numberingSequencePad < 3 || numberingSequencePad > 8) {
    return renderSettingsPage(req, res, {
      error: req.t("numberingSequencePadInvalid")
    });
  }
  if (Number.isNaN(jarLowThreshold) || jarLowThreshold < 0 || jarLowThreshold > 1000000) {
    return renderSettingsPage(req, res, {
      error: req.t("alertThresholdInvalid")
    });
  }
  if (Number.isNaN(itemLowThreshold) || itemLowThreshold < 0 || itemLowThreshold > 1000000) {
    return renderSettingsPage(req, res, {
      error: req.t("alertThresholdInvalid")
    });
  }
  if (Number.isNaN(overdueCreditDays) || overdueCreditDays < 1 || overdueCreditDays > 365) {
    return renderSettingsPage(req, res, {
      error: req.t("overdueDaysInvalid")
    });
  }
  if (Number.isNaN(vehicleComplianceDays) || vehicleComplianceDays < 1 || vehicleComplianceDays > 365) {
    return renderSettingsPage(req, res, {
      error: req.t("vehicleComplianceDaysInvalid")
    });
  }
  if (hybridSyncEnabled && !hybridSyncPostgresUrl) {
    return renderSettingsPage(req, res, {
      error: req.t("hybridSyncUrlRequired")
    });
  }
  if (hybridSyncEnabled && !hybridSyncSiteId) {
    return renderSettingsPage(req, res, {
      error: req.t("hybridSyncSiteIdRequired")
    });
  }
  if (Number.isNaN(hybridSyncIntervalMin) || hybridSyncIntervalMin < 5 || hybridSyncIntervalMin > 720) {
    return renderSettingsPage(req, res, {
      error: req.t("hybridSyncIntervalInvalid")
    });
  }
  if (iotAttendanceEnabled && iotAttendanceToken.length < 8) {
    return renderSettingsPage(req, res, {
      error: req.t("iotTokenInvalid")
    });
  }
  const importItemTypes = db.prepare(
    "SELECT code FROM import_item_types WHERE is_active = 1"
  ).all();
  const perItemThresholdValues = [];
  for (const row of importItemTypes) {
    const code = String(row.code || "").trim();
    if (!code) continue;
    const raw = String(req.body[`${alertItemThresholdPrefix}${code}`] || "").trim();
    if (!raw) {
      perItemThresholdValues.push({ code, value: "" });
      continue;
    }
    const num = Number(raw);
    if (Number.isNaN(num) || num < 0 || num > 1000000) {
      return renderSettingsPage(req, res, {
        error: req.t("alertThresholdInvalid")
      });
    }
    perItemThresholdValues.push({ code, value: String(Math.floor(num)) });
  }
  let permissionSummary = "";
  if (currentUser && currentUser.role === "SUPER_ADMIN") {
    ensureRolePermissionRows(db);
    const updateRolePermission = db.prepare(
      "UPDATE role_permissions SET can_view = ?, can_edit = ?, updated_at = datetime('now') WHERE role = ? AND module_key = ?"
    );
    const permissionParts = [];
    ROLE_PERMISSION_ROLES.forEach((role) => {
      MODULE_PERMISSION_ITEMS.forEach((item) => {
        const viewKey = `perm_${role}_${item.key}_view`;
        const editKey = `perm_${role}_${item.key}_edit`;
        const canView = req.body[viewKey] === "on";
        const canEdit = canView && req.body[editKey] === "on";
        updateRolePermission.run(canView ? 1 : 0, canEdit ? 1 : 0, role, item.key);
        permissionParts.push(`${role}.${item.key}=v${canView ? 1 : 0}e${canEdit ? 1 : 0}`);
      });
    });
    permissionSummary = permissionParts.join(", ");
  }
  setSetting("auto_backup_enabled", autoBackupEnabled ? 1 : 0);
  setSetting("auto_backup_hour", Math.floor(autoBackupHour));
  setSetting("auto_backup_keep", Math.floor(autoBackupKeep));
  setSetting("retention_enabled", retentionEnabled ? 1 : 0);
  setSetting("retention_days", Math.floor(retentionDays));
  setSetting("retention_batch_size", Math.floor(retentionBatchSize));
  setSetting("numbering_fiscal_start_month", Math.floor(numberingFiscalStartMonth));
  setSetting("numbering_sequence_pad", Math.floor(numberingSequencePad));
  setSetting("alert_low_stock_jars", Math.floor(jarLowThreshold));
  setSetting("alert_low_stock_items", Math.floor(itemLowThreshold));
  setSetting("alert_overdue_credit_days", Math.floor(overdueCreditDays));
  setSetting("alert_vehicle_compliance_days", Math.floor(vehicleComplianceDays));
  perItemThresholdValues.forEach((row) => {
    setSetting(`${alertItemThresholdPrefix}${row.code}`, row.value);
  });
  setSetting("hybrid_sync_enabled", hybridSyncEnabled ? 1 : 0);
  setSetting("hybrid_sync_pg_url", hybridSyncPostgresUrl);
  setSetting("hybrid_sync_site_id", hybridSyncSiteId);
  setSetting("hybrid_sync_interval_min", Math.floor(hybridSyncIntervalMin));
  setSetting("hybrid_sync_ssl_enabled", hybridSyncSslEnabled ? 1 : 0);
  setSetting("iot_attendance_enabled", iotAttendanceEnabled ? 1 : 0);
  setSetting("iot_attendance_token", iotAttendanceToken);
  pruneOldBackups(Math.floor(autoBackupKeep));
  logActivity({
    userId: req.session.userId,
    action: "update",
    entityType: "settings",
    entityId: "backup",
    details: `auto_backup=${autoBackupEnabled ? 1 : 0}, backup_hour=${Math.floor(autoBackupHour)}, keep=${Math.floor(autoBackupKeep)}, retention=${retentionEnabled ? 1 : 0}, retention_days=${Math.floor(retentionDays)}, retention_batch=${Math.floor(retentionBatchSize)}, fiscal_start_month=${Math.floor(numberingFiscalStartMonth)}, sequence_pad=${Math.floor(numberingSequencePad)}, jar_alert=${Math.floor(jarLowThreshold)}, item_alert=${Math.floor(itemLowThreshold)}, overdue_days=${Math.floor(overdueCreditDays)}, vehicle_compliance_days=${Math.floor(vehicleComplianceDays)}, hybrid_sync=${hybridSyncEnabled ? 1 : 0}, hybrid_interval=${Math.floor(hybridSyncIntervalMin)}, iot_attendance=${iotAttendanceEnabled ? 1 : 0}${permissionSummary ? `, permissions=${permissionSummary}` : ""}`
  });

  return renderSettingsPage(req, res, {
    success: req.t("settingsSaved")
  });
});

router.post("/retention/run", (req, res) => {
  try {
    const result = runRetentionArchive(db, { force: true });
    logActivity({
      userId: req.session.userId,
      action: "archive",
      entityType: "retention",
      entityId: "manual_run",
      details: `archived=${result.archivedCount || 0}, cutoff=${result.cutoffDateText || ""}`
    });
    return renderSettingsPage(req, res, {
      success: req.t("retentionRunSuccess", { count: result.archivedCount || 0 })
    });
  } catch (err) {
    return renderSettingsPage(req, res, {
      error: req.t("retentionRunFailed", { message: err.message || "archive_failed" })
    });
  }
});

router.post("/hybrid-sync/run", async (req, res) => {
  try {
    const result = await syncLocalToPostgres({
      db,
      reason: "manual"
    });
    logActivity({
      userId: req.session.userId,
      action: "sync",
      entityType: "hybrid",
      entityId: result.siteId,
      details: `rows=${result.syncedRows}, duration_ms=${result.durationMs}`
    });
    return renderSettingsPage(req, res, {
      success: req.t("hybridSyncRunSuccess", { rows: result.syncedRows })
    });
  } catch (err) {
    return renderSettingsPage(req, res, {
      error: req.t("hybridSyncRunFailed", { message: err.message || "sync_failed" })
    });
  }
});

router.post("/logo", logoUpload.single("logo_file"), (req, res) => {
  const currentUser = res.locals.currentUser;
  if (!currentUser || currentUser.role !== "SUPER_ADMIN") {
    return res.status(403).render("unauthorized", { title: req.t("notAllowedTitle") });
  }
  if (!req.file) {
    return renderSettingsPage(req, res, { error: req.t("logoFileRequired") });
  }
  const newLogoPath = `/uploads/${req.file.filename}`;
  setSetting("logo_path", newLogoPath);
  logActivity({
    userId: req.session.userId,
    action: "update",
    entityType: "branding",
    entityId: "logo",
    details: "logo_uploaded"
  });
  return renderSettingsPage(req, res, { logoPath: newLogoPath, success: req.t("logoUploaded") });
});

router.post("/branding", logoUpload.single("logo_file"), (req, res) => {
  const currentUser = res.locals.currentUser;
  if (!currentUser || currentUser.role !== "SUPER_ADMIN") {
    return res.status(403).render("unauthorized", { title: req.t("notAllowedTitle") });
  }
  const brandName = String(req.body.brand_name || "").trim();
  const brandTaglineText = String(req.body.brand_tagline || "").trim();
  if (!brandName) {
    return res.redirect("/admin/settings?branding_error=brandNameRequired#branding-settings");
  }

  setSetting("brand_name", brandName);
  setSetting("brand_tagline", brandTaglineText);
  if (req.file) {
    setSetting("logo_path", `/uploads/${req.file.filename}`);
  }

  logActivity({
    userId: req.session.userId,
    action: "update",
    entityType: "branding",
    entityId: "identity",
    details: `brand_name=${brandName}; tagline=${brandTaglineText || "-"}; logo_updated=${req.file ? 1 : 0}`
  });
  return res.redirect("/admin/settings?branding_saved=1#branding-settings");
});

router.post("/logo/delete", (req, res) => {
  const currentUser = res.locals.currentUser;
  if (!currentUser || currentUser.role !== "SUPER_ADMIN") {
    return res.status(403).render("unauthorized", { title: req.t("notAllowedTitle") });
  }
  setSetting("logo_path", "");
  logActivity({
    userId: req.session.userId,
    action: "delete",
    entityType: "branding",
    entityId: "logo"
  });
  return res.redirect("/admin/settings?branding_saved=1#branding-settings");
});

router.post("/brand-image", wordmarkUpload.single("brand_file"), (req, res) => {
  const currentUser = res.locals.currentUser;
  if (!currentUser || currentUser.role !== "SUPER_ADMIN") {
    return res.status(403).render("unauthorized", { title: req.t("notAllowedTitle") });
  }
  if (!req.file) {
    return res.redirect("/admin");
  }
  const newPath = `/uploads/${req.file.filename}`;
  setSetting("brand_wordmark_path", newPath);
  logActivity({
    userId: req.session.userId,
    action: "update",
    entityType: "branding",
    entityId: "wordmark",
    details: "wordmark_uploaded"
  });
  return res.redirect(req.get("Referrer") || "/admin");
});

router.post("/brand-image/delete", (req, res) => {
  const currentUser = res.locals.currentUser;
  if (!currentUser || currentUser.role !== "SUPER_ADMIN") {
    return res.status(403).render("unauthorized", { title: req.t("notAllowedTitle") });
  }
  setSetting("brand_wordmark_path", "");
  logActivity({
    userId: req.session.userId,
    action: "delete",
    entityType: "branding",
    entityId: "wordmark"
  });
  return res.redirect(req.get("Referrer") || "/admin");
});

router.get("/profile", (req, res) => {
  const user = res.locals.currentUser;
  res.render("admin/profile", {
    title: req.t("profileTitle"),
    user,
    error: req.query.error ? req.t(String(req.query.error)) : null,
    success: req.query.theme_saved === "1" ? req.t("themeSaved") : null,
    preferenceAction: "/admin/profile/preferences"
  });
});

router.get("/worker-attendance", (req, res) => {
  const date = req.query.date || dayjs().format("YYYY-MM-DD");
  const workers = db.prepare(
    "SELECT id, full_name, phone, fingerprint_id FROM users WHERE role = 'WORKER' AND is_active = 1 ORDER BY full_name"
  ).all();
  const attendanceRows = db.prepare(
    "SELECT user_id, status FROM user_attendance WHERE attendance_date = ?"
  ).all(date);
  const attendanceMap = attendanceRows.reduce((acc, row) => {
    acc[row.user_id] = row.status;
    return acc;
  }, {});
  const saved = req.query.saved === "1" ? req.t("attendanceSaved") : null;
  const iotEnabled = String(getSetting("iot_attendance_enabled", "0")) === "1";
  const iotSaved = req.query.iot === "saved" ? req.t("attendanceSaved") : null;
  const iotErrorKey = String(req.query.error || "").trim();
  const iotError = iotErrorKey ? req.t(iotErrorKey) : null;
  const iotLogs = db.prepare(
    `SELECT iot_attendance_logs.*, users.full_name as recorded_by_name
     FROM iot_attendance_logs
     LEFT JOIN users ON iot_attendance_logs.recorded_by = users.id
     WHERE iot_attendance_logs.person_type = 'WORKER'
       AND iot_attendance_logs.attendance_date = ?
     ORDER BY iot_attendance_logs.scanned_at DESC, iot_attendance_logs.id DESC
     LIMIT 20`
  ).all(date);
  res.render("admin/worker_attendance", {
    title: req.t("workerAttendanceTitle"),
    date,
    workers,
    attendanceMap,
    saved,
    iotEnabled,
    iotSaved,
    iotError,
    iotLogs
  });
});

router.post("/worker-attendance", (req, res) => {
  const attendanceDate = req.body.attendance_date || dayjs().format("YYYY-MM-DD");
  const workers = db.prepare("SELECT id FROM users WHERE role = 'WORKER' AND is_active = 1").all();
  const userId = req.session.userId || null;
  const upsert = db.prepare(
    `INSERT INTO user_attendance (user_id, attendance_date, status, recorded_by, created_at, updated_at)
     VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))
     ON CONFLICT(user_id, attendance_date)
     DO UPDATE SET status = excluded.status, recorded_by = excluded.recorded_by, updated_at = datetime('now')`
  );
  try {
    db.exec("BEGIN");
    workers.forEach((worker) => {
      const status = req.body[`status_${worker.id}`];
      if (!status) return;
      const finalStatus = status === "PRESENT" ? "PRESENT" : "ABSENT";
      upsert.run(worker.id, attendanceDate, finalStatus, userId);
    });
    db.exec("COMMIT");
  } catch (err) {
    db.exec("ROLLBACK");
    throw err;
  }

  logActivity({
    userId,
    action: "update",
    entityType: "worker_attendance",
    entityId: attendanceDate,
    details: `date=${attendanceDate}`
  });

  res.redirect(`/admin/worker-attendance?date=${attendanceDate}&saved=1`);
});

router.post("/worker-attendance/iot-mark", (req, res) => {
  const attendanceDate = req.body.attendance_date || dayjs().format("YYYY-MM-DD");
  const iotEnabled = String(getSetting("iot_attendance_enabled", "0")) === "1";
  if (!iotEnabled) {
    return res.redirect(`/admin/worker-attendance?date=${attendanceDate}&error=iotAttendanceDisabled`);
  }
  const fingerprintId = normalizeFingerprintId(req.body.fingerprint_id);
  if (!fingerprintId) {
    return res.redirect(`/admin/worker-attendance?date=${attendanceDate}&error=fingerprintIdRequired`);
  }
  const status = req.body.status === "ABSENT" ? "ABSENT" : "PRESENT";
  const worker = db.prepare(
    "SELECT id, full_name FROM users WHERE role = 'WORKER' AND is_active = 1 AND lower(trim(fingerprint_id)) = lower(trim(?))"
  ).get(fingerprintId);
  if (!worker) {
    return res.redirect(`/admin/worker-attendance?date=${attendanceDate}&error=fingerprintIdNotMapped`);
  }
  const userId = req.session.userId || null;
  db.prepare(
    `INSERT INTO user_attendance (user_id, attendance_date, status, recorded_by, created_at, updated_at)
     VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))
     ON CONFLICT(user_id, attendance_date)
     DO UPDATE SET status = excluded.status, recorded_by = excluded.recorded_by, updated_at = datetime('now')`
  ).run(worker.id, attendanceDate, status, userId);
  db.prepare(
    `INSERT INTO iot_attendance_logs (source, person_type, person_id, fingerprint_id, status, attendance_date, scanned_at, note, recorded_by)
     VALUES ('MANUAL', 'WORKER', ?, ?, ?, ?, datetime('now'), ?, ?)`
  ).run(worker.id, fingerprintId, status, attendanceDate, "manual_attendance_form", userId);

  logActivity({
    userId,
    action: "update",
    entityType: "worker_attendance_iot",
    entityId: attendanceDate,
    details: `worker=${worker.id}, fingerprint=${fingerprintId}, status=${status}`
  });

  return res.redirect(`/admin/worker-attendance?date=${attendanceDate}&iot=saved`);
});

router.get("/worker-attendance/print", (req, res) => {
  const from = req.query.from || dayjs().format("YYYY-MM-DD");
  const to = req.query.to || from;
  const rows = db.prepare(
    `SELECT user_attendance.*, users.full_name, users.phone, recorder.full_name as recorded_by
     FROM user_attendance
     JOIN users ON user_attendance.user_id = users.id
     LEFT JOIN users as recorder ON user_attendance.recorded_by = recorder.id
     WHERE attendance_date BETWEEN ? AND ? AND users.role = 'WORKER'
     ORDER BY attendance_date DESC, users.full_name`
  ).all(from, to);
  const totals = rows.reduce(
    (acc, row) => {
      if (row.status === "PRESENT") acc.present += 1;
      else acc.absent += 1;
      return acc;
    },
    { present: 0, absent: 0 }
  );
  res.render("admin/worker_attendance_print", {
    title: req.t("workerAttendanceTitle"),
    from,
    to,
    rows,
    totals
  });
});

router.post("/profile", (req, res) => {
  const user = res.locals.currentUser;
  const { full_name, phone, current_password, new_password } = req.body;
  if (!full_name) {
    return res.render("admin/profile", {
      title: req.t("profileTitle"),
      user: { ...user, full_name, phone },
      error: req.t("fullNameRequired"),
      success: null,
      preferenceAction: "/admin/profile/preferences"
    });
  }

  db.prepare("UPDATE users SET full_name = ?, phone = ?, updated_at = datetime('now') WHERE id = ?")
    .run(full_name.trim(), phone ? phone.trim() : null, user.id);

  if (new_password) {
    const dbUser = db.prepare("SELECT password_hash FROM users WHERE id = ?").get(user.id);
    if (!current_password || !dbUser || !bcrypt.compareSync(current_password, dbUser.password_hash)) {
      const refreshed = getProfileUserById(user.id);
      return res.render("admin/profile", {
        title: req.t("profileTitle"),
        user: refreshed,
        error: req.t("currentPasswordInvalid"),
        success: null,
        preferenceAction: "/admin/profile/preferences"
      });
    }
    const hash = bcrypt.hashSync(new_password, 10);
    db.prepare("UPDATE users SET password_hash = ? WHERE id = ?").run(hash, user.id);
  }

  const refreshed = getProfileUserById(user.id);
  res.render("admin/profile", {
    title: req.t("profileTitle"),
    user: refreshed,
    error: null,
    success: req.t("profileSaved"),
    preferenceAction: "/admin/profile/preferences"
  });
});

router.post("/profile/preferences", (req, res) => {
  const user = res.locals.currentUser;
  if (!user) return res.redirect("/login");
  const themePreset = normalizeThemePreset(req.body.theme_preset);
  db.prepare("UPDATE users SET theme_preset = ?, updated_at = datetime('now') WHERE id = ?").run(themePreset, user.id);
  logActivity({
    userId: user.id,
    action: "update",
    entityType: "user_preferences",
    entityId: user.id,
    details: `theme=${themePreset}`
  });
  res.redirect("/admin/profile?theme_saved=1");
});

router.get("/recovery", (req, res) => {
  const currentUser = res.locals.currentUser;
  if (!currentUser || currentUser.role !== "SUPER_ADMIN") {
    return res.status(403).render("unauthorized", { title: req.t("notAllowedTitle") });
  }
  const recovery = db.prepare("SELECT * FROM account_recovery WHERE user_id = ?").get(currentUser.id);
  res.render("admin/recovery", {
    title: req.t("recoveryTitle"),
    recovery,
    newKey: null,
    success: null,
    error: null
  });
});

router.post("/recovery/key", (req, res) => {
  const currentUser = res.locals.currentUser;
  if (!currentUser || currentUser.role !== "SUPER_ADMIN") {
    return res.status(403).render("unauthorized", { title: req.t("notAllowedTitle") });
  }
  const newKey = generateRecoveryKey();
  const keyHash = bcrypt.hashSync(newKey, 10);
  const existing = db.prepare("SELECT user_id FROM account_recovery WHERE user_id = ?").get(currentUser.id);
  if (existing) {
    db.prepare(
      "UPDATE account_recovery SET key_hash = ?, key_created_at = datetime('now'), updated_at = datetime('now') WHERE user_id = ?"
    ).run(keyHash, currentUser.id);
  } else {
    db.prepare(
      "INSERT INTO account_recovery (user_id, key_hash, key_created_at) VALUES (?, ?, datetime('now'))"
    ).run(currentUser.id, keyHash);
  }
  logActivity({
    userId: req.session.userId,
    action: "update",
    entityType: "recovery",
    entityId: "key",
    details: "recovery_key_generated"
  });
  const recovery = db.prepare("SELECT * FROM account_recovery WHERE user_id = ?").get(currentUser.id);
  res.render("admin/recovery", {
    title: req.t("recoveryTitle"),
    recovery,
    newKey,
    success: req.t("recoveryKeyGenerated"),
    error: null
  });
});

router.post("/recovery/questions", (req, res) => {
  const currentUser = res.locals.currentUser;
  if (!currentUser || currentUser.role !== "SUPER_ADMIN") {
    return res.status(403).render("unauthorized", { title: req.t("notAllowedTitle") });
  }
  const { q1, a1, q2, a2, q3, a3 } = req.body;
  if (!q1 || !a1 || !q2 || !a2 || !q3 || !a3) {
    const recovery = db.prepare("SELECT * FROM account_recovery WHERE user_id = ?").get(currentUser.id);
    return res.render("admin/recovery", {
      title: req.t("recoveryTitle"),
      recovery,
      newKey: null,
      success: null,
      error: req.t("recoveryQuestionsRequired")
    });
  }
  const existing = db.prepare("SELECT user_id FROM account_recovery WHERE user_id = ?").get(currentUser.id);
  const payload = [
    q1.trim(),
    bcrypt.hashSync(normalizeAnswer(a1), 10),
    q2.trim(),
    bcrypt.hashSync(normalizeAnswer(a2), 10),
    q3.trim(),
    bcrypt.hashSync(normalizeAnswer(a3), 10),
    currentUser.id
  ];
  if (existing) {
    db.prepare(
      "UPDATE account_recovery SET q1 = ?, a1_hash = ?, q2 = ?, a2_hash = ?, q3 = ?, a3_hash = ?, updated_at = datetime('now') WHERE user_id = ?"
    ).run(...payload);
  } else {
    db.prepare(
      "INSERT INTO account_recovery (q1, a1_hash, q2, a2_hash, q3, a3_hash, user_id) VALUES (?, ?, ?, ?, ?, ?, ?)"
    ).run(...payload);
  }
  logActivity({
    userId: req.session.userId,
    action: "update",
    entityType: "recovery",
    entityId: "questions",
    details: "recovery_questions_saved"
  });
  const recovery = db.prepare("SELECT * FROM account_recovery WHERE user_id = ?").get(currentUser.id);
  res.render("admin/recovery", {
    title: req.t("recoveryTitle"),
    recovery,
    newKey: null,
    success: req.t("recoveryQuestionsSaved"),
    error: null
  });
});

router.get("/activity", (req, res) => {
  const from = req.query.from || dayjs().subtract(7, "day").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const userId = req.query.user_id || "all";
  const action = req.query.action || "all";
  const entity = req.query.entity || "all";
  const users = db.prepare("SELECT id, full_name, role FROM users ORDER BY full_name").all();

  const userClause = userId !== "all" ? "AND activity_logs.user_id = ?" : "";
  const actionClause = action !== "all" ? "AND activity_logs.action = ?" : "";
  const entityClause = entity !== "all" ? "AND activity_logs.entity_type = ?" : "";

  const params = [from, to];
  if (userId !== "all") params.push(userId);
  if (action !== "all") params.push(action);
  if (entity !== "all") params.push(entity);

  const rows = db.prepare(
    `SELECT activity_logs.*, users.full_name, users.role
     FROM activity_logs
     LEFT JOIN users ON activity_logs.user_id = users.id
     WHERE date(activity_logs.created_at) BETWEEN ? AND ?
     ${userClause}
     ${actionClause}
     ${entityClause}
     ORDER BY activity_logs.created_at DESC`
  ).all(...params);
  const activityRows = formatActivityRows(rows, req.t);

  res.render("admin/activity_logs", {
    title: req.t("activityLogTitle"),
    from,
    to,
    userId,
    action,
    entity,
    users,
    rows: activityRows
  });
});

router.get("/backup", (req, res) => {
  try {
    const backup = createBackupFile({ db, prefix: "aqua_msk_backup" });
    const keepCount = getBackupConfig().keepCount;
    pruneOldBackups(keepCount);
    setSetting("last_backup_at", new Date().toISOString());
    logActivity({
      userId: req.session.userId,
      action: "backup",
      entityType: "system",
      entityId: "backup",
      details: `file=${backup.filename}`
    });
    res.setHeader("Content-Disposition", `attachment; filename=\"${backup.filename}\"`);
    return res.download(backup.filePath);
  } catch (err) {
    return renderSettingsPage(req, res, { error: err.message || req.t("backupFailed") });
  }
});

router.post("/backup/test", (req, res) => {
  const latestBackup = getLatestBackup();
  if (!latestBackup) {
    return renderSettingsPage(req, res, { error: req.t("backupTestNoFile") });
  }
  const test = testBackupFile(latestBackup.filePath);
  if (!test.ok) {
    return renderSettingsPage(req, res, {
      error: `${req.t("backupTestFailed")}: ${test.error || "Unknown error"}`,
      backupTest: { ok: false, file: latestBackup.name }
    });
  }
  return renderSettingsPage(req, res, {
    success: req.t("backupTestPassed"),
    backupTest: {
      ok: true,
      file: latestBackup.name,
      tableCount: test.tableCount,
      usersCount: test.usersCount
    }
  });
});

router.post("/day-close/close", (req, res) => {
  const closureDate = normalizeIsoDate(req.body.closure_date) || dayjs().format("YYYY-MM-DD");
  const note = String(req.body.note || "").trim() || null;
  db.prepare(
    `INSERT INTO day_closures (closure_date, is_closed, note, closed_by, closed_at, reopened_by, reopened_at)
     VALUES (?, 1, ?, ?, datetime('now'), NULL, NULL)
     ON CONFLICT(closure_date)
     DO UPDATE SET is_closed = 1, note = excluded.note, closed_by = excluded.closed_by, closed_at = datetime('now'), reopened_by = NULL, reopened_at = NULL`
  ).run(closureDate, note, req.session.userId || null);
  logActivity({
    userId: req.session.userId,
    action: "update",
    entityType: "system",
    entityId: `day_close_${closureDate}`,
    details: `status=closed, date=${closureDate}`
  });
  return renderSettingsPage(req, res, {
    selectedClosureDate: closureDate,
    success: req.t("dayClosedSuccess")
  });
});

router.post("/day-close/reopen", (req, res) => {
  const closureDate = normalizeIsoDate(req.body.closure_date) || dayjs().format("YYYY-MM-DD");
  db.prepare(
    "UPDATE day_closures SET is_closed = 0, reopened_by = ?, reopened_at = datetime('now') WHERE closure_date = ?"
  ).run(req.session.userId || null, closureDate);
  logActivity({
    userId: req.session.userId,
    action: "update",
    entityType: "system",
    entityId: `day_close_${closureDate}`,
    details: `status=open, date=${closureDate}`
  });
  return renderSettingsPage(req, res, {
    selectedClosureDate: closureDate,
    success: req.t("dayReopenedSuccess")
  });
});

router.post("/day-close/bulk", (req, res) => {
  const fromDate = normalizeIsoDate(req.body.from_date);
  const toDate = normalizeIsoDate(req.body.to_date);
  const bulkAction = String(req.body.bulk_action || "close").trim().toLowerCase() === "reopen"
    ? "reopen"
    : "close";
  const note = String(req.body.note || "").trim() || null;

  if (!fromDate || !toDate) {
    return renderSettingsPage(req, res, {
      selectedClosureDate: req.body.to_date || req.body.from_date || dayjs().format("YYYY-MM-DD"),
      error: req.t("dayCloseRangeRequired")
    });
  }

  const range = listClosureDatesInRange(fromDate, toDate, 366);
  if (!range.ok) {
    const errorKey = range.reason === "TOO_LARGE" ? "dayCloseRangeTooLarge" : "dayCloseRangeInvalid";
    return renderSettingsPage(req, res, {
      selectedClosureDate: toDate,
      error: req.t(errorKey)
    });
  }

  let changedCount = 0;
  if (bulkAction === "close") {
    const closeStmt = db.prepare(
      `INSERT INTO day_closures (closure_date, is_closed, note, closed_by, closed_at, reopened_by, reopened_at)
       VALUES (?, 1, ?, ?, datetime('now'), NULL, NULL)
       ON CONFLICT(closure_date)
       DO UPDATE SET is_closed = 1, note = excluded.note, closed_by = excluded.closed_by, closed_at = datetime('now'), reopened_by = NULL, reopened_at = NULL`
    );
    range.dates.forEach((dateText) => {
      closeStmt.run(dateText, note, req.session.userId || null);
      changedCount += 1;
    });
  } else {
    const reopenStmt = db.prepare(
      "UPDATE day_closures SET is_closed = 0, reopened_by = ?, reopened_at = datetime('now') WHERE closure_date = ?"
    );
    range.dates.forEach((dateText) => {
      const result = reopenStmt.run(req.session.userId || null, dateText);
      changedCount += Number(result.changes || 0);
    });
  }

  logActivity({
    userId: req.session.userId,
    action: "update",
    entityType: "system",
    entityId: "day_close_bulk",
    details: `status=${bulkAction === "close" ? "closed" : "open"}, from=${fromDate}, to=${toDate}, count=${changedCount}`
  });

  const successKey = bulkAction === "close" ? "dayBulkClosedSuccess" : "dayBulkReopenedSuccess";
  return renderSettingsPage(req, res, {
    selectedClosureDate: toDate,
    success: req.t(successKey, { count: changedCount, from: fromDate, to: toDate })
  });
});

router.post("/restore", restoreUpload.single("backup_file"), (req, res) => {
  if (!req.file) {
    return renderSettingsPage(req, res, { error: req.t("backupFileRequired") });
  }
  logActivity({
    userId: req.session.userId,
    action: "restore",
    entityType: "system",
    entityId: "backup"
  });
  try {
    fs.copyFileSync(req.file.path, dbPath);
    fs.rmSync(`${dbPath}-wal`, { force: true });
    fs.rmSync(`${dbPath}-shm`, { force: true });
    setSetting("last_restore_at", new Date().toISOString());
    return renderSettingsPage(req, res, { success: req.t("restoreSuccess") });
  } catch (err) {
    return renderSettingsPage(req, res, { error: err.message || req.t("restoreFailed") });
  }
});

module.exports = router;
