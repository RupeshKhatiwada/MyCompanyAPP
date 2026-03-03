const express = require("express");
const dayjs = require("dayjs");
const { db } = require("../db");
const { requireRole } = require("../middleware/auth");
const path = require("path");
const multer = require("multer");
const bcrypt = require("bcryptjs");
const { createReceiptNo } = require("../utils/numbering");
const { createRecycleEntry } = require("../utils/recycleBin");
const { formatActivityRows } = require("../utils/activity");
const { adToBs } = require("../utils/calendar");

const router = express.Router();
const defaultStaffRoleCodes = ["CLEANER", "MACHINE_MANAGER", "VEHICLE_CONDUCTOR", "KITCHEN_COOK"];
const documentTypeOptions = ["CITIZENSHIP", "LICENSE", "PASSPORT", "PAN", "NATIONAL_ID", "VOTER_CARD", "OTHERS"];
const importLabelKeyByCode = {
  JAR_CONTAINER: "importItemJarContainer",
  JAR_CAP: "importItemJarCap",
  CHEMICAL_LABEL: "importItemChemicalLabel",
  LABEL_STICKER: "importItemLabelSticker",
  DATE_LABEL: "importItemDateLabel",
  BOTTLE_CASE: "importItemBottleCase",
  DISPENSER: "importItemDispenser"
};
const importUnitFallbackByCode = {
  JAR_CONTAINER: "",
  JAR_CAP: "unitBora",
  CHEMICAL_LABEL: "unitGallon",
  LABEL_STICKER: "unitBundle",
  DATE_LABEL: "unitRoll",
  BOTTLE_CASE: "unitCase",
  DISPENSER: "unitPiece"
};
const vehicleExpenseTypes = ["FUEL", "REPAIR", "SERVICE", "OTHER"];
const normalizeSalaryPaymentSource = (value) => {
  const safe = String(value || "").trim().toUpperCase();
  if (safe === "OWNER_PERSONAL") return "OWNER_PERSONAL";
  if (safe === "BANK_OTHER") return "BANK_OTHER";
  return "DAILY_COLLECTION";
};
const normalizePaymentMethod = (value) => {
  const safe = String(value || "").trim().toUpperCase();
  if (safe === "BANK") return "BANK";
  if (safe === "E_WALLET") return "E_WALLET";
  return "CASH";
};
const paymentLedgerChannels = ["BANK", "E_WALLET"];
const complianceDateFields = [
  "insurance_expiry",
  "tax_expiry",
  "permit_expiry",
  "fitness_expiry",
  "pollution_expiry"
];
const normalizeLedgerChannel = (value) => {
  const safe = String(value || "").trim().toUpperCase();
  return paymentLedgerChannels.includes(safe) ? safe : "BANK";
};
const normalizeDocumentType = (value) => {
  const safe = String(value || "").trim().toUpperCase();
  return documentTypeOptions.includes(safe) ? safe : null;
};
const getDateStatus = (dateText, todayText = dayjs().format("YYYY-MM-DD")) => {
  if (!dateText) return { code: "NOT_SET", daysLeft: null };
  const target = dayjs(dateText);
  if (!target.isValid()) return { code: "NOT_SET", daysLeft: null };
  const daysLeft = target.startOf("day").diff(dayjs(todayText).startOf("day"), "day");
  if (daysLeft < 0) return { code: "EXPIRED", daysLeft };
  if (daysLeft <= 15) return { code: "DUE_SOON", daysLeft };
  return { code: "VALID", daysLeft };
};
const complianceStatusPriority = { EXPIRED: 3, DUE_SOON: 2, NOT_SET: 1, VALID: 0 };
const uploadDir = path.join(__dirname, "..", "..", "public", "uploads");
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
const vehicleStorage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, uploadDir),
  filename: (req, file, cb) => {
    const ext = path.extname(file.originalname) || ".jpg";
    const base = `vehicle_${Date.now()}_${Math.random().toString(16).slice(2)}`;
    cb(null, `${base}${ext}`);
  }
});
const vehicleUpload = multer({
  storage: vehicleStorage,
  limits: { fileSize: 5 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    if (file.mimetype && file.mimetype.startsWith("image/")) return cb(null, true);
    cb(new Error("Invalid file type"));
  }
});
const waterReportStorage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, uploadDir),
  filename: (req, file, cb) => {
    const ext = path.extname(file.originalname) || "";
    const base = `water_test_${Date.now()}_${Math.random().toString(16).slice(2)}`;
    cb(null, `${base}${ext}`);
  }
});
const waterReportUpload = multer({
  storage: waterReportStorage,
  limits: { fileSize: 10 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    const mime = String(file.mimetype || "").toLowerCase();
    if (mime.startsWith("image/") || mime === "application/pdf") return cb(null, true);
    cb(new Error("Invalid file type"));
  }
});

const computeSalaryDue = (staffRow, paidTotal, asOf) => {
  if (!staffRow || !staffRow.start_date) return 0;
  const salary = Number(staffRow.monthly_salary || 0);
  if (salary <= 0) return 0;
  const start = dayjs(staffRow.start_date).startOf("month");
  const lastCompletedMonth = dayjs(asOf).startOf("month").subtract(1, "month");
  if (!start.isValid() || lastCompletedMonth.isBefore(start)) return 0;
  const months = lastCompletedMonth.diff(start, "month") + 1;
  const accrued = months * salary;
  const paid = Number(paidTotal || 0);
  return Math.max(0, accrued - paid);
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

const getSetting = (key, fallback = 0) => {
  const row = db.prepare("SELECT value FROM settings WHERE key = ?").get(key);
  if (!row) return fallback;
  const num = Number(row.value);
  return Number.isNaN(num) ? fallback : num;
};
const setSetting = (key, value) => {
  const safeValue = String(value ?? "");
  const exists = db.prepare("SELECT key FROM settings WHERE key = ?").get(key);
  if (exists) {
    db.prepare("UPDATE settings SET value = ? WHERE key = ?").run(safeValue, key);
  } else {
    db.prepare("INSERT INTO settings (key, value) VALUES (?, ?)").run(key, safeValue);
  }
};

const getWorkerAlertData = () => {
  const itemLowThresholdRaw = Number(getSetting("alert_low_stock_items", 10));
  const overdueDaysRaw = Number(getSetting("alert_overdue_credit_days", 7));
  const itemLowThreshold = Number.isNaN(itemLowThresholdRaw) ? 10 : Math.max(0, Math.floor(itemLowThresholdRaw));
  const overdueDays = Number.isNaN(overdueDaysRaw) ? 7 : Math.max(1, Math.floor(overdueDaysRaw));
  const overdueBefore = dayjs().subtract(overdueDays, "day").format("YYYY-MM-DD");

  const lowStockItems = db.prepare(
    `SELECT import_entries.item_type, import_item_types.name as item_name, import_item_types.unit_label as unit_label,
            COALESCE(SUM(CASE WHEN import_entries.direction = 'OUT' THEN -import_entries.quantity ELSE import_entries.quantity END), 0) as balance
     FROM import_entries
     LEFT JOIN import_item_types ON import_item_types.code = import_entries.item_type
     GROUP BY import_entries.item_type, import_item_types.name, import_item_types.unit_label
     HAVING COALESCE(SUM(CASE WHEN import_entries.direction = 'OUT' THEN -import_entries.quantity ELSE import_entries.quantity END), 0) <= ?
     ORDER BY balance ASC, import_entries.item_type ASC`
  ).all(itemLowThreshold).map((row) => ({
    ...row,
    item_name: row.item_name || row.item_type
  }));

  const overdueCustomerCredits = db.prepare(
    `SELECT credits.id, credits.credit_date, credits.customer_name, credits.amount, credits.paid_amount,
            vehicles.vehicle_number,
            CASE WHEN credits.amount - credits.paid_amount < 0 THEN 0 ELSE credits.amount - credits.paid_amount END as remaining_amount
     FROM credits
     JOIN vehicles ON vehicles.id = credits.vehicle_id
     WHERE credits.credit_date <= ?
       AND credits.amount - credits.paid_amount > 0
     ORDER BY credits.credit_date ASC, remaining_amount DESC
     LIMIT 20`
  ).all(overdueBefore);

  const overdueVehicleCredits = db.prepare(
    `SELECT exports.id, exports.export_date, vehicles.vehicle_number, vehicles.owner_name,
            exports.credit_amount, exports.paid_amount,
            CASE WHEN exports.credit_amount - exports.paid_amount < 0 THEN 0 ELSE exports.credit_amount - exports.paid_amount END as remaining_amount
     FROM exports
     JOIN vehicles ON vehicles.id = exports.vehicle_id
     WHERE vehicles.is_company = 0
       AND exports.export_date <= ?
       AND exports.credit_amount - exports.paid_amount > 0
     ORDER BY exports.export_date ASC, remaining_amount DESC
     LIMIT 20`
  ).all(overdueBefore);

  const pendingCustomerTotals = db.prepare(
    `SELECT COUNT(*) as count,
            COALESCE(SUM(CASE WHEN amount - paid_amount < 0 THEN 0 ELSE amount - paid_amount END), 0) as remaining
     FROM credits
     WHERE amount - paid_amount > 0`
  ).get();

  const pendingVehicleTotals = db.prepare(
    `SELECT COUNT(*) as count,
            COALESCE(SUM(CASE WHEN exports.credit_amount - exports.paid_amount < 0 THEN 0 ELSE exports.credit_amount - exports.paid_amount END), 0) as remaining
     FROM exports
     JOIN vehicles ON vehicles.id = exports.vehicle_id
     WHERE vehicles.is_company = 0
       AND exports.credit_amount - exports.paid_amount > 0`
  ).get();

  return {
    thresholds: {
      itemLowThreshold,
      overdueDays
    },
    lowStockItems,
    overdueCustomerCredits,
    overdueVehicleCredits,
    summary: {
      lowStockCount: lowStockItems.length,
      pendingCustomerCount: Number(pendingCustomerTotals.count || 0),
      pendingCustomerAmount: Number(pendingCustomerTotals.remaining || 0),
      pendingVehicleCount: Number(pendingVehicleTotals.count || 0),
      pendingVehicleAmount: Number(pendingVehicleTotals.remaining || 0),
      totalAlerts:
        lowStockItems.length +
        Number(pendingCustomerTotals.count || 0) +
        Number(pendingVehicleTotals.count || 0)
    }
  };
};

const getStaffOptions = () => db.prepare(
  "SELECT id, full_name FROM staff WHERE COALESCE(is_active, 1) = 1 ORDER BY full_name ASC"
).all();

const getExportStaffOptions = () => db.prepare(
  `SELECT staff.id, staff.full_name
   FROM staff
   JOIN staff_roles ON staff_roles.code = staff.staff_role
   WHERE COALESCE(staff.is_active, 1) = 1
     AND COALESCE(staff_roles.show_in_exports, 0) = 1
     AND COALESCE(staff_roles.is_active, 1) = 1
   ORDER BY staff.full_name ASC`
).all();

const getExportStaffById = (staffId) => db.prepare(
  `SELECT staff.id, staff.full_name
   FROM staff
   JOIN staff_roles ON staff_roles.code = staff.staff_role
   WHERE staff.id = ?
     AND COALESCE(staff.is_active, 1) = 1
     AND COALESCE(staff_roles.show_in_exports, 0) = 1
     AND COALESCE(staff_roles.is_active, 1) = 1`
).get(staffId);

const getExportStaffByName = (fullName) => db.prepare(
  `SELECT staff.id, staff.full_name
   FROM staff
   JOIN staff_roles ON staff_roles.code = staff.staff_role
   WHERE lower(trim(staff.full_name)) = lower(trim(?))
     AND COALESCE(staff.is_active, 1) = 1
     AND COALESCE(staff_roles.show_in_exports, 0) = 1
     AND COALESCE(staff_roles.is_active, 1) = 1
   LIMIT 1`
).get(fullName);

const getExportNameSuggestions = () => {
  const staffNames = getExportStaffOptions()
    .map((row) => String(row.full_name || "").trim())
    .filter(Boolean);
  return Array.from(new Set(staffNames))
    .sort((a, b) => a.localeCompare(b));
};

const parseCheckbox = (value) => {
  if (typeof value === "boolean") return value;
  const safe = String(value || "").trim().toLowerCase();
  return safe === "1" || safe === "true" || safe === "on" || safe === "yes";
};

const parseMoneyValue = (value) => {
  const num = Number(value || 0);
  if (Number.isNaN(num) || num < 0) return 0;
  return Math.round(num * 100) / 100;
};

const paymentBreakdownOrder = ["cash", "bank", "eWallet"];
const paymentMethodByBreakdownKey = {
  cash: "CASH",
  bank: "BANK",
  eWallet: "E_WALLET"
};

const clampBreakdownToTotal = (breakdown, maxTotal) => {
  const safeMax = parseMoneyValue(maxTotal);
  if (safeMax <= 0) {
    return { cash: 0, bank: 0, eWallet: 0 };
  }
  let remaining = safeMax;
  const out = { cash: 0, bank: 0, eWallet: 0 };
  paymentBreakdownOrder.forEach((key) => {
    const amount = parseMoneyValue(breakdown?.[key] || 0);
    if (remaining <= 0 || amount <= 0) return;
    const used = parseMoneyValue(Math.min(amount, remaining));
    out[key] = used;
    remaining = parseMoneyValue(remaining - used);
  });
  return out;
};

const sumPaymentBreakdown = (breakdown) => parseMoneyValue(
  parseMoneyValue(breakdown?.cash || 0) +
  parseMoneyValue(breakdown?.bank || 0) +
  parseMoneyValue(breakdown?.eWallet || 0)
);

const getPrimaryMethodFromBreakdown = (breakdown, fallbackMethod = "CASH") => {
  const safeFallback = normalizePaymentMethod(fallbackMethod);
  const active = paymentBreakdownOrder
    .filter((key) => parseMoneyValue(breakdown?.[key] || 0) > 0)
    .map((key) => paymentMethodByBreakdownKey[key]);
  if (!active.length) return safeFallback;
  if (active.length === 1) return active[0];
  return safeFallback;
};

const getPaymentMethodFromBreakdown = (breakdown, fallbackMethod = "CASH", allowMixed = false) => {
  const safeFallback = normalizePaymentMethod(fallbackMethod);
  const active = paymentBreakdownOrder
    .filter((key) => parseMoneyValue(breakdown?.[key] || 0) > 0)
    .map((key) => paymentMethodByBreakdownKey[key]);
  if (!active.length) return safeFallback;
  if (active.length === 1) return active[0];
  return allowMixed ? "MIXED" : safeFallback;
};

const parsePaymentBreakdownFromBody = (body, options = {}) => {
  const cashField = options.cashField || "cash_amount";
  const bankField = options.bankField || "bank_amount";
  const ewalletField = options.ewalletField || "ewallet_amount";
  const amountField = options.amountField || "payment_amount";
  const methodField = options.methodField || "payment_method";
  const maxTotal = options.maxTotal;
  const strictMax = Boolean(options.strictMax);

  const splitRaw = {
    cash: parseMoneyValue(body?.[cashField] || 0),
    bank: parseMoneyValue(body?.[bankField] || 0),
    eWallet: parseMoneyValue(body?.[ewalletField] || 0)
  };
  const splitEntered = sumPaymentBreakdown(splitRaw) > 0;

  let breakdown = splitRaw;
  if (!splitEntered) {
    const total = parseMoneyValue(body?.[amountField] || 0);
    const method = normalizePaymentMethod(body?.[methodField]);
    breakdown = {
      cash: method === "CASH" ? total : 0,
      bank: method === "BANK" ? total : 0,
      eWallet: method === "E_WALLET" ? total : 0
    };
  }

  const hasMaxTotal = typeof maxTotal !== "undefined" && maxTotal !== null;
  const safeMaxTotal = hasMaxTotal ? parseMoneyValue(maxTotal) : null;
  const enteredTotal = sumPaymentBreakdown(breakdown);
  const overLimitBy = hasMaxTotal ? parseMoneyValue(enteredTotal - safeMaxTotal) : 0;
  const isOverLimit = hasMaxTotal && overLimitBy > 0;
  if (hasMaxTotal && !strictMax) {
    breakdown = clampBreakdownToTotal(breakdown, safeMaxTotal);
  }

  const total = sumPaymentBreakdown(breakdown);
  const primaryMethod = getPrimaryMethodFromBreakdown(
    breakdown,
    normalizePaymentMethod(body?.[methodField])
  );

  return {
    breakdown,
    total,
    primaryMethod,
    splitEntered,
    enteredTotal,
    maxTotal: safeMaxTotal,
    isOverLimit,
    overLimitBy
  };
};

const roundMoneySigned = (value) => {
  const num = Number(value || 0);
  if (Number.isNaN(num)) return 0;
  return Math.round(num * 100) / 100;
};

const computeRemainingMoney = (total, paid) => {
  const safeTotal = parseMoneyValue(total);
  const safePaid = parseMoneyValue(paid);
  const diff = parseMoneyValue(safeTotal - safePaid);
  return diff > 0 ? diff : 0;
};

const findDuplicateExportEntries = ({ vehicleId, exportDate, totalAmount, excludeId = null }) => {
  if (!vehicleId || !exportDate) return [];
  const safeAmount = parseMoneyValue(totalAmount);
  return db.prepare(
    `SELECT exports.id, exports.receipt_no, exports.export_date, exports.total_amount,
            exports.created_at, vehicles.vehicle_number, vehicles.owner_name
     FROM exports
     JOIN vehicles ON vehicles.id = exports.vehicle_id
     WHERE exports.vehicle_id = ?
       AND exports.export_date = ?
       AND ABS(exports.total_amount - ?) < 0.01
       AND (? IS NULL OR exports.id != ?)
     ORDER BY exports.created_at DESC, exports.id DESC
     LIMIT 5`
  ).all(vehicleId, exportDate, safeAmount, excludeId, excludeId);
};

const findDuplicateCreditEntries = ({ vehicleId, creditDate, customerName, amount, excludeId = null }) => {
  if (!vehicleId || !creditDate || !customerName) return [];
  const safeAmount = parseMoneyValue(amount);
  const safeCustomer = String(customerName || "").trim();
  if (!safeCustomer) return [];
  return db.prepare(
    `SELECT credits.id, credits.receipt_no, credits.credit_date, credits.customer_name,
            credits.amount, credits.created_at, vehicles.vehicle_number, vehicles.owner_name
     FROM credits
     JOIN vehicles ON vehicles.id = credits.vehicle_id
     WHERE credits.vehicle_id = ?
       AND credits.credit_date = ?
       AND lower(trim(credits.customer_name)) = lower(trim(?))
       AND ABS(credits.amount - ?) < 0.01
       AND (? IS NULL OR credits.id != ?)
     ORDER BY credits.created_at DESC, credits.id DESC
     LIMIT 5`
  ).all(vehicleId, creditDate, safeCustomer, safeAmount, excludeId, excludeId);
};

const getDailyReconciliationSnapshot = (businessDate) => {
  const exportPaid = db.prepare(
    `SELECT
        COALESCE(SUM(paid_cash_amount), 0) AS cash_amount,
        COALESCE(SUM(paid_bank_amount), 0) AS bank_amount,
        COALESCE(SUM(paid_ewallet_amount), 0) AS ewallet_amount
     FROM exports
     WHERE export_date = ?`
  ).get(businessDate);
  const creditPaid = db.prepare(
    `SELECT
        COALESCE(SUM(CASE WHEN payment_method = 'CASH' THEN amount ELSE 0 END), 0) AS cash_amount,
        COALESCE(SUM(CASE WHEN payment_method = 'BANK' THEN amount ELSE 0 END), 0) AS bank_amount,
        COALESCE(SUM(CASE WHEN payment_method = 'E_WALLET' THEN amount ELSE 0 END), 0) AS ewallet_amount
     FROM credit_payments
     WHERE date(paid_at) = ?`
  ).get(businessDate);
  const rentPaid = db.prepare(
    `SELECT
        COALESCE(SUM(CASE WHEN add_to_collection = 1 AND payment_method = 'CASH' THEN amount ELSE 0 END), 0) AS cash_amount,
        COALESCE(SUM(CASE WHEN add_to_collection = 1 AND payment_method = 'BANK' THEN amount ELSE 0 END), 0) AS bank_amount,
        COALESCE(SUM(CASE WHEN add_to_collection = 1 AND payment_method = 'E_WALLET' THEN amount ELSE 0 END), 0) AS ewallet_amount
     FROM rent_entries
     WHERE rent_date = ?`
  ).get(businessDate);
  const jarSalePaid = db.prepare(
    `SELECT COALESCE(SUM(paid_amount), 0) AS amount
     FROM jar_sales
     WHERE sale_date = ?`
  ).get(businessDate);
  const savingsDeposit = db.prepare(
    `SELECT COALESCE(SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END), 0) AS amount
     FROM vehicle_savings
     WHERE entry_date = ?`
  ).get(businessDate);

  const expectedCash = parseMoneyValue(
    Number(exportPaid.cash_amount || 0) +
    Number(creditPaid.cash_amount || 0) +
    Number(rentPaid.cash_amount || 0) +
    Number(jarSalePaid.amount || 0) +
    Number(savingsDeposit.amount || 0)
  );
  const expectedBank = parseMoneyValue(
    Number(exportPaid.bank_amount || 0) +
    Number(creditPaid.bank_amount || 0) +
    Number(rentPaid.bank_amount || 0)
  );
  const expectedEwallet = parseMoneyValue(
    Number(exportPaid.ewallet_amount || 0) +
    Number(creditPaid.ewallet_amount || 0) +
    Number(rentPaid.ewallet_amount || 0)
  );
  const expectedTotal = parseMoneyValue(expectedCash + expectedBank + expectedEwallet);

  const importsFromCollection = db.prepare(
    "SELECT COALESCE(SUM(amount), 0) AS amount FROM import_payments WHERE payment_date = ? AND payment_source = 'DAILY_COLLECTION'"
  ).get(businessDate);
  const purchasesFromCollection = db.prepare(
    "SELECT COALESCE(SUM(amount), 0) AS amount FROM company_purchase_payments WHERE payment_date = ? AND payment_source = 'DAILY_COLLECTION'"
  ).get(businessDate);
  const vehicleExpensesFromCollection = db.prepare(
    "SELECT COALESCE(SUM(amount), 0) AS amount FROM vehicle_expense_payments WHERE payment_date = ? AND payment_source = 'DAILY_COLLECTION'"
  ).get(businessDate);
  const staffSalaryFromCollection = db.prepare(
    "SELECT COALESCE(SUM(amount), 0) AS amount FROM staff_salary_payments WHERE payment_date = ? AND payment_source = 'DAILY_COLLECTION'"
  ).get(businessDate);
  const workerSalaryFromCollection = db.prepare(
    "SELECT COALESCE(SUM(amount), 0) AS amount FROM worker_salary_payments WHERE payment_date = ? AND payment_source = 'DAILY_COLLECTION'"
  ).get(businessDate);
  const savingsWithdrawFromCollection = db.prepare(
    `SELECT COALESCE(SUM(CASE WHEN amount < 0 AND payment_source = 'DAILY_COLLECTION' THEN ABS(amount) ELSE 0 END), 0) AS amount
     FROM vehicle_savings
     WHERE entry_date = ?`
  ).get(businessDate);

  const deductedFromCollection = parseMoneyValue(
    Number(importsFromCollection.amount || 0) +
    Number(purchasesFromCollection.amount || 0) +
    Number(vehicleExpensesFromCollection.amount || 0) +
    Number(staffSalaryFromCollection.amount || 0) +
    Number(workerSalaryFromCollection.amount || 0) +
    Number(savingsWithdrawFromCollection.amount || 0)
  );
  const expectedNet = roundMoneySigned(expectedTotal - deductedFromCollection);

  return {
    date: businessDate,
    inflow: {
      exports: {
        cash: parseMoneyValue(exportPaid.cash_amount || 0),
        bank: parseMoneyValue(exportPaid.bank_amount || 0),
        eWallet: parseMoneyValue(exportPaid.ewallet_amount || 0)
      },
      customerCredits: {
        cash: parseMoneyValue(creditPaid.cash_amount || 0),
        bank: parseMoneyValue(creditPaid.bank_amount || 0),
        eWallet: parseMoneyValue(creditPaid.ewallet_amount || 0)
      },
      rentIncome: {
        cash: parseMoneyValue(rentPaid.cash_amount || 0),
        bank: parseMoneyValue(rentPaid.bank_amount || 0),
        eWallet: parseMoneyValue(rentPaid.ewallet_amount || 0)
      },
      jarSalesCash: parseMoneyValue(jarSalePaid.amount || 0),
      savingsDepositsCash: parseMoneyValue(savingsDeposit.amount || 0),
      expectedCash,
      expectedBank,
      expectedEwallet,
      expectedTotal
    },
    deductions: {
      imports: parseMoneyValue(importsFromCollection.amount || 0),
      companyPurchases: parseMoneyValue(purchasesFromCollection.amount || 0),
      vehicleExpenses: parseMoneyValue(vehicleExpensesFromCollection.amount || 0),
      staffSalaries: parseMoneyValue(staffSalaryFromCollection.amount || 0),
      workerSalaries: parseMoneyValue(workerSalaryFromCollection.amount || 0),
      savingsWithdrawals: parseMoneyValue(savingsWithdrawFromCollection.amount || 0),
      total: deductedFromCollection
    },
    expected: {
      collection: expectedTotal,
      netAfterDeductions: expectedNet
    }
  };
};

const getLedgerOpeningSettingKey = (channel) => (
  channel === "E_WALLET" ? "ledger_opening_ewallet" : "ledger_opening_bank"
);

const buildPaymentLedgerData = ({ channel, from, to, openingBalance }) => {
  const entries = [];
  const pushEntry = (row, kind) => {
    const inflow = kind === "INFLOW" ? parseMoneyValue(row.amount || 0) : 0;
    const outflow = kind === "OUTFLOW" ? parseMoneyValue(row.amount || 0) : 0;
    entries.push({
      date: row.date,
      created_at: row.created_at || `${row.date} 00:00:00`,
      source: row.source,
      reference: row.reference || "-",
      party: row.party || "-",
      note: row.note || "-",
      inflow,
      outflow
    });
  };

  const exportAmountExpr = channel === "E_WALLET"
    ? "COALESCE(exports.paid_ewallet_amount, 0)"
    : "COALESCE(exports.paid_bank_amount, 0)";
  db.prepare(
    `SELECT exports.export_date as date,
            exports.created_at as created_at,
            ${exportAmountExpr} as amount,
            'EXPORT' as source,
            COALESCE(exports.receipt_no, '#' || exports.id) as reference,
            (vehicles.vehicle_number || ' • ' || vehicles.owner_name) as party,
            COALESCE(exports.note, '') as note
     FROM exports
     JOIN vehicles ON vehicles.id = exports.vehicle_id
     WHERE exports.export_date BETWEEN ? AND ?
       AND ${exportAmountExpr} > 0`
  ).all(from, to).forEach((row) => pushEntry(row, "INFLOW"));

  db.prepare(
    `SELECT date(credit_payments.paid_at) as date,
            credit_payments.paid_at as created_at,
            credit_payments.amount as amount,
            'CUSTOMER_CREDIT_PAYMENT' as source,
            ('CREDIT#' || credits.id) as reference,
            COALESCE(credits.customer_name, '-') as party,
            COALESCE(credit_payments.note, '') as note
     FROM credit_payments
     JOIN credits ON credits.id = credit_payments.credit_id
     WHERE date(credit_payments.paid_at) BETWEEN ? AND ?
       AND credit_payments.payment_method = ?`
  ).all(from, to, channel).forEach((row) => pushEntry(row, "INFLOW"));

  db.prepare(
    `SELECT rent_entries.rent_date as date,
            rent_entries.created_at as created_at,
            rent_entries.amount as amount,
            'RENT_INCOME' as source,
            ('RENT#' || rent_entries.id) as reference,
            COALESCE(rent_entries.renter_name, '-') as party,
            COALESCE(rent_entries.item_name, '') as note
     FROM rent_entries
     WHERE rent_entries.rent_date BETWEEN ? AND ?
       AND rent_entries.add_to_collection = 1
       AND rent_entries.payment_method = ?`
  ).all(from, to, channel).forEach((row) => pushEntry(row, "INFLOW"));

  db.prepare(
    `SELECT import_payments.payment_date as date,
            import_payments.created_at as created_at,
            import_payments.amount as amount,
            'IMPORT_PAYMENT' as source,
            ('IMPORT#' || import_entries.id) as reference,
            COALESCE(import_entries.seller_name, '-') as party,
            COALESCE(import_payments.note, '') as note
     FROM import_payments
     JOIN import_entries ON import_entries.id = import_payments.import_entry_id
     WHERE import_payments.payment_date BETWEEN ? AND ?
       AND import_payments.payment_method = ?`
  ).all(from, to, channel).forEach((row) => pushEntry(row, "OUTFLOW"));

  db.prepare(
    `SELECT company_purchase_payments.payment_date as date,
            company_purchase_payments.created_at as created_at,
            company_purchase_payments.amount as amount,
            'COMPANY_PURCHASE' as source,
            ('PURCHASE#' || company_purchases.id) as reference,
            COALESCE(company_purchases.seller_name, '-') as party,
            COALESCE(company_purchase_payments.note, company_purchases.item_name, '') as note
     FROM company_purchase_payments
     JOIN company_purchases ON company_purchases.id = company_purchase_payments.company_purchase_id
     WHERE company_purchase_payments.payment_date BETWEEN ? AND ?
       AND company_purchase_payments.payment_method = ?`
  ).all(from, to, channel).forEach((row) => pushEntry(row, "OUTFLOW"));

  db.prepare(
    `SELECT vehicle_expense_payments.payment_date as date,
            vehicle_expense_payments.created_at as created_at,
            vehicle_expense_payments.amount as amount,
            'VEHICLE_EXPENSE' as source,
            ('VEXP#' || vehicle_expenses.id) as reference,
            (vehicles.vehicle_number || ' • ' || vehicles.owner_name) as party,
            COALESCE(vehicle_expense_payments.note, vehicle_expenses.expense_type, '') as note
     FROM vehicle_expense_payments
     JOIN vehicle_expenses ON vehicle_expenses.id = vehicle_expense_payments.vehicle_expense_id
     JOIN vehicles ON vehicles.id = vehicle_expenses.vehicle_id
     WHERE vehicle_expense_payments.payment_date BETWEEN ? AND ?
       AND vehicle_expense_payments.payment_method = ?`
  ).all(from, to, channel).forEach((row) => pushEntry(row, "OUTFLOW"));

  entries.sort((a, b) => {
    const byDate = String(a.date).localeCompare(String(b.date));
    if (byDate !== 0) return byDate;
    const byTime = String(a.created_at).localeCompare(String(b.created_at));
    if (byTime !== 0) return byTime;
    return String(a.reference).localeCompare(String(b.reference));
  });

  let running = parseMoneyValue(openingBalance || 0);
  let totalInflow = 0;
  let totalOutflow = 0;
  const rows = entries.map((entry) => {
    totalInflow = parseMoneyValue(totalInflow + entry.inflow);
    totalOutflow = parseMoneyValue(totalOutflow + entry.outflow);
    running = roundMoneySigned(running + entry.inflow - entry.outflow);
    return { ...entry, balance: running };
  });

  return {
    rows,
    totals: {
      inflow: totalInflow,
      outflow: totalOutflow,
      net: roundMoneySigned(totalInflow - totalOutflow),
      opening: parseMoneyValue(openingBalance || 0),
      closing: running
    }
  };
};

const getVendorAgingBucket = (days) => {
  if (days <= 7) return "0_7";
  if (days <= 30) return "8_30";
  return "30_plus";
};

const getComplianceSummaryRow = (row, todayDate) => {
  const statuses = complianceDateFields.map((key) => getDateStatus(row[key], todayDate));
  let overall = "VALID";
  statuses.forEach((status) => {
    if (complianceStatusPriority[status.code] > complianceStatusPriority[overall]) {
      overall = status.code;
    }
  });
  return {
    ...row,
    status_insurance: statuses[0],
    status_tax: statuses[1],
    status_permit: statuses[2],
    status_fitness: statuses[3],
    status_pollution: statuses[4],
    overall_status: overall
  };
};

const getBottleCaseStorageBalance = ({ excludeExportId = null } = {}) => {
  const importRow = db.prepare(
    `SELECT COALESCE(SUM(CASE WHEN direction = 'OUT' THEN -quantity ELSE quantity END), 0) as qty
     FROM import_entries
     WHERE item_type = 'BOTTLE_CASE'`
  ).get();
  const exportRow = excludeExportId
    ? db.prepare(
      `SELECT COALESCE(SUM(bottle_case_count), 0) as exported,
              COALESCE(SUM(return_bottle_case_count), 0) as returned
       FROM exports
       WHERE id != ?`
    ).get(excludeExportId)
    : db.prepare(
      `SELECT COALESCE(SUM(bottle_case_count), 0) as exported,
              COALESCE(SUM(return_bottle_case_count), 0) as returned
       FROM exports`
    ).get();
  const imported = Number(importRow?.qty || 0);
  const exported = Number(exportRow?.exported || 0);
  const returned = Number(exportRow?.returned || 0);
  const netExport = Math.max(0, exported - returned);
  return imported - netExport;
};

const getDispenserStorageBalance = ({ excludeExportId = null } = {}) => {
  const importRow = db.prepare(
    `SELECT COALESCE(SUM(CASE WHEN direction = 'OUT' THEN -quantity ELSE quantity END), 0) as qty
     FROM import_entries
     WHERE item_type = 'DISPENSER'`
  ).get();
  const exportRow = excludeExportId
    ? db.prepare(
      `SELECT COALESCE(SUM(dispenser_count), 0) as exported
       FROM exports
       WHERE id != ?`
    ).get(excludeExportId)
    : db.prepare(
      `SELECT COALESCE(SUM(dispenser_count), 0) as exported
       FROM exports`
    ).get();
  const imported = Number(importRow?.qty || 0);
  const exported = Number(exportRow?.exported || 0);
  return imported - exported;
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

const getAttendanceIotEnabled = () => String(
  (db.prepare("SELECT value FROM settings WHERE key = 'iot_attendance_enabled'").get() || { value: "0" }).value
) === "1";

const parsePaymentAmount = (details) => {
  const match = String(details || "").match(/payment=([0-9.]+)/i);
  if (!match) return 0;
  const value = Number(match[1]);
  return Number.isNaN(value) ? 0 : value;
};

const parsePaymentMethodFromDetails = (details) => {
  const match = String(details || "").match(/method=([A-Z_]+)/i);
  if (!match) return "CASH";
  const raw = String(match[1] || "").toUpperCase();
  if (raw === "MIXED") return "MIXED";
  return normalizePaymentMethod(raw);
};

const buildCreditsListUrl = (params = {}) => {
  const from = params.from || dayjs().subtract(7, "day").format("YYYY-MM-DD");
  const to = params.to || dayjs().format("YYYY-MM-DD");
  const customerCreditFrom = String(params.customer_credit_from || params.customerCreditFrom || from);
  const customerCreditTo = String(params.customer_credit_to || params.customerCreditTo || to);
  const status = ["all", "paid", "unpaid", "partial"].includes(params.status) ? params.status : "all";
  const sort = String(params.sort || "date_desc");
  const q = String(params.q || "").trim();
  const notice = String(params.notice || "").trim();
  const error = String(params.error || "").trim();

  const query = new URLSearchParams();
  query.set("from", from);
  query.set("to", to);
  query.set("status", status);
  query.set("sort", sort);
  query.set("customer_credit_from", customerCreditFrom);
  query.set("customer_credit_to", customerCreditTo);
  if (q) query.set("q", q);
  if (notice) query.set("notice", notice);
  if (error) query.set("error", error);
  return `/records/credits?${query.toString()}`;
};

const buildExportsListUrl = (params = {}) => {
  const from = params.from || dayjs().subtract(7, "day").format("YYYY-MM-DD");
  const to = params.to || dayjs().format("YYYY-MM-DD");
  const vehicleCreditFrom = String(params.vehicle_credit_from || params.vehicleCreditFrom || from);
  const vehicleCreditTo = String(params.vehicle_credit_to || params.vehicleCreditTo || to);
  const q = String(params.q || "").trim();
  const sort = String(params.sort || "date_desc");
  const tripVehicleId = String(params.trip_vehicle_id || params.tripVehicleId || "").trim();
  const status = String(params.status || "").trim();
  const error = String(params.error || "").trim();

  const query = new URLSearchParams();
  query.set("from", from);
  query.set("to", to);
  query.set("sort", sort);
  query.set("vehicle_credit_from", vehicleCreditFrom);
  query.set("vehicle_credit_to", vehicleCreditTo);
  if (q) query.set("q", q);
  if (tripVehicleId) query.set("trip_vehicle_id", tripVehicleId);
  if (status) query.set("status", status);
  if (error) query.set("error", error);
  return `/records/exports?${query.toString()}`;
};

const applyCreditSettlementPayment = ({ creditRows, paymentAmount, paymentBreakdown, note, userId, paymentMethod }) => {
  const rows = Array.isArray(creditRows) ? creditRows : [];
  const totalRemaining = rows.reduce((sum, row) => {
    const remaining = computeRemainingMoney(row.amount || 0, row.paid_amount || 0);
    return parseMoneyValue(sum + remaining);
  }, 0);
  if (totalRemaining <= 0) return { applied: 0, totalRemaining: 0, count: 0 };

  let breakdown = paymentBreakdown && typeof paymentBreakdown === "object"
    ? {
      cash: parseMoneyValue(paymentBreakdown.cash || 0),
      bank: parseMoneyValue(paymentBreakdown.bank || 0),
      eWallet: parseMoneyValue(paymentBreakdown.eWallet || 0)
    }
    : null;
  if (!breakdown || sumPaymentBreakdown(breakdown) <= 0) {
    const safeMethod = normalizePaymentMethod(paymentMethod);
    const safeAmount = parseMoneyValue(paymentAmount || 0);
    breakdown = {
      cash: safeMethod === "CASH" ? safeAmount : 0,
      bank: safeMethod === "BANK" ? safeAmount : 0,
      eWallet: safeMethod === "E_WALLET" ? safeAmount : 0
    };
  }
  breakdown = clampBreakdownToTotal(breakdown, totalRemaining);
  const totalToApply = sumPaymentBreakdown(breakdown);
  if (Number.isNaN(totalToApply) || totalToApply <= 0) {
    return { applied: 0, totalRemaining, count: 0 };
  }
  const remainingByMethod = { ...breakdown };

  let applied = 0;
  let count = 0;
  db.exec("BEGIN;");
  try {
    rows.forEach((row) => {
      if (sumPaymentBreakdown(remainingByMethod) <= 0) return;
      const amount = parseMoneyValue(row.amount || 0);
      const paid = parseMoneyValue(row.paid_amount || 0);
      const remaining = computeRemainingMoney(amount, paid);
      if (remaining <= 0) return;

      let rowApplied = 0;
      const rowBreakdown = { cash: 0, bank: 0, eWallet: 0 };
      paymentBreakdownOrder.forEach((key) => {
        const methodRemaining = parseMoneyValue(remainingByMethod[key] || 0);
        if (methodRemaining <= 0) return;
        const rowRemaining = parseMoneyValue(remaining - rowApplied);
        if (rowRemaining <= 0) return;
        const share = parseMoneyValue(Math.min(methodRemaining, rowRemaining));
        if (share <= 0) return;
        rowBreakdown[key] = share;
        remainingByMethod[key] = parseMoneyValue(methodRemaining - share);
        rowApplied = parseMoneyValue(rowApplied + share);
      });
      if (rowApplied <= 0) return;

      const newPaid = parseMoneyValue(Math.min(amount, paid + rowApplied));
      const paidFlag = amount === 0 ? 1 : newPaid >= amount ? 1 : 0;

      db.prepare("UPDATE credits SET paid_amount = ?, paid = ? WHERE id = ?").run(newPaid, paidFlag, row.id);
      paymentBreakdownOrder.forEach((key) => {
        const share = parseMoneyValue(rowBreakdown[key] || 0);
        if (share <= 0) return;
        insertCreditPayment({
          creditId: row.id,
          amount: share,
          note,
          userId,
          paymentMethod: paymentMethodByBreakdownKey[key]
        });
      });

      applied = parseMoneyValue(applied + rowApplied);
      count += 1;
    });
    db.exec("COMMIT;");
  } catch (err) {
    db.exec("ROLLBACK;");
    throw err;
  }

  return { applied, totalRemaining, count };
};

const resolveExportVehicleInput = ({
  vehicleId,
  useExternalVehicle,
  externalVehicleNumber,
  externalOwnerName,
  externalPhone
}) => {
  const useExternal = parseCheckbox(useExternalVehicle);
  const externalVehicleNumberSafe = String(externalVehicleNumber || "").trim();
  const externalOwnerNameSafe = String(externalOwnerName || "").trim();
  const externalPhoneSafe = String(externalPhone || "").trim();
  let resolvedVehicleId = Number(vehicleId || 0);

  if (useExternal) {
    if (!externalVehicleNumberSafe || !externalOwnerNameSafe) {
      return {
        errorKey: "externalVehicleRequired",
        useExternalVehicle: true,
        vehicleId: null,
        externalVehicleNumber: externalVehicleNumberSafe,
        externalOwnerName: externalOwnerNameSafe,
        externalPhone: externalPhoneSafe
      };
    }

    const existingVehicle = db.prepare(
      "SELECT id, is_company FROM vehicles WHERE lower(trim(vehicle_number)) = lower(trim(?)) LIMIT 1"
    ).get(externalVehicleNumberSafe);

    if (existingVehicle) {
      if (Number(existingVehicle.is_company) === 1) {
        return {
          errorKey: "externalVehicleCompanyConflict",
          useExternalVehicle: true,
          vehicleId: null,
          externalVehicleNumber: externalVehicleNumberSafe,
          externalOwnerName: externalOwnerNameSafe,
          externalPhone: externalPhoneSafe
        };
      }
      resolvedVehicleId = Number(existingVehicle.id);
    } else {
      const created = db.prepare(
        "INSERT INTO vehicles (vehicle_number, owner_name, phone, is_company) VALUES (?, ?, ?, 0)"
      ).run(
        externalVehicleNumberSafe,
        externalOwnerNameSafe,
        externalPhoneSafe || null
      );
      resolvedVehicleId = Number(created.lastInsertRowid || 0);
    }
  }

  if (!resolvedVehicleId) {
    return {
      errorKey: "salesRequired",
      useExternalVehicle: useExternal,
      vehicleId: null,
      externalVehicleNumber: externalVehicleNumberSafe,
      externalOwnerName: externalOwnerNameSafe,
      externalPhone: externalPhoneSafe
    };
  }

  return {
    errorKey: null,
    useExternalVehicle: useExternal,
    vehicleId: resolvedVehicleId,
    externalVehicleNumber: externalVehicleNumberSafe,
    externalOwnerName: externalOwnerNameSafe,
    externalPhone: externalPhoneSafe
  };
};

const buildExternalVehicleNote = ({
  useExternalVehicle,
  externalOwnerName,
  externalPhone,
  externalOrganization
}) => {
  if (!useExternalVehicle) return null;
  const owner = String(externalOwnerName || "").trim();
  const phone = String(externalPhone || "").trim();
  const organization = String(externalOrganization || "").trim();
  const segments = [];
  if (owner) segments.push(`person=${owner}`);
  if (phone) segments.push(`phone=${phone}`);
  if (organization) segments.push(`org=${organization}`);
  if (!segments.length) return null;
  return `external(${segments.join(", ")})`;
};

const mergeNoteWithExternalVehicle = (noteValue, externalNote) => {
  const base = String(noteValue || "").trim();
  if (!externalNote) return base || null;
  if (!base) return externalNote;
  if (base.includes(externalNote)) return base;
  return `${base} | ${externalNote}`;
};

const getImportItemTypes = (includeInactive = false) => {
  const where = includeInactive ? "" : "WHERE is_active = 1";
  return db.prepare(
    `SELECT id, code, name, unit_label, uses_direction, is_predefined, is_active
     FROM import_item_types
     ${where}
     ORDER BY is_predefined DESC, name ASC`
  ).all();
};

const getImportItemTypeByCode = (code) => db.prepare(
  "SELECT id, code, name, unit_label, uses_direction, is_predefined, is_active FROM import_item_types WHERE code = ?"
).get(code);

const resolveImportItemLabel = (code, rawName, t) => {
  const key = importLabelKeyByCode[code];
  if (key) return t(key);
  return rawName || code;
};

const resolveImportItemUnit = (code, rawUnitLabel, t) => {
  if (rawUnitLabel) return rawUnitLabel;
  const key = importUnitFallbackByCode[code];
  return key ? t(key) : "";
};

const normalizeVehicleExpenseType = (value) => {
  const safe = String(value || "").trim().toUpperCase();
  return vehicleExpenseTypes.includes(safe) ? safe : "FUEL";
};

const getVehicleExpenseTypeLabel = (type, t) => {
  const safe = normalizeVehicleExpenseType(type);
  if (safe === "REPAIR") return t("vehicleExpenseTypeRepair");
  if (safe === "SERVICE") return t("vehicleExpenseTypeService");
  if (safe === "OTHER") return t("vehicleExpenseTypeOther");
  return t("vehicleExpenseTypeFuel");
};

const getImportItemsForUi = (t, includeInactive = false) => {
  return getImportItemTypes(includeInactive).map((row) => ({
    ...row,
    label: resolveImportItemLabel(row.code, row.name, t),
    unit: resolveImportItemUnit(row.code, row.unit_label, t),
    uses_direction: Number(row.uses_direction) === 1
  }));
};

const getCreditVehicles = () => db.prepare(
  "SELECT id, vehicle_number, owner_name, is_company FROM vehicles WHERE is_company = 0 ORDER BY vehicle_number"
).all();

const getCreditTripRows = () => {
  const from = dayjs().subtract(120, "day").format("YYYY-MM-DD");
  return db.prepare(
    `SELECT exports.id, exports.vehicle_id, exports.export_date, exports.receipt_no, exports.total_amount
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     WHERE exports.export_date >= ? AND vehicles.is_company = 0
     ORDER BY exports.export_date DESC, exports.id DESC`
  ).all(from);
};

const parseOptionalId = (value) => {
  const num = Number(value || 0);
  if (Number.isNaN(num) || num <= 0) return null;
  return num;
};

const parseOptionalDate = (value) => {
  const text = String(value || "").trim();
  if (!text) return null;
  return /^\d{4}-\d{2}-\d{2}$/.test(text) ? text : null;
};

const insertCreditPayment = ({ creditId, amount, note, paidAt, userId, paymentMethod }) => {
  const numericAmount = Number(amount || 0);
  if (!creditId || Number.isNaN(numericAmount) || numericAmount === 0) return;
  const timestamp = paidAt || dayjs().format("YYYY-MM-DD HH:mm:ss");
  const method = normalizePaymentMethod(paymentMethod);
  db.prepare(
    "INSERT INTO credit_payments (credit_id, amount, payment_method, note, created_by, paid_at) VALUES (?, ?, ?, ?, ?, ?)"
  ).run(creditId, numericAmount, method, note || null, userId || null, timestamp);
};

const getUserAttendancePayload = (userId) => {
  const today = dayjs().format("YYYY-MM-DD");
  const attendanceRow = db.prepare(
    "SELECT status FROM user_attendance WHERE user_id = ? AND attendance_date = ?"
  ).get(userId, today);
  const attendanceHistory = db.prepare(
    `SELECT user_attendance.attendance_date, user_attendance.status, users.full_name as recorded_by
     FROM user_attendance
     LEFT JOIN users ON user_attendance.recorded_by = users.id
     WHERE user_attendance.user_id = ?
     ORDER BY attendance_date DESC
     LIMIT 14`
  ).all(userId);
  return {
    attendanceDate: today,
    attendanceStatus: attendanceRow ? attendanceRow.status : null,
    attendanceHistory
  };
};

const attachWorkerPhoto = (user) => {
  if (!user || user.role !== "WORKER") return user;
  const doc = db.prepare("SELECT photo_path FROM worker_documents WHERE worker_id = ?").get(user.id);
  return { ...user, photo_path: doc ? doc.photo_path : null };
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

const getDateFromEntityById = (table, id, column) => {
  if (!id) return null;
  const row = db.prepare(`SELECT ${column} FROM ${table} WHERE id = ?`).get(id);
  return row ? row[column] : null;
};

const extractIdFromPath = (pathName, pattern) => {
  const match = pathName.match(pattern);
  return match ? match[1] : null;
};

const getClosedDateForMutation = (req) => {
  const body = req.body || {};
  const pathName = req.path || "";

  if (pathName === "/exports") return body.export_date || null;
  if (/^\/exports\/\d+$/.test(pathName)) {
    const id = extractIdFromPath(pathName, /^\/exports\/(\d+)$/);
    return body.export_date || getDateFromEntityById("exports", id, "export_date");
  }
  if (/^\/exports\/\d+\/delete$/.test(pathName)) {
    const id = extractIdFromPath(pathName, /^\/exports\/(\d+)\/delete$/);
    return getDateFromEntityById("exports", id, "export_date");
  }
  if (/^\/exports\/\d+\/pay-credit$/.test(pathName)) {
    const id = extractIdFromPath(pathName, /^\/exports\/(\d+)\/pay-credit$/);
    return getDateFromEntityById("exports", id, "export_date");
  }

  if (pathName === "/imports") return body.entry_date || null;
  if (/^\/imports\/\d+$/.test(pathName)) {
    const id = extractIdFromPath(pathName, /^\/imports\/(\d+)$/);
    return body.entry_date || getDateFromEntityById("import_entries", id, "entry_date");
  }
  if (/^\/imports\/\d+\/delete$/.test(pathName)) {
    const id = extractIdFromPath(pathName, /^\/imports\/(\d+)\/delete$/);
    return getDateFromEntityById("import_entries", id, "entry_date");
  }

  if (pathName === "/savings") return body.entry_date || null;
  if (/^\/savings\/\d+$/.test(pathName)) {
    const id = extractIdFromPath(pathName, /^\/savings\/(\d+)$/);
    return body.entry_date || getDateFromEntityById("vehicle_savings", id, "entry_date");
  }
  if (/^\/savings\/\d+\/delete$/.test(pathName)) {
    const id = extractIdFromPath(pathName, /^\/savings\/(\d+)\/delete$/);
    return getDateFromEntityById("vehicle_savings", id, "entry_date");
  }

  if (pathName === "/vehicle-expenses") return body.expense_date || null;
  if (/^\/vehicle-expenses\/\d+\/delete$/.test(pathName)) {
    const id = extractIdFromPath(pathName, /^\/vehicle-expenses\/(\d+)\/delete$/);
    return getDateFromEntityById("vehicle_expenses", id, "expense_date");
  }

  if (pathName === "/rentals") return body.rent_date || null;
  if (/^\/rentals\/\d+\/delete$/.test(pathName)) {
    const id = extractIdFromPath(pathName, /^\/rentals\/(\d+)\/delete$/);
    return getDateFromEntityById("rent_entries", id, "rent_date");
  }

  if (pathName === "/reconciliation") return body.business_date || null;

  if (pathName === "/jar-sales") return body.sale_date || null;
  if (/^\/jar-sales\/\d+$/.test(pathName)) {
    const id = extractIdFromPath(pathName, /^\/jar-sales\/(\d+)$/);
    return body.sale_date || getDateFromEntityById("jar_sales", id, "sale_date");
  }
  if (/^\/jar-sales\/\d+\/delete$/.test(pathName)) {
    const id = extractIdFromPath(pathName, /^\/jar-sales\/(\d+)\/delete$/);
    return getDateFromEntityById("jar_sales", id, "sale_date");
  }

  if (pathName === "/credits") return body.credit_date || null;
  if (/^\/credits\/\d+$/.test(pathName)) {
    const id = extractIdFromPath(pathName, /^\/credits\/(\d+)$/);
    return body.credit_date || getDateFromEntityById("credits", id, "credit_date");
  }
  if (/^\/credits\/\d+\/delete$/.test(pathName)) {
    const id = extractIdFromPath(pathName, /^\/credits\/(\d+)\/delete$/);
    return getDateFromEntityById("credits", id, "credit_date");
  }

  return null;
};

const getCombinedCredits = ({ from, to, q }) => {
  const exportSearchClause = q ? "AND (vehicles.vehicle_number LIKE ? OR vehicles.owner_name LIKE ?)" : "";
  const creditSearchClause = q ? "AND (vehicles.vehicle_number LIKE ? OR vehicles.owner_name LIKE ? OR credits.customer_name LIKE ?)" : "";
  const exportParams = q ? [from, to, `%${q}%`, `%${q}%`] : [from, to];
  const creditParams = q ? [from, to, `%${q}%`, `%${q}%`, `%${q}%`] : [from, to];
  const params = [...exportParams, ...creditParams];

  const rows = db.prepare(
    `SELECT 'export' as source, exports.export_date as credit_date, vehicles.vehicle_number, vehicles.owner_name,
            NULL as customer_name, exports.credit_amount as credit_amount, 0 as paid_amount,
            exports.credit_amount as remaining_amount
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     WHERE exports.export_date BETWEEN ? AND ?
       AND vehicles.is_company = 0
       AND exports.credit_amount > 0
       ${exportSearchClause}
     UNION ALL
     SELECT 'customer' as source, credits.credit_date as credit_date, vehicles.vehicle_number, vehicles.owner_name,
            credits.customer_name as customer_name, credits.amount as credit_amount, credits.paid_amount as paid_amount,
            CASE WHEN credits.amount - credits.paid_amount < 0 THEN 0 ELSE credits.amount - credits.paid_amount END as remaining_amount
     FROM credits
     JOIN vehicles ON credits.vehicle_id = vehicles.id
     WHERE credits.credit_date BETWEEN ? AND ?
       AND vehicles.is_company = 0
       ${creditSearchClause}
     ORDER BY credit_date DESC`
  ).all(...params);

  const totals = rows.reduce(
    (acc, row) => {
      acc.total += Number(row.credit_amount || 0);
      acc.paid += Number(row.paid_amount || 0);
      acc.remaining += Number(row.remaining_amount || 0);
      if (row.source === "export") acc.exportTotal += Number(row.credit_amount || 0);
      if (row.source === "customer") acc.customerTotal += Number(row.credit_amount || 0);
      return acc;
    },
    { total: 0, paid: 0, remaining: 0, exportTotal: 0, customerTotal: 0 }
  );

  return { rows, totals };
};

router.use(requireRole(["SUPER_ADMIN", "ADMIN", "WORKER"]));
router.use((req, res, next) => {
  if (req.method !== "POST") return next();
  const currentUser = res.locals.currentUser;
  if (!currentUser || currentUser.role !== "WORKER") return next();
  const lockedDate = getClosedDateForMutation(req);
  if (!lockedDate) return next();
  const closure = db.prepare("SELECT is_closed FROM day_closures WHERE closure_date = ?").get(lockedDate);
  if (!closure || Number(closure.is_closed) !== 1) return next();
  return res.status(423).render("day_closed", {
    title: req.t("dayClosedTitle"),
    closureDate: lockedDate,
    backUrl: req.get("Referrer") || "/records/exports"
  });
});

router.get("/alerts", (req, res) => {
  const alertData = getWorkerAlertData();
  res.render("records/alerts", {
    title: req.t("alertCenterTitle"),
    ...alertData
  });
});

router.get("/history", (req, res) => {
  const today = dayjs().format("YYYY-MM-DD");
  const from = /^\d{4}-\d{2}-\d{2}$/.test(String(req.query.from || ""))
    ? String(req.query.from)
    : dayjs().subtract(30, "day").format("YYYY-MM-DD");
  const to = /^\d{4}-\d{2}-\d{2}$/.test(String(req.query.to || ""))
    ? String(req.query.to)
    : today;
  const action = String(req.query.action || "all");
  const entity = String(req.query.entity || "all");
  const q = String(req.query.q || "").trim();

  const params = [from, to];
  const clauses = [];
  if (action !== "all") {
    clauses.push("activity_logs.action = ?");
    params.push(action);
  }
  if (entity !== "all") {
    clauses.push("activity_logs.entity_type = ?");
    params.push(entity);
  }
  if (q) {
    clauses.push("(COALESCE(users.full_name, '') LIKE ? OR COALESCE(activity_logs.details, '') LIKE ?)");
    const like = `%${q}%`;
    params.push(like, like);
  }
  const extraWhere = clauses.length ? ` AND ${clauses.join(" AND ")}` : "";
  const rows = db.prepare(
    `SELECT activity_logs.action, activity_logs.entity_type, activity_logs.details, activity_logs.created_at,
            users.full_name
     FROM activity_logs
     LEFT JOIN users ON users.id = activity_logs.user_id
     WHERE date(activity_logs.created_at) BETWEEN ? AND ?
     ${extraWhere}
     ORDER BY activity_logs.created_at DESC
     LIMIT 500`
  ).all(...params);

  const formattedRows = formatActivityRows(rows, req.t);
  const actionOptions = ["all", "create", "update", "delete", "payment", "backup", "restore"];
  const entityOptions = [
    "all",
    "export",
    "credit",
    "jar_sale",
    "vehicle",
    "worker",
    "admin",
    "staff",
    "staff_salary",
    "import_entry",
    "vehicle_savings",
    "stock_ledger",
    "system"
  ];

  res.render("records/history", {
    title: req.t("activityLogTitle"),
    from,
    to,
    action,
    entity,
    q,
    actionOptions,
    entityOptions,
    rows: formattedRows
  });
});

router.get("/vehicles", (req, res) => {
  const includeInactive = String(req.query.include_inactive || "1") === "1";
  const where = includeInactive ? "" : "WHERE COALESCE(is_active, 1) = 1";
  const vehicles = db.prepare(
    `SELECT *
     FROM vehicles
     ${where}
     ORDER BY COALESCE(is_active, 1) DESC, vehicle_number ASC`
  ).all();
  const success = req.query.archived
    ? req.t("vehicleArchived")
    : req.query.unarchived || req.query.activated
      ? req.t("vehicleActivated")
      : null;
  res.render("admin/vehicles", {
    title: req.t("vehicles"),
    vehicles,
    includeInactive,
    success,
    vehicleRouteBase: "/records/vehicles"
  });
});

router.get("/vehicles/new", (req, res) => {
  res.render("admin/vehicle_form", {
    title: req.t("addVehicleTitle"),
    vehicle: null,
    error: null,
    vehicleRouteBase: "/records/vehicles"
  });
});

router.post("/vehicles", vehicleUpload.single("profile_pic"), (req, res) => {
  const { vehicle_number, owner_name, phone, is_company } = req.body;
  if (!vehicle_number || !owner_name) {
    return res.render("admin/vehicle_form", {
      title: req.t("addVehicleTitle"),
      vehicle: null,
      error: req.t("vehicleRequired"),
      vehicleRouteBase: "/records/vehicles"
    });
  }

  const profilePath = req.file ? `/uploads/${req.file.filename}` : null;
  const companyFlag = is_company === "on" ? 1 : 0;

  try {
    const result = db.prepare(
      "INSERT INTO vehicles (vehicle_number, owner_name, phone, is_company, profile_pic_path) VALUES (?, ?, ?, ?, ?)"
    ).run(vehicle_number.trim(), owner_name.trim(), phone ? phone.trim() : null, companyFlag, profilePath);
    logActivity({
      userId: req.session.userId,
      action: "create",
      entityType: "vehicle",
      entityId: result.lastInsertRowid,
      details: `vehicle_number=${vehicle_number.trim()}, owner=${owner_name.trim()}, company=${companyFlag}`
    });
    return res.redirect("/records/vehicles");
  } catch (err) {
    return res.render("admin/vehicle_form", {
      title: req.t("addVehicleTitle"),
      vehicle: null,
      error: req.t("vehicleExists"),
      vehicleRouteBase: "/records/vehicles"
    });
  }
});

router.get("/vehicles/:id/edit", (req, res) => {
  const vehicle = db.prepare("SELECT * FROM vehicles WHERE id = ?").get(req.params.id);
  if (!vehicle) return res.redirect("/records/vehicles");
  res.render("admin/vehicle_form", {
    title: req.t("editVehicleTitle"),
    vehicle,
    error: null,
    vehicleRouteBase: "/records/vehicles"
  });
});

router.post("/vehicles/:id", vehicleUpload.single("profile_pic"), (req, res) => {
  const vehicle = db.prepare("SELECT * FROM vehicles WHERE id = ?").get(req.params.id);
  if (!vehicle) return res.redirect("/records/vehicles");

  const { vehicle_number, owner_name, phone, is_company } = req.body;
  if (!vehicle_number || !owner_name) {
    return res.render("admin/vehicle_form", {
      title: req.t("editVehicleTitle"),
      vehicle,
      error: req.t("vehicleRequired"),
      vehicleRouteBase: "/records/vehicles"
    });
  }

  const profilePath = req.file ? `/uploads/${req.file.filename}` : vehicle.profile_pic_path;
  const companyFlag = is_company === "on" ? 1 : 0;

  try {
    db.prepare(
      "UPDATE vehicles SET vehicle_number = ?, owner_name = ?, phone = ?, is_company = ?, profile_pic_path = ?, updated_at = datetime('now') WHERE id = ?"
    ).run(vehicle_number.trim(), owner_name.trim(), phone ? phone.trim() : null, companyFlag, profilePath, req.params.id);
    logActivity({
      userId: req.session.userId,
      action: "update",
      entityType: "vehicle",
      entityId: req.params.id,
      details: `vehicle_number=${vehicle_number.trim()}, owner=${owner_name.trim()}, company=${companyFlag}`
    });
    return res.redirect("/records/vehicles");
  } catch (err) {
    return res.render("admin/vehicle_form", {
      title: req.t("editVehicleTitle"),
      vehicle,
      error: req.t("vehicleExists"),
      vehicleRouteBase: "/records/vehicles"
    });
  }
});

router.post("/vehicles/:id/archive", (req, res) => {
  const vehicle = db.prepare("SELECT id, vehicle_number, is_active FROM vehicles WHERE id = ?").get(req.params.id);
  if (!vehicle) return res.redirect("/records/vehicles");
  setVehicleActiveStatus(req.params.id, false, req.session.userId);
  logActivity({
    userId: req.session.userId,
    action: "update",
    entityType: "vehicle",
    entityId: req.params.id,
    details: `status=archived, vehicle_number=${vehicle.vehicle_number || ""}`
  });
  return res.redirect("/records/vehicles?archived=1&include_inactive=1");
});

router.post("/vehicles/:id/activate", (req, res) => {
  const vehicle = db.prepare("SELECT id, vehicle_number FROM vehicles WHERE id = ?").get(req.params.id);
  if (!vehicle) return res.redirect("/records/vehicles");
  setVehicleActiveStatus(req.params.id, true, req.session.userId);
  logActivity({
    userId: req.session.userId,
    action: "update",
    entityType: "vehicle",
    entityId: req.params.id,
    details: `status=active, vehicle_number=${vehicle.vehicle_number || ""}`
  });
  return res.redirect("/records/vehicles?activated=1&include_inactive=1");
});

router.post("/vehicles/:id/delete", (req, res) => {
  return res.redirect(307, `/records/vehicles/${req.params.id}/archive`);
});

router.get("/water-tests", (req, res) => {
  const from = req.query.from || dayjs().startOf("month").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const q = String(req.query.q || "").trim();
  const searchClause = q ? "AND (COALESCE(water_test_reports.coliform, '') LIKE ? OR COALESCE(water_test_reports.note, '') LIKE ?)" : "";
  const params = q ? [from, to, `%${q}%`, `%${q}%`] : [from, to];
  const rows = db.prepare(
    `SELECT water_test_reports.*, users.full_name as recorded_by
     FROM water_test_reports
     LEFT JOIN users ON water_test_reports.created_by = users.id
     WHERE water_test_reports.test_date BETWEEN ? AND ?
     ${searchClause}
     ORDER BY water_test_reports.test_date DESC, water_test_reports.created_at DESC`
  ).all(...params);

  res.render("records/water_tests", {
    title: req.t("waterTestsTitle"),
    from,
    to,
    q,
    rows
  });
});

router.get("/water-tests/new", (req, res) => {
  res.render("records/water_test_form", {
    title: req.t("addWaterTestTitle"),
    record: null,
    error: null,
    defaultDate: dayjs().format("YYYY-MM-DD")
  });
});

router.post(
  "/water-tests",
  waterReportUpload.fields([
    { name: "forensic_report", maxCount: 1 },
    { name: "government_report", maxCount: 1 }
  ]),
  (req, res) => {
    const { test_date, ph_value, tds_value, coliform, note } = req.body;
    const phValue = Number(ph_value || 0);
    const tdsValue = Number(tds_value || 0);
    if (!test_date || Number.isNaN(phValue) || Number.isNaN(tdsValue)) {
      return res.render("records/water_test_form", {
        title: req.t("addWaterTestTitle"),
        record: null,
        error: req.t("waterTestRequired"),
        defaultDate: test_date || dayjs().format("YYYY-MM-DD")
      });
    }
    const forensicFile = req.files && req.files.forensic_report ? req.files.forensic_report[0] : null;
    const governmentFile = req.files && req.files.government_report ? req.files.government_report[0] : null;
    const result = db.prepare(
      `INSERT INTO water_test_reports
      (test_date, ph_value, tds_value, coliform, forensic_report_path, forensic_report_name, government_report_path, government_report_name, note, created_by)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    ).run(
      test_date,
      phValue,
      tdsValue,
      coliform ? String(coliform).trim() : null,
      forensicFile ? `/uploads/${forensicFile.filename}` : null,
      forensicFile ? forensicFile.originalname : null,
      governmentFile ? `/uploads/${governmentFile.filename}` : null,
      governmentFile ? governmentFile.originalname : null,
      note ? String(note).trim() : null,
      req.session.userId
    );
    logActivity({
      userId: req.session.userId,
      action: "create",
      entityType: "water_test_report",
      entityId: result.lastInsertRowid,
      details: `test_date=${test_date}; ph=${phValue}; tds=${tdsValue}; coliform=${coliform || ""}`
    });
    return res.redirect(`/records/water-tests?from=${test_date}&to=${test_date}`);
  }
);

router.get("/water-tests/:id/edit", (req, res) => {
  const record = db.prepare("SELECT * FROM water_test_reports WHERE id = ?").get(req.params.id);
  if (!record) return res.redirect("/records/water-tests");
  res.render("records/water_test_form", {
    title: req.t("editWaterTestTitle"),
    record,
    error: null,
    defaultDate: record.test_date
  });
});

router.post(
  "/water-tests/:id",
  waterReportUpload.fields([
    { name: "forensic_report", maxCount: 1 },
    { name: "government_report", maxCount: 1 }
  ]),
  (req, res) => {
    const existing = db.prepare("SELECT * FROM water_test_reports WHERE id = ?").get(req.params.id);
    if (!existing) return res.redirect("/records/water-tests");
    const { test_date, ph_value, tds_value, coliform, note, remove_forensic, remove_government } = req.body;
    const phValue = Number(ph_value || 0);
    const tdsValue = Number(tds_value || 0);
    if (!test_date || Number.isNaN(phValue) || Number.isNaN(tdsValue)) {
      return res.render("records/water_test_form", {
        title: req.t("editWaterTestTitle"),
        record: existing,
        error: req.t("waterTestRequired"),
        defaultDate: test_date || existing.test_date
      });
    }
    const forensicFile = req.files && req.files.forensic_report ? req.files.forensic_report[0] : null;
    const governmentFile = req.files && req.files.government_report ? req.files.government_report[0] : null;
    let forensicPath = existing.forensic_report_path || null;
    let forensicName = existing.forensic_report_name || null;
    let governmentPath = existing.government_report_path || null;
    let governmentName = existing.government_report_name || null;
    if (remove_forensic === "on") {
      forensicPath = null;
      forensicName = null;
    }
    if (remove_government === "on") {
      governmentPath = null;
      governmentName = null;
    }
    if (forensicFile) {
      forensicPath = `/uploads/${forensicFile.filename}`;
      forensicName = forensicFile.originalname;
    }
    if (governmentFile) {
      governmentPath = `/uploads/${governmentFile.filename}`;
      governmentName = governmentFile.originalname;
    }
    db.prepare(
      `UPDATE water_test_reports
       SET test_date = ?, ph_value = ?, tds_value = ?, coliform = ?, forensic_report_path = ?, forensic_report_name = ?,
           government_report_path = ?, government_report_name = ?, note = ?, updated_at = datetime('now')
       WHERE id = ?`
    ).run(
      test_date,
      phValue,
      tdsValue,
      coliform ? String(coliform).trim() : null,
      forensicPath,
      forensicName,
      governmentPath,
      governmentName,
      note ? String(note).trim() : null,
      req.params.id
    );
    logActivity({
      userId: req.session.userId,
      action: "update",
      entityType: "water_test_report",
      entityId: req.params.id,
      details: `test_date=${test_date}; ph=${phValue}; tds=${tdsValue}; coliform=${coliform || ""}`
    });
    return res.redirect(`/records/water-tests?from=${test_date}&to=${test_date}`);
  }
);

router.post("/water-tests/:id/delete", (req, res) => {
  const record = db.prepare("SELECT id, test_date FROM water_test_reports WHERE id = ?").get(req.params.id);
  if (!record) return res.redirect("/records/water-tests");
  db.prepare("DELETE FROM water_test_reports WHERE id = ?").run(req.params.id);
  logActivity({
    userId: req.session.userId,
    action: "delete",
    entityType: "water_test_report",
    entityId: req.params.id,
    details: `test_date=${record.test_date || ""}`
  });
  return res.redirect("/records/water-tests");
});

router.get("/exports", (req, res) => {
  const from = req.query.from || dayjs().subtract(7, "day").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const vehicleCreditFrom = req.query.vehicle_credit_from || from;
  const vehicleCreditTo = req.query.vehicle_credit_to || to;
  const q = (req.query.q || "").trim();
  const status = String(req.query.status || "");
  const errorKey = String(req.query.error || "");
  const success = status === "credit_paid"
    ? req.t("exportCreditPaymentSaved")
    : status === "day_credit_paid"
      ? req.t("dayCreditPaymentSaved")
      : status === "vehicle_cumulative_paid"
        ? req.t("vehicleCumulativePaymentSaved")
      : null;
  const error = errorKey ? req.t(errorKey) : null;
  const tripVehicleRaw = Number(req.query.trip_vehicle_id || 0);
  const tripVehicleId = Number.isInteger(tripVehicleRaw) && tripVehicleRaw > 0 ? tripVehicleRaw : null;
  const sortRaw = req.query.sort || "date_desc";
  const sortMap = {
    date_desc: "export_date DESC, exports.created_at DESC",
    date_asc: "export_date ASC, exports.created_at ASC",
    total_desc: "exports.total_amount DESC",
    total_asc: "exports.total_amount ASC",
    jars_desc: "exports.jar_count DESC",
    paid_desc: "exports.paid_amount DESC",
    credit_desc: "exports.credit_amount DESC"
  };
  const sort = sortMap[sortRaw] ? sortRaw : "date_desc";
  const orderBy = sortMap[sort];
  const searchClause = q ? "AND (vehicles.vehicle_number LIKE ? OR vehicles.owner_name LIKE ?)" : "";
  const vehicleFilterClause = tripVehicleId ? "AND exports.vehicle_id = ?" : "";
  const params = [from, to];
  if (tripVehicleId) params.push(tripVehicleId);
  if (q) params.push(`%${q}%`, `%${q}%`);
  const exportsRows = db.prepare(
    `SELECT exports.*, vehicles.vehicle_number, vehicles.owner_name, vehicles.is_company,
            users.full_name as recorded_by,
            COALESCE(NULLIF(TRIM(exports.checked_by_staff_name), ''), checked_staff.full_name) as checked_by_name
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     LEFT JOIN users ON exports.created_by = users.id
     LEFT JOIN staff as checked_staff ON exports.checked_by_staff_id = checked_staff.id
     WHERE export_date BETWEEN ? AND ?
     ${vehicleFilterClause}
     ${searchClause}
     ORDER BY ${orderBy}`
  ).all(...params);

  const exportTotals = db.prepare(
    `SELECT
        COUNT(exports.id) AS total_trips,
        COALESCE(SUM(exports.jar_count), 0) AS total_jars,
        COALESCE(SUM(exports.bottle_case_count), 0) AS total_bottle_cases,
        COALESCE(SUM(exports.dispenser_count), 0) AS total_dispensers,
        COALESCE(SUM(exports.dispenser_count * exports.dispenser_unit_price), 0) AS total_dispenser_amount,
        COALESCE(SUM(exports.return_jar_count), 0) AS total_return_jars,
        COALESCE(SUM(exports.return_bottle_case_count), 0) AS total_return_bottles,
        COALESCE(SUM(exports.leakage_jar_count), 0) AS total_leakage_jars,
        COALESCE(SUM(exports.sold_jar_count), 0) AS total_sold_jars,
        COALESCE(SUM(exports.sold_jar_amount), 0) AS total_sold_jar_amount,
        COALESCE(SUM(exports.collection_amount), 0) AS total_collection_amount,
        COALESCE(SUM(exports.expense_amount), 0) AS total_expense_amount,
        COALESCE(SUM(exports.total_amount), 0) AS total_amount,
        COALESCE(SUM(exports.paid_amount), 0) AS total_paid,
        COALESCE(SUM(exports.credit_amount), 0) AS total_credit
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     WHERE export_date BETWEEN ? AND ?
     ${vehicleFilterClause}
     ${searchClause}`
  ).get(...params);
  const rangePaidByMethod = db.prepare(
    `SELECT
        COALESCE(SUM(exports.paid_cash_amount), 0) AS cash_paid,
        COALESCE(SUM(exports.paid_bank_amount), 0) AS bank_paid,
        COALESCE(SUM(exports.paid_ewallet_amount), 0) AS ewallet_paid
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     WHERE export_date BETWEEN ? AND ?
     ${vehicleFilterClause}
     ${searchClause}`
  ).get(...params);

  const today = dayjs().format("YYYY-MM-DD");
  const todayTotals = db.prepare(
    `SELECT
        COUNT(id) AS total_trips,
        COALESCE(SUM(jar_count), 0) AS total_jars,
        COALESCE(SUM(bottle_case_count), 0) AS total_bottle_cases,
        COALESCE(SUM(dispenser_count), 0) AS total_dispensers,
        COALESCE(SUM(dispenser_count * dispenser_unit_price), 0) AS total_dispenser_amount,
        COALESCE(SUM(return_jar_count), 0) AS total_return_jars,
        COALESCE(SUM(return_bottle_case_count), 0) AS total_return_bottles,
        COALESCE(SUM(leakage_jar_count), 0) AS total_leakage_jars,
        COALESCE(SUM(sold_jar_count), 0) AS total_sold_jars,
        COALESCE(SUM(sold_jar_amount), 0) AS total_sold_jar_amount,
        COALESCE(SUM(collection_amount), 0) AS total_collection_amount,
        COALESCE(SUM(expense_amount), 0) AS total_expense_amount,
        COALESCE(SUM(total_amount), 0) AS total_amount,
        COALESCE(SUM(paid_amount), 0) AS total_paid,
        COALESCE(SUM(credit_amount), 0) AS total_credit
     FROM exports
     WHERE export_date = ?`
  ).get(today);
  const todayPaidByMethod = db.prepare(
    `SELECT
        COALESCE(SUM(paid_cash_amount), 0) AS cash_paid,
        COALESCE(SUM(paid_bank_amount), 0) AS bank_paid,
        COALESCE(SUM(paid_ewallet_amount), 0) AS ewallet_paid
     FROM exports
     WHERE export_date = ?`
  ).get(today);
  const monthStart = dayjs().startOf("month").format("YYYY-MM-DD");
  const yearStart = dayjs().startOf("year").format("YYYY-MM-DD");
  const topToday = db.prepare(
    `SELECT vehicles.vehicle_number, vehicles.owner_name,
            COALESCE(SUM(exports.total_amount), 0) as total,
            COALESCE(SUM(exports.jar_count), 0) as jars,
            COALESCE(SUM(exports.bottle_case_count), 0) as bottles
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     WHERE export_date = ?
     GROUP BY exports.vehicle_id
     ORDER BY total DESC
     LIMIT 1`
  ).get(today);
  const topMonth = db.prepare(
    `SELECT vehicles.vehicle_number, vehicles.owner_name,
            COALESCE(SUM(exports.total_amount), 0) as total,
            COALESCE(SUM(exports.jar_count), 0) as jars,
            COALESCE(SUM(exports.bottle_case_count), 0) as bottles
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     WHERE export_date BETWEEN ? AND ?
     GROUP BY exports.vehicle_id
     ORDER BY total DESC
     LIMIT 1`
  ).get(monthStart, today);
  const weekStart = dayjs().startOf("week").format("YYYY-MM-DD");
  const tripVehicles = db.prepare(
    "SELECT id, vehicle_number, owner_name, is_company FROM vehicles ORDER BY vehicle_number"
  ).all();

  const cumulativeVehicleSearchClause = q ? "AND (vehicles.vehicle_number LIKE ? OR vehicles.owner_name LIKE ?)" : "";
  const cumulativeVehicleParams = q
    ? [vehicleCreditFrom, vehicleCreditTo, `%${q}%`, `%${q}%`]
    : [vehicleCreditFrom, vehicleCreditTo];
  const vehicleCumulativeCredits = db.prepare(
    `SELECT exports.vehicle_id,
            vehicles.vehicle_number,
            vehicles.owner_name,
            COUNT(exports.id) AS trip_count,
            MAX(exports.export_date) AS last_export_date,
            ROUND(COALESCE(SUM(exports.total_amount), 0), 2) AS total_amount,
            ROUND(COALESCE(SUM(exports.paid_amount), 0), 2) AS total_paid,
            ROUND(COALESCE(SUM(CASE
              WHEN (exports.total_amount - exports.paid_amount) > 0 THEN (exports.total_amount - exports.paid_amount)
              ELSE 0
            END), 0), 2) AS total_remaining
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     WHERE vehicles.is_company = 0
       AND exports.export_date BETWEEN ? AND ?
     ${cumulativeVehicleSearchClause}
     GROUP BY exports.vehicle_id, vehicles.vehicle_number, vehicles.owner_name
     HAVING total_remaining > 0
     ORDER BY total_remaining DESC, vehicles.vehicle_number ASC`
  ).all(...cumulativeVehicleParams);
  const vehicleCumulativeTotals = vehicleCumulativeCredits.reduce(
    (acc, row) => {
      acc.vehicle_count += 1;
      acc.total_amount = parseMoneyValue(acc.total_amount + Number(row.total_amount || 0));
      acc.total_paid = parseMoneyValue(acc.total_paid + Number(row.total_paid || 0));
      acc.total_remaining = parseMoneyValue(acc.total_remaining + Number(row.total_remaining || 0));
      return acc;
    },
    { vehicle_count: 0, total_amount: 0, total_paid: 0, total_remaining: 0 }
  );
  const vehicleCumulativeAllTime = db.prepare(
    `SELECT ROUND(COALESCE(SUM(CASE
      WHEN (exports.total_amount - exports.paid_amount) > 0 THEN (exports.total_amount - exports.paid_amount)
      ELSE 0
    END), 0), 2) AS total_remaining
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     WHERE vehicles.is_company = 0
     ${cumulativeVehicleSearchClause}`
  ).get(...(q ? [`%${q}%`, `%${q}%`] : []));

  let dailyCreditGroups = [];
  if (from === to) {
    const dailyCreditTrips = db.prepare(
      `SELECT exports.id, exports.export_date, exports.receipt_no, exports.total_amount, exports.paid_amount,
              exports.vehicle_id, vehicles.vehicle_number, vehicles.owner_name
       FROM exports
       JOIN vehicles ON exports.vehicle_id = vehicles.id
       WHERE exports.export_date = ?
         AND vehicles.is_company = 0
         AND (exports.total_amount - exports.paid_amount) > 0
       ORDER BY vehicles.vehicle_number ASC, exports.id ASC`
    ).all(from);

    const grouped = new Map();
    dailyCreditTrips.forEach((row) => {
      const remaining = computeRemainingMoney(row.total_amount || 0, row.paid_amount || 0);
      if (remaining <= 0) return;
      const key = String(row.vehicle_id);
      if (!grouped.has(key)) {
        grouped.set(key, {
          vehicle_id: row.vehicle_id,
          vehicle_number: row.vehicle_number,
          owner_name: row.owner_name,
          total_remaining: 0,
          trips: []
        });
      }
      const entry = grouped.get(key);
      entry.total_remaining = parseMoneyValue(entry.total_remaining + remaining);
      entry.trips.push({
        id: row.id,
        receipt_no: row.receipt_no || `#${row.id}`,
        total_amount: Number(row.total_amount || 0),
        paid_amount: Number(row.paid_amount || 0),
        remaining
      });
    });
    dailyCreditGroups = Array.from(grouped.values())
      .filter((row) => row.total_remaining > 0)
      .sort((a, b) => a.vehicle_number.localeCompare(b.vehicle_number));
  }

  res.render("records/exports", {
    title: req.t("exportsTitle"),
    from,
    to,
    q,
    sort,
    success,
    error,
    exportsRows,
    exportTotals,
    rangePaidByMethod,
    todayTotals,
    todayPaidByMethod,
    topToday,
    topMonth,
    today,
    monthStart,
    weekStart,
    yearStart,
    tripVehicles,
    tripVehicleId,
    vehicleCumulativeCredits,
    vehicleCumulativeTotals,
    vehicleCumulativeAllTime: Number(vehicleCumulativeAllTime?.total_remaining || 0),
    vehicleCreditFrom,
    vehicleCreditTo,
    dailyCreditGroups
  });
});

router.get("/exports/daily-credit", (req, res) => {
  const date = req.query.date || dayjs().format("YYYY-MM-DD");
  const vehicleId = Number(req.query.vehicle_id || 0);
  if (!vehicleId) {
    return res.redirect(`/records/exports?from=${date}&to=${date}&error=dayCreditSelectVehicle`);
  }
  const vehicle = db.prepare(
    "SELECT id, vehicle_number, owner_name, is_company FROM vehicles WHERE id = ?"
  ).get(vehicleId);
  if (!vehicle || Number(vehicle.is_company) === 1) {
    return res.redirect(`/records/exports?from=${date}&to=${date}&error=companyVehicleNoCredit`);
  }

  const trips = db.prepare(
    `SELECT exports.id, exports.receipt_no, exports.total_amount, exports.paid_amount
     FROM exports
     WHERE exports.export_date = ?
       AND exports.vehicle_id = ?
       AND (exports.total_amount - exports.paid_amount) > 0
     ORDER BY exports.id ASC`
  ).all(date, vehicleId).map((row) => ({
    ...row,
    receipt_no: row.receipt_no || `#${row.id}`,
    total_amount: parseMoneyValue(row.total_amount || 0),
    paid_amount: parseMoneyValue(row.paid_amount || 0),
    remaining: computeRemainingMoney(row.total_amount || 0, row.paid_amount || 0)
  }));

  if (!trips.length) {
    return res.redirect(`/records/exports?from=${date}&to=${date}&error=dayCreditNoRemaining`);
  }
  const totalRemaining = trips.reduce((sum, row) => parseMoneyValue(sum + row.remaining), 0);

  res.render("records/export_daily_credit", {
    title: req.t("dayCreditTitle"),
    date,
    vehicle,
    trips,
    totalRemaining,
    error: null
  });
});

router.post("/exports/daily-credit", (req, res) => {
  const date = req.body.date || dayjs().format("YYYY-MM-DD");
  const vehicleId = Number(req.body.vehicle_id || 0);
  if (!vehicleId) {
    return res.redirect(`/records/exports?from=${date}&to=${date}&error=dayCreditSelectVehicle`);
  }
  const vehicle = db.prepare(
    "SELECT id, vehicle_number, owner_name, is_company FROM vehicles WHERE id = ?"
  ).get(vehicleId);
  if (!vehicle || Number(vehicle.is_company) === 1) {
    return res.redirect(`/records/exports?from=${date}&to=${date}&error=companyVehicleNoCredit`);
  }
  const trips = db.prepare(
    `SELECT exports.id, exports.total_amount, exports.paid_amount,
            exports.paid_cash_amount, exports.paid_bank_amount, exports.paid_ewallet_amount,
            exports.receipt_no
     FROM exports
     WHERE exports.export_date = ?
       AND exports.vehicle_id = ?
       AND (exports.total_amount - exports.paid_amount) > 0
     ORDER BY exports.id ASC`
  ).all(date, vehicleId).map((row) => ({
    ...row,
    total_amount: parseMoneyValue(row.total_amount || 0),
    paid_amount: parseMoneyValue(row.paid_amount || 0),
    paid_cash_amount: parseMoneyValue(row.paid_cash_amount || 0),
    paid_bank_amount: parseMoneyValue(row.paid_bank_amount || 0),
    paid_ewallet_amount: parseMoneyValue(row.paid_ewallet_amount || 0),
    remaining: computeRemainingMoney(row.total_amount || 0, row.paid_amount || 0)
  }));
  const totalRemaining = trips.reduce((sum, row) => parseMoneyValue(sum + row.remaining), 0);
  if (!trips.length || totalRemaining <= 0) {
    return res.redirect(`/records/exports?from=${date}&to=${date}&error=dayCreditNoRemaining`);
  }

  const paymentParsed = parsePaymentBreakdownFromBody(req.body, {
    maxTotal: totalRemaining
  });
  if (paymentParsed.total <= 0) {
    return res.render("records/export_daily_credit", {
      title: req.t("dayCreditTitle"),
      date,
      vehicle,
      trips,
      totalRemaining,
      error: req.t("dayCreditPaymentInvalid")
    });
  }

  const remainingByMethod = {
    cash: parseMoneyValue(paymentParsed.breakdown.cash || 0),
    bank: parseMoneyValue(paymentParsed.breakdown.bank || 0),
    eWallet: parseMoneyValue(paymentParsed.breakdown.eWallet || 0)
  };
  const appliedByMethod = { cash: 0, bank: 0, eWallet: 0 };
  db.exec("BEGIN;");
  try {
    trips.forEach((trip) => {
      const remainingOnTrip = computeRemainingMoney(trip.total_amount || 0, trip.paid_amount || 0);
      if (remainingOnTrip <= 0) return;

      const shareByMethod = { cash: 0, bank: 0, eWallet: 0 };
      let applied = 0;
      paymentBreakdownOrder.forEach((key) => {
        const methodRemaining = parseMoneyValue(remainingByMethod[key] || 0);
        if (methodRemaining <= 0) return;
        const availableOnTrip = parseMoneyValue(remainingOnTrip - applied);
        if (availableOnTrip <= 0) return;
        const share = parseMoneyValue(Math.min(methodRemaining, availableOnTrip));
        if (share <= 0) return;
        shareByMethod[key] = share;
        remainingByMethod[key] = parseMoneyValue(methodRemaining - share);
        appliedByMethod[key] = parseMoneyValue(appliedByMethod[key] + share);
        applied = parseMoneyValue(applied + share);
      });
      if (applied <= 0) return;

      const nextPaidCash = parseMoneyValue(trip.paid_cash_amount + shareByMethod.cash);
      const nextPaidBank = parseMoneyValue(trip.paid_bank_amount + shareByMethod.bank);
      const nextPaidEWallet = parseMoneyValue(trip.paid_ewallet_amount + shareByMethod.eWallet);
      const newPaid = parseMoneyValue(Math.min(trip.total_amount, trip.paid_amount + applied));
      const newCredit = computeRemainingMoney(trip.total_amount, newPaid);
      const methodForTrip = getPaymentMethodFromBreakdown(
        { cash: nextPaidCash, bank: nextPaidBank, eWallet: nextPaidEWallet },
        paymentParsed.primaryMethod,
        true
      );
      db.prepare(
        "UPDATE exports SET paid_amount = ?, paid_cash_amount = ?, paid_bank_amount = ?, paid_ewallet_amount = ?, credit_amount = ?, payment_method = ? WHERE id = ?"
      ).run(
        newPaid,
        nextPaidCash,
        nextPaidBank,
        nextPaidEWallet,
        newCredit,
        methodForTrip,
        trip.id
      );
    });
    db.exec("COMMIT;");
  } catch (err) {
    db.exec("ROLLBACK;");
    throw err;
  }

  const appliedTotal = sumPaymentBreakdown(appliedByMethod);
  const appliedMethodLabel = getPaymentMethodFromBreakdown(appliedByMethod, paymentParsed.primaryMethod, true);
  logActivity({
    userId: req.session.userId,
    action: "payment",
    entityType: "export_day_credit",
    entityId: `${vehicleId}:${date}`,
    details: `date=${date}, vehicle_id=${vehicleId}, payment=${appliedTotal}, method=${appliedMethodLabel}, cash=${appliedByMethod.cash}, bank=${appliedByMethod.bank}, ewallet=${appliedByMethod.eWallet}`
  });

  return res.redirect(`/records/exports?from=${date}&to=${date}&status=day_credit_paid`);
});

router.post("/exports/vehicle-credits/pay", (req, res) => {
  const vehicleId = parseOptionalId(req.body.vehicle_id);
  const vehicleCreditFrom = String(req.body.vehicle_credit_from || req.body.from || "").trim();
  const vehicleCreditTo = String(req.body.vehicle_credit_to || req.body.to || "").trim();
  const hasVehicleCreditRange = Boolean(vehicleCreditFrom && vehicleCreditTo);
  if (!vehicleId) {
    return res.redirect(buildExportsListUrl({ ...req.body, error: "vehicleCumulativePaymentInvalid" }));
  }

  const vehicle = db.prepare(
    "SELECT id, vehicle_number, owner_name, is_company FROM vehicles WHERE id = ?"
  ).get(vehicleId);
  if (!vehicle || Number(vehicle.is_company) === 1) {
    return res.redirect(buildExportsListUrl({ ...req.body, error: "vehicleCumulativeNoOutstanding" }));
  }

  const rangeClause = hasVehicleCreditRange ? "AND exports.export_date BETWEEN ? AND ?" : "";
  const rowParams = hasVehicleCreditRange ? [vehicleId, vehicleCreditFrom, vehicleCreditTo] : [vehicleId];
  const rows = db.prepare(
    `SELECT exports.id, exports.total_amount, exports.paid_amount, exports.credit_amount,
            exports.paid_cash_amount, exports.paid_bank_amount, exports.paid_ewallet_amount
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     WHERE exports.vehicle_id = ?
       AND vehicles.is_company = 0
       AND (exports.total_amount - exports.paid_amount) > 0
      ${rangeClause}
     ORDER BY exports.export_date ASC, exports.id ASC`
  ).all(...rowParams);
  if (!rows.length) {
    return res.redirect(buildExportsListUrl({ ...req.body, error: "vehicleCumulativeNoOutstanding" }));
  }

  const totalRemaining = rows.reduce((sum, row) => {
    const remaining = computeRemainingMoney(row.total_amount || 0, row.paid_amount || 0);
    return parseMoneyValue(sum + remaining);
  }, 0);
  const paymentParsed = parsePaymentBreakdownFromBody(req.body, {
    maxTotal: totalRemaining
  });
  if (paymentParsed.total <= 0) {
    return res.redirect(buildExportsListUrl({ ...req.body, error: "vehicleCumulativePaymentInvalid" }));
  }

  const remainingByMethod = {
    cash: parseMoneyValue(paymentParsed.breakdown.cash || 0),
    bank: parseMoneyValue(paymentParsed.breakdown.bank || 0),
    eWallet: parseMoneyValue(paymentParsed.breakdown.eWallet || 0)
  };
  const appliedByMethod = { cash: 0, bank: 0, eWallet: 0 };
  let tripCount = 0;
  db.exec("BEGIN;");
  try {
    rows.forEach((row) => {
      const total = parseMoneyValue(row.total_amount || 0);
      const paid = parseMoneyValue(row.paid_amount || 0);
      const paidCash = parseMoneyValue(row.paid_cash_amount || 0);
      const paidBank = parseMoneyValue(row.paid_bank_amount || 0);
      const paidEWallet = parseMoneyValue(row.paid_ewallet_amount || 0);
      const remaining = computeRemainingMoney(total, paid);
      if (remaining <= 0) return;

      const shareByMethod = { cash: 0, bank: 0, eWallet: 0 };
      let share = 0;
      paymentBreakdownOrder.forEach((key) => {
        const methodRemaining = parseMoneyValue(remainingByMethod[key] || 0);
        if (methodRemaining <= 0) return;
        const availableOnRow = parseMoneyValue(remaining - share);
        if (availableOnRow <= 0) return;
        const use = parseMoneyValue(Math.min(methodRemaining, availableOnRow));
        if (use <= 0) return;
        shareByMethod[key] = use;
        remainingByMethod[key] = parseMoneyValue(methodRemaining - use);
        appliedByMethod[key] = parseMoneyValue(appliedByMethod[key] + use);
        share = parseMoneyValue(share + use);
      });
      if (share <= 0) return;

      const nextPaidCash = parseMoneyValue(paidCash + shareByMethod.cash);
      const nextPaidBank = parseMoneyValue(paidBank + shareByMethod.bank);
      const nextPaidEWallet = parseMoneyValue(paidEWallet + shareByMethod.eWallet);
      const newPaid = parseMoneyValue(Math.min(total, paid + share));
      const newCredit = computeRemainingMoney(total, newPaid);
      const methodForRow = getPaymentMethodFromBreakdown(
        { cash: nextPaidCash, bank: nextPaidBank, eWallet: nextPaidEWallet },
        paymentParsed.primaryMethod,
        true
      );
      db.prepare(
        "UPDATE exports SET paid_amount = ?, paid_cash_amount = ?, paid_bank_amount = ?, paid_ewallet_amount = ?, credit_amount = ?, payment_method = ? WHERE id = ?"
      ).run(
        newPaid,
        nextPaidCash,
        nextPaidBank,
        nextPaidEWallet,
        newCredit,
        methodForRow,
        row.id
      );
      tripCount += 1;
    });
    db.exec("COMMIT;");
  } catch (err) {
    db.exec("ROLLBACK;");
    throw err;
  }

  const applied = sumPaymentBreakdown(appliedByMethod);
  const methodLabel = getPaymentMethodFromBreakdown(appliedByMethod, paymentParsed.primaryMethod, true);
  logActivity({
    userId: req.session.userId,
    action: "payment",
    entityType: "export_vehicle_cumulative_settlement",
    entityId: vehicleId,
    details: `vehicle=${vehicle.owner_name} • ${vehicle.vehicle_number}; payment=${applied}; method=${methodLabel}; cash=${appliedByMethod.cash}; bank=${appliedByMethod.bank}; ewallet=${appliedByMethod.eWallet}; trips=${tripCount}`
  });

  return res.redirect(buildExportsListUrl({ ...req.body, status: "vehicle_cumulative_paid" }));
});

router.get("/exports/payment-history", (req, res) => {
  const from = req.query.from || dayjs().subtract(7, "day").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const vehicleId = Number(req.query.vehicle_id || 0);
  if (!vehicleId) {
    return res.redirect(`/records/exports?from=${from}&to=${to}&error=dayCreditSelectVehicle`);
  }
  const vehicle = db.prepare(
    "SELECT id, vehicle_number, owner_name, is_company FROM vehicles WHERE id = ?"
  ).get(vehicleId);
  if (!vehicle) {
    return res.redirect(`/records/exports?from=${from}&to=${to}`);
  }

  const tripPayments = db.prepare(
    `SELECT activity_logs.created_at, activity_logs.details,
            exports.export_date, exports.receipt_no, exports.id as export_id
     FROM activity_logs
     JOIN exports ON exports.id = CAST(activity_logs.entity_id AS INTEGER)
     WHERE activity_logs.action = 'payment'
       AND activity_logs.entity_type = 'export'
       AND exports.export_date BETWEEN ? AND ?
       AND exports.vehicle_id = ?
     ORDER BY activity_logs.created_at DESC`
  ).all(from, to, vehicleId).map((row) => ({
    created_at: row.created_at,
    export_date: row.export_date,
    receipt_no: row.receipt_no || `#${row.export_id}`,
    payment_amount: parsePaymentAmount(row.details),
    payment_method: parsePaymentMethodFromDetails(row.details),
    details: row.details,
    source: "trip"
  }));

  const dayLogs = db.prepare(
    `SELECT created_at, details, entity_id
     FROM activity_logs
     WHERE action = 'payment'
       AND entity_type = 'export_day_credit'
     ORDER BY created_at DESC`
  ).all();

  const dayPayments = dayLogs.map((row) => {
    const [logVehicleIdRaw, logDate] = String(row.entity_id || "").split(":");
    const logVehicleId = Number(logVehicleIdRaw || 0);
    if (!logVehicleId || !logDate) return null;
    if (logVehicleId !== vehicleId) return null;
    if (logDate < from || logDate > to) return null;
    return {
      created_at: row.created_at,
      export_date: logDate,
      receipt_no: logDate,
      payment_amount: parsePaymentAmount(row.details),
      payment_method: parsePaymentMethodFromDetails(row.details),
      details: row.details,
      source: "day"
    };
  }).filter(Boolean);

  const payments = [...tripPayments, ...dayPayments].sort((a, b) => {
    return new Date(b.created_at).getTime() - new Date(a.created_at).getTime();
  });
  const totalPaid = payments.reduce((sum, row) => sum + Number(row.payment_amount || 0), 0);

  res.render("records/export_payment_history", {
    title: req.t("exportPaymentHistoryTitle"),
    from,
    to,
    vehicle,
    payments,
    totalPaid
  });
});

router.get("/exports/print", (req, res) => {
  const from = req.query.from || dayjs().subtract(7, "day").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const q = (req.query.q || "").trim();
  const sortRaw = req.query.sort || "date_desc";
  const sortMap = {
    date_desc: "export_date DESC, exports.created_at DESC",
    date_asc: "export_date ASC, exports.created_at ASC",
    total_desc: "exports.total_amount DESC",
    total_asc: "exports.total_amount ASC",
    jars_desc: "exports.jar_count DESC",
    paid_desc: "exports.paid_amount DESC",
    credit_desc: "exports.credit_amount DESC"
  };
  const sort = sortMap[sortRaw] ? sortRaw : "date_desc";
  const orderBy = sortMap[sort];
  const searchClause = q ? "AND (vehicles.vehicle_number LIKE ? OR vehicles.owner_name LIKE ?)" : "";
  const params = q ? [from, to, `%${q}%`, `%${q}%`] : [from, to];
  const exportsRows = db.prepare(
    `SELECT exports.*, vehicles.vehicle_number, vehicles.owner_name,
            users.full_name as recorded_by,
            COALESCE(NULLIF(TRIM(exports.checked_by_staff_name), ''), checked_staff.full_name) as checked_by_name
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     LEFT JOIN users ON exports.created_by = users.id
     LEFT JOIN staff as checked_staff ON exports.checked_by_staff_id = checked_staff.id
     WHERE export_date BETWEEN ? AND ?
     ${searchClause}
     ORDER BY ${orderBy}`
  ).all(...params);

  const exportTotals = db.prepare(
    `SELECT
        COUNT(exports.id) AS total_trips,
        COALESCE(SUM(exports.jar_count), 0) AS total_jars,
        COALESCE(SUM(exports.bottle_case_count), 0) AS total_bottle_cases,
        COALESCE(SUM(exports.dispenser_count), 0) AS total_dispensers,
        COALESCE(SUM(exports.dispenser_count * exports.dispenser_unit_price), 0) AS total_dispenser_amount,
        COALESCE(SUM(exports.return_jar_count), 0) AS total_return_jars,
        COALESCE(SUM(exports.return_bottle_case_count), 0) AS total_return_bottles,
        COALESCE(SUM(exports.leakage_jar_count), 0) AS total_leakage_jars,
        COALESCE(SUM(exports.sold_jar_count), 0) AS total_sold_jars,
        COALESCE(SUM(exports.sold_jar_amount), 0) AS total_sold_jar_amount,
        COALESCE(SUM(exports.collection_amount), 0) AS total_collection_amount,
        COALESCE(SUM(exports.expense_amount), 0) AS total_expense_amount,
        COALESCE(SUM(exports.total_amount), 0) AS total_amount,
        COALESCE(SUM(exports.paid_amount), 0) AS total_paid,
        COALESCE(SUM(exports.credit_amount), 0) AS total_credit
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     WHERE export_date BETWEEN ? AND ?
     ${searchClause}`
  ).get(...params);

  res.render("records/exports_print", {
    title: req.t("exportsTitle"),
    from,
    to,
    q,
    sort,
    exportsRows,
    exportTotals
  });
});

router.get("/exports/new", (req, res) => {
  const vehicles = db.prepare("SELECT id, vehicle_number, owner_name, is_company FROM vehicles ORDER BY vehicle_number").all();
  const staffOptions = getExportStaffOptions();
  const nameSuggestions = getExportNameSuggestions();
  res.render("records/export_form", {
    title: req.t("addExportTitle"),
    record: null,
    vehicles,
    staffOptions,
    nameSuggestions,
    formValues: null,
    error: null,
    defaultDate: dayjs().format("YYYY-MM-DD"),
    selectedVehicleId: "",
    checkedByStaffName: "",
    forceWashStaffName: "",
    useExternalVehicle: false,
    externalVehicleNumber: "",
    externalOwnerName: "",
    externalPhone: "",
    externalOrganization: ""
  });
});

router.post("/exports", (req, res) => {
  const {
    vehicle_id,
    use_external_vehicle,
    external_vehicle_number,
    external_owner_name,
    external_phone,
    external_organization,
    export_date,
    jar_count,
    bottle_case_count,
    dispenser_count,
    jar_unit_price,
    bottle_case_unit_price,
    dispenser_unit_price,
    return_jar_count,
    return_bottle_case_count,
    leakage_jar_count,
    sold_jar_count,
    sold_jar_price,
    collection_amount,
    note,
    paid_amount,
    payment_method,
    route,
    checked_by_staff_name,
    force_wash_staff_name,
    allow_duplicate_entry
  } = req.body;
  const vehicles = db.prepare("SELECT id, vehicle_number, owner_name, is_company FROM vehicles ORDER BY vehicle_number").all();
  const staffOptions = getExportStaffOptions();
  const nameSuggestions = getExportNameSuggestions();
  const checkedByStaffNameRaw = String(checked_by_staff_name || "").trim();
  const forceWashStaffNameRaw = String(force_wash_staff_name || "").trim();
  const useExternalVehicleRaw = parseCheckbox(use_external_vehicle);
  const externalVehicleNumberRaw = String(external_vehicle_number || "").trim();
  const externalOwnerNameRaw = String(external_owner_name || "").trim();
  const externalPhoneRaw = String(external_phone || "").trim();
  const externalOrganizationRaw = String(external_organization || "").trim();
  const allowDuplicateEntry = parseCheckbox(allow_duplicate_entry);
  if (!export_date) {
    return res.render("records/export_form", {
      title: req.t("addExportTitle"),
      record: null,
      vehicles,
      staffOptions,
      nameSuggestions,
      formValues: req.body,
      error: req.t("salesRequired"),
      defaultDate: dayjs().format("YYYY-MM-DD"),
      selectedVehicleId: vehicle_id || "",
      checkedByStaffName: checkedByStaffNameRaw,
      forceWashStaffName: forceWashStaffNameRaw,
      useExternalVehicle: useExternalVehicleRaw,
      externalVehicleNumber: externalVehicleNumberRaw,
      externalOwnerName: externalOwnerNameRaw,
      externalPhone: externalPhoneRaw,
      externalOrganization: externalOrganizationRaw
    });
  }
  const vehicleResolution = resolveExportVehicleInput({
    vehicleId: vehicle_id,
    useExternalVehicle: useExternalVehicleRaw,
    externalVehicleNumber: externalVehicleNumberRaw,
    externalOwnerName: externalOwnerNameRaw,
    externalPhone: externalPhoneRaw
  });
  if (vehicleResolution.errorKey) {
    return res.render("records/export_form", {
      title: req.t("addExportTitle"),
      record: null,
      vehicles,
      staffOptions,
      nameSuggestions,
      formValues: req.body,
      error: req.t(vehicleResolution.errorKey),
      defaultDate: export_date || dayjs().format("YYYY-MM-DD"),
      selectedVehicleId: vehicle_id || "",
      checkedByStaffName: checkedByStaffNameRaw,
      forceWashStaffName: forceWashStaffNameRaw,
      useExternalVehicle: useExternalVehicleRaw,
      externalVehicleNumber: externalVehicleNumberRaw,
      externalOwnerName: externalOwnerNameRaw,
      externalPhone: externalPhoneRaw,
      externalOrganization: externalOrganizationRaw
    });
  }
  const checkedByStaff = checkedByStaffNameRaw ? getExportStaffByName(checkedByStaffNameRaw) : null;
  const checkedByStaffId = checkedByStaff ? checkedByStaff.id : null;
  const checkedByStaffName = checkedByStaffNameRaw || (checkedByStaff ? checkedByStaff.full_name : null);
  const forceWashStaffName = forceWashStaffNameRaw || null;
  const forceWashRequired = forceWashStaffName ? 1 : 0;

  const jarCount = Number(jar_count || 0);
  const bottleCount = Number(bottle_case_count || 0);
  const dispenserCountRaw = Number(dispenser_count || 0);
  const dispenserCount = Number.isNaN(dispenserCountRaw) || dispenserCountRaw < 0 ? 0 : dispenserCountRaw;
  let jarUnitPrice = Number(jar_unit_price || 0);
  let bottleCaseUnitPrice = Number(bottle_case_unit_price || 0);
  let dispenserUnitPrice = Number(dispenser_unit_price || 0);
  const returnJars = Number(return_jar_count || 0);
  const returnBottles = Number(return_bottle_case_count || 0);
  const leakageJars = Number(leakage_jar_count || 0);
  let soldJars = Number(sold_jar_count || 0);
  let soldJarPrice = Number(sold_jar_price || 0);
  if (Number.isNaN(jarUnitPrice) || jarUnitPrice < 0) jarUnitPrice = 0;
  if (Number.isNaN(bottleCaseUnitPrice) || bottleCaseUnitPrice < 0) bottleCaseUnitPrice = 0;
  if (Number.isNaN(dispenserUnitPrice) || dispenserUnitPrice < 0) dispenserUnitPrice = 0;
  if (Number.isNaN(soldJars) || soldJars < 0) soldJars = 0;
  if (Number.isNaN(soldJarPrice) || soldJarPrice < 0) soldJarPrice = 0;
  const netJars = Math.max(0, jarCount - returnJars - leakageJars);
  const netBottles = Math.max(0, bottleCount - returnBottles);
  const bottleCaseAvailable = getBottleCaseStorageBalance();
  if (netBottles > bottleCaseAvailable) {
    return res.render("records/export_form", {
      title: req.t("addExportTitle"),
      record: null,
      vehicles,
      staffOptions,
      nameSuggestions,
      formValues: req.body,
      error: req.t("bottleCaseInsufficient", { available: bottleCaseAvailable }),
      defaultDate: export_date || dayjs().format("YYYY-MM-DD"),
      selectedVehicleId: vehicle_id || "",
      checkedByStaffName: checkedByStaffNameRaw,
      forceWashStaffName: forceWashStaffNameRaw,
      useExternalVehicle: useExternalVehicleRaw,
      externalVehicleNumber: externalVehicleNumberRaw,
      externalOwnerName: externalOwnerNameRaw,
      externalPhone: externalPhoneRaw,
      externalOrganization: externalOrganizationRaw
    });
  }
  const dispenserAvailable = getDispenserStorageBalance();
  if (dispenserCount > dispenserAvailable) {
    return res.render("records/export_form", {
      title: req.t("addExportTitle"),
      record: null,
      vehicles,
      staffOptions,
      nameSuggestions,
      formValues: req.body,
      error: req.t("dispenserInsufficient", { available: dispenserAvailable }),
      defaultDate: export_date || dayjs().format("YYYY-MM-DD"),
      selectedVehicleId: vehicle_id || "",
      checkedByStaffName: checkedByStaffNameRaw,
      forceWashStaffName: forceWashStaffNameRaw,
      useExternalVehicle: useExternalVehicleRaw,
      externalVehicleNumber: externalVehicleNumberRaw,
      externalOwnerName: externalOwnerNameRaw,
      externalPhone: externalPhoneRaw,
      externalOrganization: externalOrganizationRaw
    });
  }
  const resolvedVehicleId = Number(vehicleResolution.vehicleId);
  const vehicleRow = db.prepare("SELECT is_company FROM vehicles WHERE id = ?").get(resolvedVehicleId);
  const isCompany = vehicleRow && Number(vehicleRow.is_company) === 1;
  if (isCompany) {
    jarUnitPrice = 0;
    bottleCaseUnitPrice = 0;
    dispenserUnitPrice = 0;
  }
  const dispenserAmount = Math.max(0, dispenserCount) * Math.max(0, dispenserUnitPrice);
  if (!isCompany) {
    soldJars = 0;
    soldJarPrice = 0;
  }
  const soldJarAmount = Math.max(0, soldJars) * Math.max(0, soldJarPrice);
  let totalAmount = netJars * jarUnitPrice + netBottles * bottleCaseUnitPrice + dispenserAmount + soldJarAmount;
  let collectionAmount = Number(collection_amount || 0);
  const expenseAmount = 0;
  if (Number.isNaN(collectionAmount) || collectionAmount < 0) collectionAmount = 0;
  if (!isCompany) {
    collectionAmount = 0;
  }
  if (isCompany) totalAmount = collectionAmount;
  const paymentParsed = parsePaymentBreakdownFromBody(req.body, {
    cashField: "paid_cash_amount",
    bankField: "paid_bank_amount",
    ewalletField: "paid_ewallet_amount",
    amountField: "paid_amount",
    methodField: "payment_method",
    maxTotal: isCompany ? collectionAmount : totalAmount
  });
  const paidBreakdown = isCompany
    ? { cash: parseMoneyValue(collectionAmount), bank: 0, eWallet: 0 }
    : paymentParsed.breakdown;
  const effectivePaid = sumPaymentBreakdown(paidBreakdown);
  const paymentMethod = getPaymentMethodFromBreakdown(
    paidBreakdown,
    normalizePaymentMethod(payment_method),
    true
  );
  const creditAmount = isCompany ? 0 : Math.max(0, totalAmount - effectivePaid);
  const expenseNoteValue = null;
  const externalVehicleNote = buildExternalVehicleNote({
    useExternalVehicle: vehicleResolution.useExternalVehicle,
    externalOwnerName: vehicleResolution.externalOwnerName,
    externalPhone: vehicleResolution.externalPhone,
    externalOrganization: externalOrganizationRaw
  });
  const noteValue = mergeNoteWithExternalVehicle(note, externalVehicleNote);
  const duplicateEntries = findDuplicateExportEntries({
    vehicleId: resolvedVehicleId,
    exportDate: export_date,
    totalAmount,
    excludeId: null
  });
  if (duplicateEntries.length > 0 && !allowDuplicateEntry) {
    return res.render("records/export_form", {
      title: req.t("addExportTitle"),
      record: null,
      vehicles,
      staffOptions,
      nameSuggestions,
      formValues: req.body,
      error: null,
      duplicateWarning: {
        type: "export",
        rows: duplicateEntries,
        exportDate: export_date,
        amount: totalAmount
      },
      defaultDate: export_date || dayjs().format("YYYY-MM-DD"),
      selectedVehicleId: vehicle_id || "",
      checkedByStaffName: checkedByStaffNameRaw,
      forceWashStaffName: forceWashStaffNameRaw,
      useExternalVehicle: useExternalVehicleRaw,
      externalVehicleNumber: externalVehicleNumberRaw,
      externalOwnerName: externalOwnerNameRaw,
      externalPhone: externalPhoneRaw,
      externalOrganization: externalOrganizationRaw
    });
  }
  if (duplicateEntries.length > 0 && allowDuplicateEntry) {
    const duplicateIds = duplicateEntries.map((row) => row.id).join(",");
    logActivity({
      userId: req.session.userId,
      action: "duplicate_override",
      entityType: "export",
      entityId: `vehicle:${resolvedVehicleId}`,
      details: `duplicate_ids=${duplicateIds}, date=${export_date}, amount=${totalAmount}`
    });
  }

  const exportResult = db.prepare(
    "INSERT INTO exports (vehicle_id, export_date, jar_count, bottle_case_count, dispenser_count, jar_unit_price, bottle_case_unit_price, dispenser_unit_price, return_jar_count, return_bottle_case_count, leakage_jar_count, sold_jar_count, sold_jar_price, sold_jar_amount, collection_amount, expense_amount, expense_note, total_amount, paid_amount, paid_cash_amount, paid_bank_amount, paid_ewallet_amount, payment_method, credit_amount, note, route, checked_by_staff_id, checked_by_staff_name, force_wash_required, force_wash_staff_name, created_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
  ).run(
    resolvedVehicleId,
    export_date,
    jarCount,
    bottleCount,
    dispenserCount,
    jarUnitPrice,
    bottleCaseUnitPrice,
    dispenserUnitPrice,
    returnJars,
    returnBottles,
    leakageJars,
    soldJars,
    soldJarPrice,
    soldJarAmount,
    collectionAmount,
    expenseAmount,
    expenseNoteValue,
    totalAmount,
    effectivePaid,
    paidBreakdown.cash,
    paidBreakdown.bank,
    paidBreakdown.eWallet,
    paymentMethod,
    creditAmount,
    noteValue,
    route || null,
    checkedByStaffId,
    checkedByStaffName,
    forceWashRequired,
    forceWashStaffName,
    req.session.userId
  );
  const exportId = Number(exportResult.lastInsertRowid);
  const exportReceiptNo = createReceiptNo(db, "EXP", export_date || dayjs().format("YYYY-MM-DD"));
  db.prepare("UPDATE exports SET receipt_no = ? WHERE id = ?").run(exportReceiptNo, exportId);
  logActivity({
    userId: req.session.userId,
    action: "create",
    entityType: "export",
    entityId: exportId,
    details: `receipt=${exportReceiptNo}, export_date=${export_date}, vehicle_id=${resolvedVehicleId}, external_vehicle=${vehicleResolution.useExternalVehicle ? 1 : 0}, external_vehicle_number=${vehicleResolution.externalVehicleNumber || ''}, external_owner=${vehicleResolution.externalOwnerName || ''}, external_phone=${vehicleResolution.externalPhone || ''}, external_org=${externalOrganizationRaw || ''}, jars=${jarCount}, jar_price=${jarUnitPrice}, bottles=${bottleCount}, bottle_price=${bottleCaseUnitPrice}, dispensers=${dispenserCount}, dispenser_price=${dispenserUnitPrice}, return_jars=${returnJars}, return_bottles=${returnBottles}, leakage_jars=${leakageJars}, sold_jars=${soldJars}, sold_price=${soldJarPrice}, collection=${collectionAmount}, paid_method=${paymentMethod}, paid_cash=${paidBreakdown.cash}, paid_bank=${paidBreakdown.bank}, paid_ewallet=${paidBreakdown.eWallet}, expense=${expenseAmount}, route=${route || ''}, checked_staff=${checkedByStaffId || ''}, checked_staff_name=${checkedByStaffName || ''}, force_wash=${forceWashRequired}, force_wash_by=${forceWashStaffName || ''}`
  });

  res.redirect(`/records/exports/${exportId}/saved`);
});

router.get("/exports/:id/saved", (req, res) => {
  const record = db.prepare(
    `SELECT exports.id, exports.export_date, exports.receipt_no, exports.vehicle_id, vehicles.vehicle_number, vehicles.owner_name, vehicles.is_company
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     WHERE exports.id = ?`
  ).get(req.params.id);
  if (!record) return res.redirect("/records/exports");
  res.render("records/export_saved", {
    title: req.t("exportSavedTitle"),
    record
  });
});

router.get("/exports/:id/pay-credit", (req, res) => {
  const record = db.prepare(
    `SELECT exports.id, exports.export_date, exports.receipt_no, exports.total_amount, exports.paid_amount, exports.paid_cash_amount, exports.paid_bank_amount, exports.paid_ewallet_amount, exports.credit_amount,
            exports.payment_method,
            exports.vehicle_id, vehicles.vehicle_number, vehicles.owner_name, vehicles.is_company
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     WHERE exports.id = ?`
  ).get(req.params.id);
  if (!record) return res.redirect("/records/exports");
  if (Number(record.is_company) === 1) {
    return res.redirect("/records/exports?error=companyVehicleNoCredit");
  }
  const remaining = computeRemainingMoney(record.total_amount || 0, record.paid_amount || 0);
  if (remaining <= 0) {
    return res.redirect(`/records/exports?from=${record.export_date}&to=${record.export_date}&error=exportCreditPaymentNoRemaining`);
  }
  res.render("records/export_credit_payment", {
    title: req.t("payExportCreditTitle"),
    record,
    remaining,
    error: null
  });
});

router.post("/exports/:id/pay-credit", (req, res) => {
  const record = db.prepare(
    `SELECT exports.id, exports.export_date, exports.total_amount, exports.paid_amount, exports.paid_cash_amount, exports.paid_bank_amount, exports.paid_ewallet_amount, exports.credit_amount,
            exports.payment_method,
            exports.vehicle_id, vehicles.vehicle_number, vehicles.owner_name, vehicles.is_company
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     WHERE exports.id = ?`
  ).get(req.params.id);
  if (!record) return res.redirect("/records/exports");
  if (Number(record.is_company) === 1) {
    return res.redirect("/records/exports?error=companyVehicleNoCredit");
  }

  const remaining = computeRemainingMoney(record.total_amount || 0, record.paid_amount || 0);
  if (remaining <= 0) {
    return res.redirect(`/records/exports?from=${record.export_date}&to=${record.export_date}&error=exportCreditPaymentNoRemaining`);
  }

  const paymentParsed = parsePaymentBreakdownFromBody(req.body, {
    maxTotal: remaining
  });
  if (paymentParsed.total <= 0) {
    return res.render("records/export_credit_payment", {
      title: req.t("payExportCreditTitle"),
      record,
      remaining,
      error: req.t("exportCreditPaymentInvalid")
    });
  }

  const safeRemaining = computeRemainingMoney(record.total_amount || 0, record.paid_amount || 0);
  const appliedPayment = parseMoneyValue(Math.min(paymentParsed.total, safeRemaining));
  const newPaidAmount = parseMoneyValue(Math.min(
    parseMoneyValue(record.total_amount || 0),
    parseMoneyValue(record.paid_amount || 0) + appliedPayment
  ));
  const newCreditAmount = computeRemainingMoney(record.total_amount || 0, newPaidAmount);
  const nextPaidCash = parseMoneyValue(Number(record.paid_cash_amount || 0) + Number(paymentParsed.breakdown.cash || 0));
  const nextPaidBank = parseMoneyValue(Number(record.paid_bank_amount || 0) + Number(paymentParsed.breakdown.bank || 0));
  const nextPaidEWallet = parseMoneyValue(Number(record.paid_ewallet_amount || 0) + Number(paymentParsed.breakdown.eWallet || 0));
  const paymentMethod = getPaymentMethodFromBreakdown(
    { cash: nextPaidCash, bank: nextPaidBank, eWallet: nextPaidEWallet },
    req.body.payment_method || record.payment_method,
    true
  );

  db.prepare("UPDATE exports SET paid_amount = ?, paid_cash_amount = ?, paid_bank_amount = ?, paid_ewallet_amount = ?, credit_amount = ?, payment_method = ? WHERE id = ?").run(
    newPaidAmount,
    nextPaidCash,
    nextPaidBank,
    nextPaidEWallet,
    newCreditAmount,
    paymentMethod,
    req.params.id
  );

  logActivity({
    userId: req.session.userId,
    action: "payment",
    entityType: "export",
    entityId: req.params.id,
    details: `payment=${appliedPayment}; method=${getPaymentMethodFromBreakdown(paymentParsed.breakdown, paymentParsed.primaryMethod, true)}; cash=${paymentParsed.breakdown.cash || 0}; bank=${paymentParsed.breakdown.bank || 0}; ewallet=${paymentParsed.breakdown.eWallet || 0}; paid_amount: ${formatDiffValue(record.paid_amount)} -> ${formatDiffValue(newPaidAmount)}; credit_amount: ${formatDiffValue(record.credit_amount)} -> ${formatDiffValue(newCreditAmount)}`
  });

  return res.redirect(`/records/exports?from=${record.export_date}&to=${record.export_date}&status=credit_paid`);
});

router.get("/exports/:id(\\d+)", (req, res) => {
  const record = db.prepare(
    `SELECT exports.*, vehicles.vehicle_number, vehicles.owner_name, vehicles.phone, vehicles.is_company,
            users.full_name as recorded_by,
            COALESCE(NULLIF(TRIM(exports.checked_by_staff_name), ''), checked_staff.full_name) as checked_by_name
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     LEFT JOIN users ON exports.created_by = users.id
     LEFT JOIN staff as checked_staff ON exports.checked_by_staff_id = checked_staff.id
     WHERE exports.id = ?`
  ).get(req.params.id);
  if (!record) return res.redirect("/records/exports");
  res.render("records/export_details", {
    title: req.t("exportsTitle"),
    record
  });
});

router.get("/exports/:id/edit", (req, res) => {
  const record = db.prepare("SELECT * FROM exports WHERE id = ?").get(req.params.id);
  if (!record) return res.redirect("/records/exports");
  const vehicles = db.prepare("SELECT id, vehicle_number, owner_name, is_company FROM vehicles ORDER BY vehicle_number").all();
  const staffOptions = getExportStaffOptions();
  const nameSuggestions = getExportNameSuggestions();
  res.render("records/export_form", {
    title: req.t("editExportTitle"),
    record,
    vehicles,
    staffOptions,
    nameSuggestions,
    formValues: null,
    error: null,
    defaultDate: record.export_date,
    selectedVehicleId: record.vehicle_id,
    checkedByStaffName: record.checked_by_staff_name || "",
    forceWashStaffName: record.force_wash_staff_name || "",
    useExternalVehicle: false,
    externalVehicleNumber: "",
    externalOwnerName: "",
    externalPhone: "",
    externalOrganization: ""
  });
});

router.post("/exports/:id", (req, res) => {
  const {
    vehicle_id,
    use_external_vehicle,
    external_vehicle_number,
    external_owner_name,
    external_phone,
    external_organization,
    export_date,
    jar_count,
    bottle_case_count,
    dispenser_count,
    jar_unit_price,
    bottle_case_unit_price,
    dispenser_unit_price,
    return_jar_count,
    return_bottle_case_count,
    leakage_jar_count,
    sold_jar_count,
    sold_jar_price,
    collection_amount,
    note,
    paid_amount,
    payment_method,
    route,
    checked_by_staff_name,
    force_wash_staff_name
  } = req.body;
  const record = db.prepare("SELECT * FROM exports WHERE id = ?").get(req.params.id);
  const vehicles = db.prepare("SELECT id, vehicle_number, owner_name, is_company FROM vehicles ORDER BY vehicle_number").all();
  const staffOptions = getExportStaffOptions();
  const nameSuggestions = getExportNameSuggestions();
  const checkedByStaffNameRaw = String(checked_by_staff_name || "").trim();
  const forceWashStaffNameRaw = String(force_wash_staff_name || "").trim();
  if (!record) return res.redirect("/records/exports");
  const useExternalVehicleRaw = parseCheckbox(use_external_vehicle);
  const externalVehicleNumberRaw = String(external_vehicle_number || "").trim();
  const externalOwnerNameRaw = String(external_owner_name || "").trim();
  const externalPhoneRaw = String(external_phone || "").trim();
  const externalOrganizationRaw = String(external_organization || "").trim();
  if (!export_date) {
    return res.render("records/export_form", {
      title: req.t("editExportTitle"),
      record,
      vehicles,
      staffOptions,
      nameSuggestions,
      formValues: req.body,
      error: req.t("salesRequired"),
      defaultDate: record.export_date,
      selectedVehicleId: vehicle_id || record.vehicle_id,
      checkedByStaffName: checkedByStaffNameRaw,
      forceWashStaffName: forceWashStaffNameRaw,
      useExternalVehicle: useExternalVehicleRaw,
      externalVehicleNumber: externalVehicleNumberRaw,
      externalOwnerName: externalOwnerNameRaw,
      externalPhone: externalPhoneRaw,
      externalOrganization: externalOrganizationRaw
    });
  }
  const vehicleResolution = resolveExportVehicleInput({
    vehicleId: vehicle_id,
    useExternalVehicle: useExternalVehicleRaw,
    externalVehicleNumber: externalVehicleNumberRaw,
    externalOwnerName: externalOwnerNameRaw,
    externalPhone: externalPhoneRaw
  });

  if (vehicleResolution.errorKey) {
    return res.render("records/export_form", {
      title: req.t("editExportTitle"),
      record,
      vehicles,
      staffOptions,
      nameSuggestions,
      formValues: req.body,
      error: req.t(vehicleResolution.errorKey),
      defaultDate: export_date || record.export_date,
      selectedVehicleId: vehicle_id || record.vehicle_id,
      checkedByStaffName: checkedByStaffNameRaw,
      forceWashStaffName: forceWashStaffNameRaw,
      useExternalVehicle: useExternalVehicleRaw,
      externalVehicleNumber: externalVehicleNumberRaw,
      externalOwnerName: externalOwnerNameRaw,
      externalPhone: externalPhoneRaw,
      externalOrganization: externalOrganizationRaw
    });
  }
  const checkedByStaff = checkedByStaffNameRaw ? getExportStaffByName(checkedByStaffNameRaw) : null;
  const checkedByStaffId = checkedByStaff ? checkedByStaff.id : null;
  const checkedByStaffName = checkedByStaffNameRaw || (checkedByStaff ? checkedByStaff.full_name : null);
  const forceWashStaffName = forceWashStaffNameRaw || null;
  const forceWashRequired = forceWashStaffName ? 1 : 0;

  const jarCount = Number(jar_count || 0);
  const bottleCount = Number(bottle_case_count || 0);
  const dispenserCountRaw = Number(dispenser_count || 0);
  const dispenserCount = Number.isNaN(dispenserCountRaw) || dispenserCountRaw < 0 ? 0 : dispenserCountRaw;
  let jarUnitPrice = Number(jar_unit_price || 0);
  let bottleCaseUnitPrice = Number(bottle_case_unit_price || 0);
  let dispenserUnitPrice = Number(dispenser_unit_price || 0);
  const returnJars = Number(return_jar_count || 0);
  const returnBottles = Number(return_bottle_case_count || 0);
  const leakageJars = Number(leakage_jar_count || 0);
  let soldJars = Number(sold_jar_count || 0);
  let soldJarPrice = Number(sold_jar_price || 0);
  if (Number.isNaN(jarUnitPrice) || jarUnitPrice < 0) jarUnitPrice = 0;
  if (Number.isNaN(bottleCaseUnitPrice) || bottleCaseUnitPrice < 0) bottleCaseUnitPrice = 0;
  if (Number.isNaN(dispenserUnitPrice) || dispenserUnitPrice < 0) dispenserUnitPrice = 0;
  if (Number.isNaN(soldJars) || soldJars < 0) soldJars = 0;
  if (Number.isNaN(soldJarPrice) || soldJarPrice < 0) soldJarPrice = 0;
  const netJars = Math.max(0, jarCount - returnJars - leakageJars);
  const netBottles = Math.max(0, bottleCount - returnBottles);
  const bottleCaseAvailable = getBottleCaseStorageBalance({ excludeExportId: req.params.id });
  if (netBottles > bottleCaseAvailable) {
    return res.render("records/export_form", {
      title: req.t("editExportTitle"),
      record,
      vehicles,
      staffOptions,
      nameSuggestions,
      formValues: req.body,
      error: req.t("bottleCaseInsufficient", { available: bottleCaseAvailable }),
      defaultDate: export_date || record.export_date,
      selectedVehicleId: vehicle_id || record.vehicle_id,
      checkedByStaffName: checkedByStaffNameRaw,
      forceWashStaffName: forceWashStaffNameRaw,
      useExternalVehicle: useExternalVehicleRaw,
      externalVehicleNumber: externalVehicleNumberRaw,
      externalOwnerName: externalOwnerNameRaw,
      externalPhone: externalPhoneRaw,
      externalOrganization: externalOrganizationRaw
    });
  }
  const dispenserAvailable = getDispenserStorageBalance({ excludeExportId: req.params.id });
  if (dispenserCount > dispenserAvailable) {
    return res.render("records/export_form", {
      title: req.t("editExportTitle"),
      record,
      vehicles,
      staffOptions,
      nameSuggestions,
      formValues: req.body,
      error: req.t("dispenserInsufficient", { available: dispenserAvailable }),
      defaultDate: export_date || record.export_date,
      selectedVehicleId: vehicle_id || record.vehicle_id,
      checkedByStaffName: checkedByStaffNameRaw,
      forceWashStaffName: forceWashStaffNameRaw,
      useExternalVehicle: useExternalVehicleRaw,
      externalVehicleNumber: externalVehicleNumberRaw,
      externalOwnerName: externalOwnerNameRaw,
      externalPhone: externalPhoneRaw,
      externalOrganization: externalOrganizationRaw
    });
  }
  const resolvedVehicleId = Number(vehicleResolution.vehicleId);
  const vehicleRow = db.prepare("SELECT is_company FROM vehicles WHERE id = ?").get(resolvedVehicleId);
  const isCompany = vehicleRow && Number(vehicleRow.is_company) === 1;
  if (isCompany) {
    jarUnitPrice = 0;
    bottleCaseUnitPrice = 0;
    dispenserUnitPrice = 0;
  }
  const dispenserAmount = Math.max(0, dispenserCount) * Math.max(0, dispenserUnitPrice);
  if (!isCompany) {
    soldJars = 0;
    soldJarPrice = 0;
  }
  const soldJarAmount = Math.max(0, soldJars) * Math.max(0, soldJarPrice);
  let totalAmount = netJars * jarUnitPrice + netBottles * bottleCaseUnitPrice + dispenserAmount + soldJarAmount;
  let collectionAmount = Number(collection_amount || 0);
  const expenseAmount = 0;
  if (Number.isNaN(collectionAmount) || collectionAmount < 0) collectionAmount = 0;
  if (!isCompany) {
    collectionAmount = 0;
  }
  if (isCompany) totalAmount = collectionAmount;
  const paymentParsed = parsePaymentBreakdownFromBody(req.body, {
    cashField: "paid_cash_amount",
    bankField: "paid_bank_amount",
    ewalletField: "paid_ewallet_amount",
    amountField: "paid_amount",
    methodField: "payment_method",
    maxTotal: isCompany ? collectionAmount : totalAmount
  });
  const paidBreakdown = isCompany
    ? { cash: parseMoneyValue(collectionAmount), bank: 0, eWallet: 0 }
    : paymentParsed.breakdown;
  const effectivePaid = sumPaymentBreakdown(paidBreakdown);
  const paymentMethod = getPaymentMethodFromBreakdown(
    paidBreakdown,
    normalizePaymentMethod(payment_method),
    true
  );
  const creditAmount = isCompany ? 0 : Math.max(0, totalAmount - effectivePaid);
  const expenseNoteValue = null;
  const externalVehicleNote = buildExternalVehicleNote({
    useExternalVehicle: vehicleResolution.useExternalVehicle,
    externalOwnerName: vehicleResolution.externalOwnerName,
    externalPhone: vehicleResolution.externalPhone,
    externalOrganization: externalOrganizationRaw
  });
  const noteValue = mergeNoteWithExternalVehicle(note, externalVehicleNote);

  db.prepare(
    "UPDATE exports SET vehicle_id = ?, export_date = ?, jar_count = ?, bottle_case_count = ?, dispenser_count = ?, jar_unit_price = ?, bottle_case_unit_price = ?, dispenser_unit_price = ?, return_jar_count = ?, return_bottle_case_count = ?, leakage_jar_count = ?, sold_jar_count = ?, sold_jar_price = ?, sold_jar_amount = ?, collection_amount = ?, expense_amount = ?, expense_note = ?, total_amount = ?, paid_amount = ?, paid_cash_amount = ?, paid_bank_amount = ?, paid_ewallet_amount = ?, payment_method = ?, credit_amount = ?, note = ?, route = ?, checked_by_staff_id = ?, checked_by_staff_name = ?, force_wash_required = ?, force_wash_staff_name = ? WHERE id = ?"
  ).run(
    resolvedVehicleId,
    export_date,
    jarCount,
    bottleCount,
    dispenserCount,
    jarUnitPrice,
    bottleCaseUnitPrice,
    dispenserUnitPrice,
    returnJars,
    returnBottles,
    leakageJars,
    soldJars,
    soldJarPrice,
    soldJarAmount,
    collectionAmount,
    expenseAmount,
    expenseNoteValue,
    totalAmount,
    effectivePaid,
    paidBreakdown.cash,
    paidBreakdown.bank,
    paidBreakdown.eWallet,
    paymentMethod,
    creditAmount,
    noteValue,
    route || null,
    checkedByStaffId,
    checkedByStaffName,
    forceWashRequired,
    forceWashStaffName,
    req.params.id
  );
  logActivity({
    userId: req.session.userId,
    action: "update",
    entityType: "export",
    entityId: req.params.id,
    details: buildDiffDetails(
      record,
      {
        vehicle_id: Number(resolvedVehicleId),
        export_date,
        jar_count: jarCount,
        bottle_case_count: bottleCount,
        dispenser_count: dispenserCount,
        jar_unit_price: jarUnitPrice,
        bottle_case_unit_price: bottleCaseUnitPrice,
        dispenser_unit_price: dispenserUnitPrice,
        return_jar_count: returnJars,
        return_bottle_case_count: returnBottles,
        leakage_jar_count: leakageJars,
        sold_jar_count: soldJars,
        sold_jar_price: soldJarPrice,
        sold_jar_amount: soldJarAmount,
        collection_amount: collectionAmount,
        expense_amount: expenseAmount,
        expense_note: expenseNoteValue,
        total_amount: totalAmount,
        paid_amount: effectivePaid,
        paid_cash_amount: paidBreakdown.cash,
        paid_bank_amount: paidBreakdown.bank,
        paid_ewallet_amount: paidBreakdown.eWallet,
        payment_method: paymentMethod,
        credit_amount: creditAmount,
        note: noteValue,
        route: route || null,
        checked_by_staff_id: checkedByStaffId,
        checked_by_staff_name: checkedByStaffName,
        force_wash_required: forceWashRequired,
        force_wash_staff_name: forceWashStaffName
      },
      [
        "vehicle_id",
        "export_date",
        "jar_count",
        "bottle_case_count",
        "dispenser_count",
        "jar_unit_price",
        "bottle_case_unit_price",
        "dispenser_unit_price",
        "return_jar_count",
        "return_bottle_case_count",
        "leakage_jar_count",
        "sold_jar_count",
        "sold_jar_price",
        "sold_jar_amount",
        "collection_amount",
        "expense_amount",
        "expense_note",
        "total_amount",
        "paid_amount",
        "paid_cash_amount",
        "paid_bank_amount",
        "paid_ewallet_amount",
        "payment_method",
        "credit_amount",
        "note",
        "route",
        "checked_by_staff_id",
        "checked_by_staff_name",
        "force_wash_required",
        "force_wash_staff_name"
      ]
    )
  });

  res.redirect(`/records/exports?from=${export_date}&to=${export_date}`);
});

router.post("/exports/:id/delete", (req, res) => {
  const record = db.prepare("SELECT * FROM exports WHERE id = ?").get(req.params.id);
  if (!record) return res.redirect("/records/exports");
  const recycleId = createRecycleEntry({
    entityType: "export",
    entityId: req.params.id,
    payload: { export: record },
    deletedBy: req.session.userId,
    note: `date=${record.export_date || ""}`
  });
  logActivity({
    userId: req.session.userId,
    action: "delete",
    entityType: "export",
    entityId: req.params.id,
    details: `recycle_id=${recycleId}`
  });
  db.prepare("DELETE FROM exports WHERE id = ?").run(req.params.id);
  res.redirect("/records/exports");
});

router.get("/exports/:id/print", (req, res) => {
  const record = db.prepare(
    `SELECT exports.*, vehicles.vehicle_number, vehicles.owner_name, vehicles.phone,
            COALESCE(NULLIF(TRIM(exports.checked_by_staff_name), ''), checked_staff.full_name) as checked_by_name
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     LEFT JOIN staff as checked_staff ON exports.checked_by_staff_id = checked_staff.id
     WHERE exports.id = ?`
  ).get(req.params.id);
  if (!record) return res.redirect("/records/exports");
  res.render("records/export_print", { title: req.t("exportsTitle"), record });
});

router.get("/exports/export", (req, res) => {
  const from = req.query.from || dayjs().subtract(7, "day").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const q = (req.query.q || "").trim();
  const sortRaw = req.query.sort || "date_desc";
  const sortMap = {
    date_desc: "export_date DESC, exports.created_at DESC",
    date_asc: "export_date ASC, exports.created_at ASC",
    total_desc: "exports.total_amount DESC",
    total_asc: "exports.total_amount ASC",
    jars_desc: "exports.jar_count DESC",
    paid_desc: "exports.paid_amount DESC",
    credit_desc: "exports.credit_amount DESC"
  };
  const sort = sortMap[sortRaw] ? sortRaw : "date_desc";
  const orderBy = sortMap[sort];
  const searchClause = q ? "AND (vehicles.vehicle_number LIKE ? OR vehicles.owner_name LIKE ?)" : "";
  const params = q ? [from, to, `%${q}%`, `%${q}%`] : [from, to];
  const exportsRows = db.prepare(
    `SELECT exports.export_date, exports.receipt_no, vehicles.vehicle_number, vehicles.owner_name,
            exports.jar_count, exports.bottle_case_count, exports.dispenser_count,
            exports.jar_unit_price, exports.bottle_case_unit_price, exports.dispenser_unit_price,
            exports.return_jar_count, exports.return_bottle_case_count, exports.leakage_jar_count,
            exports.sold_jar_count, exports.sold_jar_price, exports.sold_jar_amount,
            exports.collection_amount, exports.expense_amount, exports.expense_note,
            exports.total_amount, exports.paid_amount, exports.credit_amount,
            exports.note, exports.route,
            COALESCE(NULLIF(TRIM(exports.checked_by_staff_name), ''), checked_staff.full_name) as checked_by_name,
            exports.force_wash_required,
            exports.force_wash_staff_name
     FROM exports
     JOIN vehicles ON exports.vehicle_id = vehicles.id
     LEFT JOIN staff as checked_staff ON exports.checked_by_staff_id = checked_staff.id
     WHERE export_date BETWEEN ? AND ?
     ${searchClause}
     ORDER BY ${orderBy}`
  ).all(...params);

  const header = "Date (AD),Date (BS),Receipt No,Vehicle Number,Owner Name,Checked By Staff,Force Wash,Force Wash By,Jars,Bottle Cases,Dispensers,Jar Unit Price,Bottle Case Unit Price,Dispenser Unit Price,Return Jars,Return Bottle Cases,Leakage Jars,Sold Jars,Sold Jar Price,Sold Jar Amount,Collection Amount,Expense Amount,Expense Note,Total Amount,Paid Amount,Credit Amount,Note,Route";
  const lines = exportsRows.map((row) => {
    const bsDate = adToBs(row.export_date) || "";
    const safe = [
      row.export_date,
      bsDate,
      row.receipt_no || "",
      row.vehicle_number,
      row.owner_name,
      row.checked_by_name || "",
      Number(row.force_wash_required || 0) === 1 ? "Yes" : "No",
      row.force_wash_staff_name || "",
      row.jar_count,
      row.bottle_case_count,
      row.dispenser_count || 0,
      row.jar_unit_price || 0,
      row.bottle_case_unit_price || 0,
      row.dispenser_unit_price || 0,
      row.return_jar_count,
      row.return_bottle_case_count,
      row.leakage_jar_count,
      row.sold_jar_count,
      row.sold_jar_price,
      row.sold_jar_amount,
      row.collection_amount,
      row.expense_amount || 0,
      row.expense_note || "",
      row.total_amount,
      row.paid_amount,
      row.credit_amount,
      row.note || "",
      row.route || ""
    ].map((val) => {
      const str = String(val ?? "").replace(/\"/g, "\"\"");
      return `"${str}"`;
    });
    return safe.join(",");
  });

  const csv = [header, ...lines].join("\n");
  res.setHeader("Content-Type", "text/csv");
  res.setHeader("Content-Disposition", `attachment; filename="exports_${from}_to_${to}.csv"`);
  res.send(csv);
});

router.get("/imports", (req, res) => {
  const from = req.query.from || dayjs().startOf("month").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const q = String(req.query.q || "").trim();
  const itemTypes = getImportItemsForUi(req.t);
  const itemCodes = new Set(itemTypes.map((row) => row.code));
  const rawItem = req.query.item || "all";
  const item = rawItem === "all" || itemCodes.has(rawItem) ? rawItem : "all";
  const itemClause = item === "all" ? "" : "AND import_entries.item_type = ?";
  const searchClause = q
    ? "AND (COALESCE(import_entries.seller_name, '') LIKE ? OR COALESCE(import_entries.note, '') LIKE ?)"
    : "";
  const params = [from, to];
  if (item !== "all") params.push(item);
  if (q) params.push(`%${q}%`, `%${q}%`);

  const entries = db.prepare(
    `SELECT import_entries.*, jar_types.name as jar_type_name, jar_cap_types.name as jar_cap_type_name,
            import_item_types.name as item_type_name, import_item_types.unit_label as item_unit_label,
            users.full_name as recorded_by
     FROM import_entries
     LEFT JOIN jar_types ON import_entries.jar_type_id = jar_types.id
     LEFT JOIN jar_cap_types ON import_entries.jar_cap_type_id = jar_cap_types.id
     LEFT JOIN import_item_types ON import_entries.item_type = import_item_types.code
     LEFT JOIN users ON import_entries.created_by = users.id
     WHERE entry_date BETWEEN ? AND ?
     ${itemClause}
     ${searchClause}
     ORDER BY entry_date DESC, created_at DESC`
  ).all(...params);

  const totals = entries.reduce((acc, row) => {
    const qty = Number(row.quantity || 0);
    const sign = row.direction === "OUT" ? -1 : 1;
    acc.incoming += row.direction === "OUT" ? 0 : qty;
    acc.outgoing += row.direction === "OUT" ? qty : 0;
    acc.total += sign * qty;
    acc.byItem[row.item_type] = (acc.byItem[row.item_type] || 0) + (sign * qty);
    return acc;
  }, { total: 0, incoming: 0, outgoing: 0, byItem: {} });
  const payableTotals = entries.reduce(
    (acc, row) => {
      const total = parseMoneyValue(row.total_amount || 0);
      const paid = parseMoneyValue(row.paid_amount || 0);
      const due = Math.max(0, total - paid);
      acc.total += total;
      acc.paid += paid;
      acc.due += due;
      if (due > 0) acc.open_count += 1;
      return acc;
    },
    { total: 0, paid: 0, due: 0, open_count: 0 }
  );

  const jarTypes = db.prepare("SELECT id, name, default_qty FROM jar_types WHERE active = 1 ORDER BY name").all();
  const jarCapTypes = db.prepare("SELECT id, name, default_qty FROM jar_cap_types WHERE active = 1 ORDER BY name").all();
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
  const importMap = jarImportTotals.reduce((acc, row) => {
    acc[row.jar_type_id] = Number(row.qty || 0);
    return acc;
  }, {});
  const salesMap = jarSalesTotals.reduce((acc, row) => {
    acc[row.jar_type_id] = Number(row.qty || 0);
    return acc;
  }, {});
  const jarTypeBalances = {};
  let jarContainerBalance = 0;
  jarTypes.forEach((type) => {
    const balance = (importMap[type.id] || 0) - (salesMap[type.id] || 0);
    jarTypeBalances[type.id] = balance;
    jarContainerBalance += balance;
  });

  const bottleCaseImportRow = db.prepare(
    `SELECT COALESCE(SUM(CASE WHEN direction = 'OUT' THEN -quantity ELSE quantity END), 0) as qty
     FROM import_entries
     WHERE item_type = 'BOTTLE_CASE'`
  ).get();
  const bottleCaseExportRow = db.prepare(
    `SELECT COALESCE(SUM(bottle_case_count), 0) as exported,
            COALESCE(SUM(return_bottle_case_count), 0) as returned
     FROM exports`
  ).get();
  const bottleCaseImported = Number(bottleCaseImportRow?.qty || 0);
  const bottleCaseExported = Number(bottleCaseExportRow?.exported || 0);
  const bottleCaseReturned = Number(bottleCaseExportRow?.returned || 0);
  const bottleCaseNetExport = Math.max(0, bottleCaseExported - bottleCaseReturned);
  const bottleCaseBalance = bottleCaseImported - bottleCaseNetExport;
  const dispenserImportRow = db.prepare(
    `SELECT COALESCE(SUM(CASE WHEN direction = 'OUT' THEN -quantity ELSE quantity END), 0) as qty
     FROM import_entries
     WHERE item_type = 'DISPENSER'`
  ).get();
  const dispenserExportRow = db.prepare(
    `SELECT COALESCE(SUM(dispenser_count), 0) as exported
     FROM exports`
  ).get();
  const dispenserImported = Number(dispenserImportRow?.qty || 0);
  const dispenserExported = Number(dispenserExportRow?.exported || 0);
  const dispenserBalance = dispenserImported - dispenserExported;

  const itemMetaByCode = itemTypes.reduce((acc, row) => {
    acc[row.code] = {
      label: row.label,
      unit: row.unit,
      usesDirection: Boolean(row.uses_direction)
    };
    return acc;
  }, {});

  const paymentItemClause = item === "all" ? "" : "AND import_entries.item_type = ?";
  const paymentSearchClause = q
    ? "AND (COALESCE(import_entries.seller_name, '') LIKE ? OR COALESCE(import_payments.note, '') LIKE ?)"
    : "";
  const paymentParams = [from, to];
  if (item !== "all") paymentParams.push(item);
  if (q) paymentParams.push(`%${q}%`, `%${q}%`);
  const paymentRows = db.prepare(
    `SELECT import_payments.*, import_entries.item_type, import_entries.entry_date, import_entries.seller_name,
            import_entries.total_amount, import_entries.paid_amount,
            import_item_types.name as item_type_name, import_item_types.unit_label as item_unit_label,
            users.full_name as recorded_by
     FROM import_payments
     JOIN import_entries ON import_payments.import_entry_id = import_entries.id
     LEFT JOIN import_item_types ON import_entries.item_type = import_item_types.code
     LEFT JOIN users ON import_payments.created_by = users.id
     WHERE import_payments.payment_date BETWEEN ? AND ?
     ${paymentItemClause}
     ${paymentSearchClause}
     ORDER BY import_payments.payment_date DESC, import_payments.id DESC`
  ).all(...paymentParams);
  const errorKey = String(req.query.error || "").trim();
  const status = String(req.query.status || "").trim();
  const error = errorKey ? req.t(errorKey) : null;
  const success = status === "payment_saved"
    ? req.t("paymentSaved")
    : status === "payment_deleted"
      ? req.t("paymentDeleted")
      : null;

  res.render("records/imports", {
    title: req.t("importsTitle"),
    from,
    to,
    q,
    item,
    entries,
    totals,
    payableTotals,
    paymentRows,
    error,
    success,
    jarContainerBalance,
    bottleCaseBalance,
    dispenserBalance,
    jarTypeBalances,
    itemTypes,
    itemMetaByCode,
    resolveImportItemLabel,
    resolveImportItemUnit,
    jarTypes,
    jarCapTypes
  });
});

router.post("/imports", (req, res) => {
  const {
    item_type,
    quantity,
    entry_date,
    note,
    direction,
    jar_type_id,
    jar_cap_type_id,
    seller_name,
    total_amount,
    paid_amount,
    is_credit,
    payment_method,
    payment_source
  } = req.body;
  const itemRow = getImportItemTypeByCode(item_type);
  if (!item_type || !entry_date || !itemRow || Number(itemRow.is_active) !== 1) {
    return res.redirect("/records/imports");
  }
  let qty = Number(quantity || 0);
  if (Number.isNaN(qty) || qty <= 0) {
    qty = 0;
  }
  let entryDirection = direction === "OUT" ? "OUT" : "IN";
  if (item_type === "JAR_CONTAINER") {
    entryDirection = "IN";
    if (!jar_type_id) {
      return res.redirect("/records/imports");
    }
    const typeRow = db.prepare("SELECT default_qty FROM jar_types WHERE id = ?").get(jar_type_id);
    if (!typeRow) {
      return res.redirect("/records/imports");
    }
    const defaultQty = Number(typeRow.default_qty || 0);
    if (defaultQty > 0) {
      qty = defaultQty;
    }
  }
  if (item_type === "JAR_CAP") {
    if (!jar_cap_type_id) {
      return res.redirect("/records/imports");
    }
    const capRow = db.prepare("SELECT default_qty FROM jar_cap_types WHERE id = ?").get(jar_cap_type_id);
    if (!capRow) {
      return res.redirect("/records/imports");
    }
    const defaultQty = Number(capRow.default_qty || 0);
    if (qty <= 0 && defaultQty > 0) {
      qty = defaultQty;
    }
  }
  if (item_type !== "JAR_CONTAINER" && Number(itemRow.uses_direction) !== 1) {
    entryDirection = "IN";
  }
  if (qty <= 0) {
    return res.redirect("/records/imports");
  }

  const sellerName = String(seller_name || "").trim();
  const paymentMethod = normalizePaymentMethod(payment_method);
  const paymentSource = normalizeSalaryPaymentSource(payment_source);
  let totalAmount = parseMoneyValue(total_amount);
  let paidAmount = parseMoneyValue(paid_amount);
  const wantsCredit = parseCheckbox(is_credit);
  if (totalAmount <= 0) {
    totalAmount = 0;
    paidAmount = 0;
  }
  if (paidAmount > totalAmount) {
    return res.redirect(`/records/imports?from=${entry_date}&to=${entry_date}&error=paidMoreThanTotal`);
  }
  if (!wantsCredit && totalAmount > 0 && paidAmount < totalAmount) {
    paidAmount = totalAmount;
  }
  const isCreditFinal = totalAmount > paidAmount ? 1 : 0;
  if (isCreditFinal === 1 && !sellerName) {
    return res.redirect(`/records/imports?from=${entry_date}&to=${entry_date}&error=sellerRequiredForCredit`);
  }

  const entryId = db.prepare(
    `INSERT INTO import_entries (
      item_type, quantity, direction, jar_type_id, jar_cap_type_id, entry_date,
      seller_name, total_amount, paid_amount, is_credit, note, created_by
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
  ).run(
    item_type,
    qty,
    entryDirection,
    item_type === "JAR_CONTAINER" ? jar_type_id : null,
    item_type === "JAR_CAP" ? jar_cap_type_id : null,
    entry_date,
    sellerName || null,
    totalAmount,
    paidAmount,
    isCreditFinal,
    note || null,
    req.session.userId
  ).lastInsertRowid;

  if (paidAmount > 0) {
    db.prepare(
      "INSERT INTO import_payments (import_entry_id, payment_date, amount, payment_method, payment_source, note, created_by) VALUES (?, ?, ?, ?, ?, ?, ?)"
    ).run(
      entryId,
      entry_date,
      paidAmount,
      paymentMethod,
      paymentSource,
      req.t("openingPaymentNote"),
      req.session.userId || null
    );
  }

  logActivity({
    userId: req.session.userId,
    action: "create",
    entityType: "import_entry",
    entityId: `${entryId}`,
    details: `direction=${entryDirection}, qty=${qty}, total=${totalAmount}, paid=${paidAmount}, method=${paymentMethod}, source=${paymentSource}`
  });

  res.redirect(`/records/imports?from=${entry_date}&to=${entry_date}`);
});

router.get("/imports/:id/edit", (req, res) => {
  const entry = db.prepare("SELECT * FROM import_entries WHERE id = ?").get(req.params.id);
  if (!entry) return res.redirect("/records/imports");
  const from = dayjs(entry.entry_date).startOf("month").format("YYYY-MM-DD");
  const to = dayjs(entry.entry_date).format("YYYY-MM-DD");
  let itemTypes = getImportItemsForUi(req.t, true);
  if (!itemTypes.find((row) => row.code === entry.item_type)) {
    itemTypes = [
      ...itemTypes,
      {
        id: 0,
        code: entry.item_type,
        name: entry.item_type,
        unit_label: "",
        label: entry.item_type,
        unit: "",
        uses_direction: true,
        is_predefined: 0,
        is_active: 0
      }
    ];
  }
  const itemMetaByCode = itemTypes.reduce((acc, row) => {
    acc[row.code] = {
      label: row.label,
      unit: row.unit,
      usesDirection: Boolean(row.uses_direction)
    };
    return acc;
  }, {});
  const jarTypes = db.prepare("SELECT id, name, default_qty FROM jar_types WHERE active = 1 ORDER BY name").all();
  const jarCapTypes = db.prepare("SELECT id, name, default_qty FROM jar_cap_types WHERE active = 1 ORDER BY name").all();
  const payments = db.prepare(
    `SELECT import_payments.*, users.full_name as recorded_by
     FROM import_payments
     LEFT JOIN users ON import_payments.created_by = users.id
     WHERE import_payments.import_entry_id = ?
     ORDER BY import_payments.payment_date DESC, import_payments.id DESC`
  ).all(req.params.id);
  const paymentTotals = payments.reduce(
    (acc, row) => {
      acc.paid += parseMoneyValue(row.amount || 0);
      return acc;
    },
    { paid: 0 }
  );
  const totalAmount = parseMoneyValue(entry.total_amount || 0);
  const dueAmount = Math.max(0, totalAmount - paymentTotals.paid);
  const errorKey = String(req.query.error || "").trim();
  const status = String(req.query.status || "").trim();
  const error = errorKey ? req.t(errorKey) : null;
  const success = status === "payment_saved"
    ? req.t("paymentSaved")
    : status === "payment_deleted"
      ? req.t("paymentDeleted")
      : null;

  res.render("records/imports_edit", {
    title: req.t("editImportEntryTitle"),
    entry,
    itemTypes,
    itemMetaByCode,
    resolveImportItemLabel,
    resolveImportItemUnit,
    jarTypes,
    jarCapTypes,
    payments,
    paymentTotals,
    dueAmount,
    error,
    success,
    from,
    to
  });
});

router.post("/imports/:id", (req, res) => {
  const entry = db.prepare("SELECT * FROM import_entries WHERE id = ?").get(req.params.id);
  if (!entry) return res.redirect("/records/imports");
  const {
    item_type,
    quantity,
    entry_date,
    note,
    direction,
    jar_type_id,
    jar_cap_type_id,
    seller_name,
    total_amount,
    is_credit,
    payment_method,
    payment_source
  } = req.body;
  const itemRow = getImportItemTypeByCode(item_type);
  if (!item_type || !entry_date || !itemRow) {
    return res.redirect(`/records/imports/${req.params.id}/edit`);
  }
  let qty = Number(quantity || 0);
  if (Number.isNaN(qty) || qty <= 0) {
    qty = 0;
  }
  let entryDirection = direction === "OUT" ? "OUT" : "IN";
  if (item_type === "JAR_CONTAINER") {
    entryDirection = "IN";
    if (!jar_type_id) {
      return res.redirect(`/records/imports/${req.params.id}/edit`);
    }
    const typeRow = db.prepare("SELECT default_qty FROM jar_types WHERE id = ?").get(jar_type_id);
    if (!typeRow) {
      return res.redirect(`/records/imports/${req.params.id}/edit`);
    }
    const defaultQty = Number(typeRow.default_qty || 0);
    if (defaultQty > 0 && qty <= 0) {
      qty = defaultQty;
    }
  }
  if (item_type === "JAR_CAP") {
    if (!jar_cap_type_id) {
      return res.redirect(`/records/imports/${req.params.id}/edit`);
    }
    const capRow = db.prepare("SELECT default_qty FROM jar_cap_types WHERE id = ?").get(jar_cap_type_id);
    if (!capRow) {
      return res.redirect(`/records/imports/${req.params.id}/edit`);
    }
    const defaultQty = Number(capRow.default_qty || 0);
    if (qty <= 0 && defaultQty > 0) {
      qty = defaultQty;
    }
  }
  if (item_type !== "JAR_CONTAINER" && Number(itemRow.uses_direction) !== 1) {
    entryDirection = "IN";
  }
  if (qty <= 0) {
    return res.redirect(`/records/imports/${req.params.id}/edit`);
  }

  const sellerName = String(seller_name || "").trim();
  const paymentMethod = normalizePaymentMethod(payment_method);
  const paymentSource = normalizeSalaryPaymentSource(payment_source);
  let totalAmount = parseMoneyValue(total_amount);
  if (totalAmount < 0) totalAmount = 0;
  const paidRow = db.prepare(
    "SELECT COALESCE(SUM(amount), 0) as paid FROM import_payments WHERE import_entry_id = ?"
  ).get(req.params.id);
  const existingPaid = parseMoneyValue(paidRow?.paid || 0);
  if (totalAmount < existingPaid) {
    return res.redirect(`/records/imports/${req.params.id}/edit?error=totalBelowPaid`);
  }
  const wantsCredit = parseCheckbox(is_credit);
  let finalPaidAmount = existingPaid;
  if (!wantsCredit && totalAmount > existingPaid) {
    const remaining = parseMoneyValue(totalAmount - existingPaid);
    if (remaining > 0) {
      db.prepare(
        "INSERT INTO import_payments (import_entry_id, payment_date, amount, payment_method, payment_source, note, created_by) VALUES (?, ?, ?, ?, ?, ?, ?)"
      ).run(
        req.params.id,
        entry_date,
        remaining,
        paymentMethod,
        paymentSource,
        req.t("autoSettledOnEdit"),
        req.session.userId || null
      );
      finalPaidAmount = parseMoneyValue(existingPaid + remaining);
    }
  }
  const isCreditFinal = totalAmount > finalPaidAmount ? 1 : 0;
  if (isCreditFinal === 1 && !sellerName) {
    return res.redirect(`/records/imports/${req.params.id}/edit?error=sellerRequiredForCredit`);
  }

  db.prepare(
    `UPDATE import_entries
     SET item_type = ?, quantity = ?, direction = ?, jar_type_id = ?, jar_cap_type_id = ?, entry_date = ?,
         seller_name = ?, total_amount = ?, paid_amount = ?, is_credit = ?, note = ?
     WHERE id = ?`
  ).run(
    item_type,
    qty,
    entryDirection,
    item_type === "JAR_CONTAINER" ? jar_type_id : null,
    item_type === "JAR_CAP" ? jar_cap_type_id : null,
    entry_date,
    sellerName || null,
    totalAmount,
    finalPaidAmount,
    isCreditFinal,
    note || null,
    req.params.id
  );

  logActivity({
    userId: req.session.userId,
    action: "update",
    entityType: "import_entry",
    entityId: req.params.id,
    details: buildDiffDetails(
      entry,
      {
        item_type,
        quantity: qty,
        direction: entryDirection,
        jar_type_id: item_type === "JAR_CONTAINER" ? Number(jar_type_id) : null,
        jar_cap_type_id: item_type === "JAR_CAP" ? Number(jar_cap_type_id) : null,
        entry_date,
        seller_name: sellerName || null,
        total_amount: totalAmount,
        paid_amount: finalPaidAmount,
        is_credit: isCreditFinal,
        note: note || null
      },
      [
        "item_type",
        "quantity",
        "direction",
        "jar_type_id",
        "jar_cap_type_id",
        "entry_date",
        "seller_name",
        "total_amount",
        "paid_amount",
        "is_credit",
        "note"
      ]
    )
  });

  res.redirect(`/records/imports?from=${entry_date}&to=${entry_date}`);
});

router.post("/imports/:id/payments", (req, res) => {
  const entry = db.prepare("SELECT * FROM import_entries WHERE id = ?").get(req.params.id);
  if (!entry) return res.redirect("/records/imports");
  const paymentDate = String(req.body.payment_date || "").trim() || dayjs().format("YYYY-MM-DD");
  const amount = parseMoneyValue(req.body.amount);
  const paymentMethod = normalizePaymentMethod(req.body.payment_method);
  const paymentSource = normalizeSalaryPaymentSource(req.body.payment_source);
  const note = String(req.body.note || "").trim();
  if (!paymentDate || amount <= 0) {
    return res.redirect(`/records/imports/${req.params.id}/edit?error=invalidPaymentAmount#payment-form`);
  }
  const totalAmount = parseMoneyValue(entry.total_amount || 0);
  const paidAmount = parseMoneyValue(entry.paid_amount || 0);
  const remaining = Math.max(0, totalAmount - paidAmount);
  if (remaining <= 0) {
    return res.redirect(`/records/imports/${req.params.id}/edit?error=noBalanceDue#payment-form`);
  }
  if (amount > remaining) {
    return res.redirect(`/records/imports/${req.params.id}/edit?error=paidMoreThanDue#payment-form`);
  }

  const paymentId = db.prepare(
    "INSERT INTO import_payments (import_entry_id, payment_date, amount, payment_method, payment_source, note, created_by) VALUES (?, ?, ?, ?, ?, ?, ?)"
  ).run(
    req.params.id,
    paymentDate,
    amount,
    paymentMethod,
    paymentSource,
    note || null,
    req.session.userId || null
  ).lastInsertRowid;

  const updatedPaid = parseMoneyValue(paidAmount + amount);
  const updatedCredit = updatedPaid < totalAmount ? 1 : 0;
  db.prepare("UPDATE import_entries SET paid_amount = ?, is_credit = ? WHERE id = ?").run(
    updatedPaid,
    updatedCredit,
    req.params.id
  );

  logActivity({
    userId: req.session.userId,
    action: "create",
    entityType: "import_payment",
    entityId: paymentId,
    details: `entry=${req.params.id}, amount=${amount}, date=${paymentDate}, method=${paymentMethod}, source=${paymentSource}`
  });

  return res.redirect(`/records/imports/${req.params.id}/edit?status=payment_saved#payment-history`);
});

router.post("/imports/payments/:id/delete", (req, res) => {
  const payment = db.prepare("SELECT * FROM import_payments WHERE id = ?").get(req.params.id);
  if (!payment) return res.redirect("/records/imports");
  const entry = db.prepare("SELECT * FROM import_entries WHERE id = ?").get(payment.import_entry_id);
  if (!entry) {
    db.prepare("DELETE FROM import_payments WHERE id = ?").run(req.params.id);
    return res.redirect("/records/imports");
  }

  db.prepare("DELETE FROM import_payments WHERE id = ?").run(req.params.id);
  const paidRow = db.prepare(
    "SELECT COALESCE(SUM(amount), 0) as paid FROM import_payments WHERE import_entry_id = ?"
  ).get(payment.import_entry_id);
  const newPaid = parseMoneyValue(paidRow?.paid || 0);
  const totalAmount = parseMoneyValue(entry.total_amount || 0);
  db.prepare("UPDATE import_entries SET paid_amount = ?, is_credit = ? WHERE id = ?").run(
    newPaid,
    newPaid < totalAmount ? 1 : 0,
    payment.import_entry_id
  );

  logActivity({
    userId: req.session.userId,
    action: "delete",
    entityType: "import_payment",
    entityId: req.params.id,
    details: `entry=${payment.import_entry_id}, amount=${payment.amount || 0}`
  });

  return res.redirect(`/records/imports/${payment.import_entry_id}/edit?status=payment_deleted#payment-history`);
});

router.post("/imports/:id/delete", (req, res) => {
  const entry = db.prepare("SELECT * FROM import_entries WHERE id = ?").get(req.params.id);
  if (!entry) return res.redirect("/records/imports");
  const payments = db.prepare(
    "SELECT * FROM import_payments WHERE import_entry_id = ? ORDER BY payment_date ASC, id ASC"
  ).all(req.params.id);
  const recycleId = createRecycleEntry({
    entityType: "import_entry",
    entityId: req.params.id,
    payload: { import_entry: entry, import_payments: payments },
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
  db.prepare("DELETE FROM import_payments WHERE import_entry_id = ?").run(req.params.id);
  db.prepare("DELETE FROM import_entries WHERE id = ?").run(req.params.id);
  const back = req.get("referer");
  res.redirect(back || "/records/imports");
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
    due_salary: computeSalaryDue(row, row.paid_total, today)
  }));

  const success = req.query.archived
    ? req.t("staffArchived")
    : req.query.unarchived || req.query.activated
      ? req.t("staffActivated")
      : null;

  res.render("admin/staffs", {
    title: req.t("staffsTitle"),
    staffs,
    basePath: "/records/staffs",
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
    basePath: "/records/staffs"
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
        basePath: "/records/staffs"
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
        basePath: "/records/staffs"
      });
    }
    const salary = Number(monthly_salary || 0);
    if (Number.isNaN(salary) || salary < 0) {
      return res.render("admin/staff_form", {
        title: req.t("addStaffTitle"),
        staff: null,
        document: null,
        error: req.t("salaryInvalid"),
        staffRoles: getStaffRoleChoices(req.t, staff_role),
        basePath: "/records/staffs"
      });
    }
    const fingerprintId = normalizeFingerprintId(fingerprint_id);
    const fpConflict = findFingerprintConflict({ fingerprintId });
    if (fpConflict) {
      return res.render("admin/staff_form", {
        title: req.t("addStaffTitle"),
        staff: null,
        document: null,
        error: req.t("fingerprintIdAlreadyUsed"),
        staffRoles: getStaffRoleChoices(req.t, staff_role),
        basePath: "/records/staffs"
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

    res.redirect("/records/staffs");
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
  if (!staff) return res.redirect("/records/staffs");
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
  const dueSalary = computeSalaryDue(staff, totals.total, dayjs().format("YYYY-MM-DD"));
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
    basePath: "/records/staffs"
  });
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
  if (!staff) return res.redirect("/records/staffs");
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
  const dueSalary = computeSalaryDue(staff, totals.total, dayjs().format("YYYY-MM-DD"));
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
  if (!staff) return res.redirect("/records/staffs");
  const document = db.prepare("SELECT * FROM staff_documents WHERE staff_id = ?").get(req.params.id);
  res.render("admin/staff_form", {
    title: req.t("editStaffTitle"),
    staff,
    document,
    error: null,
    staffRoles: getStaffRoleChoices(req.t, staff.staff_role),
    basePath: "/records/staffs"
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
    if (!staff) return res.redirect("/records/staffs");
    const document = db.prepare("SELECT * FROM staff_documents WHERE staff_id = ?").get(req.params.id);
    if (err) {
      return res.render("admin/staff_form", {
        title: req.t("editStaffTitle"),
        staff,
        document,
        error: err.message || req.t("uploadError"),
        staffRoles: getStaffRoleChoices(req.t, req.body ? req.body.staff_role : staff.staff_role),
        basePath: "/records/staffs"
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
        basePath: "/records/staffs"
      });
    }
    const salary = Number(monthly_salary || 0);
    if (Number.isNaN(salary) || salary < 0) {
      return res.render("admin/staff_form", {
        title: req.t("editStaffTitle"),
        staff,
        document,
        error: req.t("salaryInvalid"),
        staffRoles: getStaffRoleChoices(req.t, staff_role || staff.staff_role),
        basePath: "/records/staffs"
      });
    }
    const fingerprintId = normalizeFingerprintId(fingerprint_id);
    const fpConflict = findFingerprintConflict({ fingerprintId, skipStaffId: req.params.id });
    if (fpConflict) {
      return res.render("admin/staff_form", {
        title: req.t("editStaffTitle"),
        staff,
        document,
        error: req.t("fingerprintIdAlreadyUsed"),
        staffRoles: getStaffRoleChoices(req.t, staff_role || staff.staff_role),
        basePath: "/records/staffs"
      });
    }
    const staffRole = normalizeStaffRole(staff_role, { allowInactive: true });
    const safeDocType = normalizeDocumentType(doc_type);

    let photoPath = staff.photo_path;
    const photoFile = req.files && req.files.photo ? req.files.photo[0] : null;
    if (photoFile) photoPath = `/uploads/${photoFile.filename}`;

    db.prepare(
      "UPDATE staff SET full_name = ?, staff_role = ?, phone = ?, fingerprint_id = ?, photo_path = ?, start_date = ?, monthly_salary = ?, updated_at = datetime('now') WHERE id = ?"
    ).run(full_name.trim(), staffRole, phone || null, fingerprintId, photoPath, start_date || null, salary, req.params.id);

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

    res.redirect(`/records/staffs/${req.params.id}`);
  });
});

router.post("/staffs/:id/archive", (req, res) => {
  const staff = db.prepare("SELECT id, full_name, is_active FROM staff WHERE id = ?").get(req.params.id);
  if (!staff) return res.redirect("/records/staffs");
  setStaffActiveStatus(req.params.id, false, req.session.userId);
  logActivity({
    userId: req.session.userId,
    action: "update",
    entityType: "staff",
    entityId: req.params.id,
    details: `status=archived, name=${staff.full_name || ""}`
  });
  res.redirect("/records/staffs?archived=1&include_inactive=1");
});

router.post("/staffs/:id/activate", (req, res) => {
  const staff = db.prepare("SELECT id, full_name FROM staff WHERE id = ?").get(req.params.id);
  if (!staff) return res.redirect("/records/staffs");
  setStaffActiveStatus(req.params.id, true, req.session.userId);
  logActivity({
    userId: req.session.userId,
    action: "update",
    entityType: "staff",
    entityId: req.params.id,
    details: `status=active, name=${staff.full_name || ""}`
  });
  res.redirect("/records/staffs?activated=1&include_inactive=1");
});

router.post("/staffs/:id/delete", (req, res) => {
  res.redirect(307, `/records/staffs/${req.params.id}/archive`);
});

router.post("/staffs/:id/payments", (req, res) => {
  const staff = db.prepare("SELECT * FROM staff WHERE id = ?").get(req.params.id);
  if (!staff) return res.redirect("/records/staffs");
  const { payment_date, amount, payment_type, payment_source, note, print } = req.body;
  const amt = Number(amount || 0);
  if (!payment_date || Number.isNaN(amt) || amt <= 0) {
    return res.redirect(`/records/staffs/${req.params.id}`);
  }
  const type = payment_type === "ADVANCE" ? "ADVANCE" : "SALARY";
  const source = normalizeSalaryPaymentSource(payment_source);

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
    return res.redirect(`/records/staffs/payments/${paymentId}/print`);
  }
  res.redirect(`/records/staffs/${req.params.id}`);
});

router.get("/staffs/payments/:id/edit", (req, res) => {
  const payment = db.prepare(
    `SELECT staff_salary_payments.*, staff.full_name
     FROM staff_salary_payments
     JOIN staff ON staff_salary_payments.staff_id = staff.id
     WHERE staff_salary_payments.id = ?`
  ).get(req.params.id);
  if (!payment) return res.redirect("/records/staffs");
  res.render("admin/staff_payment_form", {
    title: req.t("editSalaryPaymentTitle"),
    payment,
    staff: { id: payment.staff_id, full_name: payment.full_name },
    basePath: "/records/staffs"
  });
});

router.post("/staffs/payments/:id", (req, res) => {
  const payment = db.prepare("SELECT * FROM staff_salary_payments WHERE id = ?").get(req.params.id);
  if (!payment) return res.redirect("/records/staffs");
  const { payment_date, amount, payment_type, payment_source, note, print } = req.body;
  const amt = Number(amount || 0);
  if (!payment_date || Number.isNaN(amt) || amt <= 0) {
    return res.redirect(`/records/staffs/payments/${req.params.id}/edit`);
  }
  const type = payment_type === "ADVANCE" ? "ADVANCE" : "SALARY";
  const source = normalizeSalaryPaymentSource(payment_source);

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
    return res.redirect(`/records/staffs/payments/${req.params.id}/print`);
  }
  res.redirect(`/records/staffs/${payment.staff_id}`);
});

router.post("/staffs/payments/:id/delete", (req, res) => {
  const payment = db.prepare("SELECT * FROM staff_salary_payments WHERE id = ?").get(req.params.id);
  if (!payment) return res.redirect("/records/staffs");
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
  res.redirect(`/records/staffs/${payment.staff_id}`);
});

router.get("/staffs/payments/:id/print", (req, res) => {
  const payment = db.prepare(
    `SELECT staff_salary_payments.*, staff.full_name, staff.phone, staff.photo_path
     FROM staff_salary_payments
     JOIN staff ON staff_salary_payments.staff_id = staff.id
     WHERE staff_salary_payments.id = ?`
  ).get(req.params.id);
  if (!payment) return res.redirect("/records/staffs");
  res.render("admin/staff_payment_print", { title: req.t("staffPaySlipTitle"), payment });
});

router.get("/staff-attendance", (req, res) => {
  const date = req.query.date || dayjs().format("YYYY-MM-DD");
  const staffs = db.prepare(
    "SELECT id, full_name, phone, photo_path, fingerprint_id FROM staff WHERE COALESCE(is_active, 1) = 1 ORDER BY full_name"
  ).all();
  const attendanceRows = db.prepare(
    "SELECT staff_id, status FROM staff_attendance WHERE attendance_date = ?"
  ).all(date);
  const attendanceMap = attendanceRows.reduce((acc, row) => {
    acc[row.staff_id] = row.status;
    return acc;
  }, {});
  const saved = req.query.saved === "1" ? req.t("attendanceSaved") : null;
  const iotEnabled = getAttendanceIotEnabled();
  const iotSaved = req.query.iot === "saved" ? req.t("attendanceSaved") : null;
  const iotErrorKey = String(req.query.error || "").trim();
  const iotError = iotErrorKey ? req.t(iotErrorKey) : null;
  const iotLogs = db.prepare(
    `SELECT iot_attendance_logs.*, users.full_name as recorded_by_name
     FROM iot_attendance_logs
     LEFT JOIN users ON iot_attendance_logs.recorded_by = users.id
     WHERE iot_attendance_logs.person_type = 'STAFF'
       AND iot_attendance_logs.attendance_date = ?
     ORDER BY iot_attendance_logs.scanned_at DESC, iot_attendance_logs.id DESC
     LIMIT 20`
  ).all(date);

  res.render("records/staff_attendance", {
    title: req.t("staffAttendanceTitle"),
    date,
    staffs,
    attendanceMap,
    saved,
    iotEnabled,
    iotSaved,
    iotError,
    iotLogs
  });
});

router.post("/staff-attendance", (req, res) => {
  const attendanceDate = req.body.attendance_date || dayjs().format("YYYY-MM-DD");
  const staffs = db.prepare("SELECT id FROM staff WHERE COALESCE(is_active, 1) = 1").all();
  const userId = req.session.userId || null;
  const upsert = db.prepare(
    `INSERT INTO staff_attendance (staff_id, attendance_date, status, recorded_by, created_at, updated_at)
     VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))
     ON CONFLICT(staff_id, attendance_date)
     DO UPDATE SET status = excluded.status, recorded_by = excluded.recorded_by, updated_at = datetime('now')`
  );
  try {
    db.exec("BEGIN");
    staffs.forEach((staff) => {
      const status = req.body[`status_${staff.id}`];
      if (!status) return;
      const finalStatus = status === "PRESENT" ? "PRESENT" : "ABSENT";
      upsert.run(staff.id, attendanceDate, finalStatus, userId);
    });
    db.exec("COMMIT");
  } catch (err) {
    db.exec("ROLLBACK");
    throw err;
  }

  logActivity({
    userId,
    action: "update",
    entityType: "staff_attendance",
    entityId: attendanceDate,
    details: `date=${attendanceDate}`
  });

  res.redirect(`/records/staff-attendance?date=${attendanceDate}&saved=1`);
});

router.post("/staff-attendance/iot-mark", (req, res) => {
  const attendanceDate = req.body.attendance_date || dayjs().format("YYYY-MM-DD");
  if (!getAttendanceIotEnabled()) {
    return res.redirect(`/records/staff-attendance?date=${attendanceDate}&error=iotAttendanceDisabled`);
  }
  const fingerprintId = normalizeFingerprintId(req.body.fingerprint_id);
  if (!fingerprintId) {
    return res.redirect(`/records/staff-attendance?date=${attendanceDate}&error=fingerprintIdRequired`);
  }
  const status = req.body.status === "ABSENT" ? "ABSENT" : "PRESENT";
  const staff = db.prepare(
    "SELECT id, full_name FROM staff WHERE COALESCE(is_active, 1) = 1 AND lower(trim(fingerprint_id)) = lower(trim(?))"
  ).get(fingerprintId);
  if (!staff) {
    return res.redirect(`/records/staff-attendance?date=${attendanceDate}&error=fingerprintIdNotMapped`);
  }
  const userId = req.session.userId || null;
  db.prepare(
    `INSERT INTO staff_attendance (staff_id, attendance_date, status, recorded_by, created_at, updated_at)
     VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))
     ON CONFLICT(staff_id, attendance_date)
     DO UPDATE SET status = excluded.status, recorded_by = excluded.recorded_by, updated_at = datetime('now')`
  ).run(staff.id, attendanceDate, status, userId);
  db.prepare(
    `INSERT INTO iot_attendance_logs (source, person_type, person_id, fingerprint_id, status, attendance_date, scanned_at, note, recorded_by)
     VALUES ('MANUAL', 'STAFF', ?, ?, ?, ?, datetime('now'), ?, ?)`
  ).run(staff.id, fingerprintId, status, attendanceDate, "manual_attendance_form", userId);

  logActivity({
    userId,
    action: "update",
    entityType: "staff_attendance_iot",
    entityId: attendanceDate,
    details: `staff=${staff.id}, fingerprint=${fingerprintId}, status=${status}`
  });

  return res.redirect(`/records/staff-attendance?date=${attendanceDate}&iot=saved`);
});

router.get("/staff-attendance/print", (req, res) => {
  const from = req.query.from || dayjs().format("YYYY-MM-DD");
  const to = req.query.to || from;
  const rows = db.prepare(
    `SELECT staff_attendance.*, staff.full_name, staff.phone, users.full_name as recorded_by
     FROM staff_attendance
     JOIN staff ON staff_attendance.staff_id = staff.id
     LEFT JOIN users ON staff_attendance.recorded_by = users.id
     WHERE attendance_date BETWEEN ? AND ?
       AND COALESCE(staff.is_active, 1) = 1
     ORDER BY attendance_date DESC, staff.full_name`
  ).all(from, to);
  const totals = rows.reduce(
    (acc, row) => {
      if (row.status === "PRESENT") acc.present += 1;
      else acc.absent += 1;
      return acc;
    },
    { present: 0, absent: 0 }
  );
  res.render("records/staff_attendance_print", {
    title: req.t("staffAttendanceTitle"),
    from,
    to,
    rows,
    totals
  });
});

router.get("/profile", (req, res) => {
  const user = attachWorkerPhoto(res.locals.currentUser);
  if (!user) return res.redirect("/login");
  const attendancePayload = getUserAttendancePayload(user.id);
  res.render("admin/profile", {
    title: req.t("profileTitle"),
    user,
    error: null,
    success: null,
    profileAction: "/records/profile",
    attendanceAction: "/records/profile/attendance",
    ...attendancePayload,
    attendanceSaved: req.query.saved === "1" ? req.t("attendanceSaved") : null
  });
});

router.post("/profile", (req, res) => {
  const user = attachWorkerPhoto(res.locals.currentUser);
  const { full_name, phone, current_password, new_password } = req.body;
  if (!full_name) {
    const attendancePayload = getUserAttendancePayload(user.id);
    return res.render("admin/profile", {
      title: req.t("profileTitle"),
      user: { ...user, full_name, phone },
      error: req.t("fullNameRequired"),
      success: null,
      profileAction: "/records/profile",
      attendanceAction: "/records/profile/attendance",
      ...attendancePayload
    });
  }

  db.prepare("UPDATE users SET full_name = ?, phone = ?, updated_at = datetime('now') WHERE id = ?")
    .run(full_name.trim(), phone ? phone.trim() : null, user.id);

  if (new_password) {
    const dbUser = db.prepare("SELECT password_hash FROM users WHERE id = ?").get(user.id);
    if (!current_password || !dbUser || !bcrypt.compareSync(current_password, dbUser.password_hash)) {
      const refreshed = db.prepare("SELECT id, username, full_name, phone, role FROM users WHERE id = ?").get(user.id);
      const attendancePayload = getUserAttendancePayload(user.id);
      return res.render("admin/profile", {
        title: req.t("profileTitle"),
        user: attachWorkerPhoto(refreshed),
        error: req.t("currentPasswordInvalid"),
        success: null,
        profileAction: "/records/profile",
        attendanceAction: "/records/profile/attendance",
        ...attendancePayload
      });
    }
    const hash = bcrypt.hashSync(new_password, 10);
    db.prepare("UPDATE users SET password_hash = ? WHERE id = ?").run(hash, user.id);
  }

  const refreshed = db.prepare("SELECT id, username, full_name, phone, role FROM users WHERE id = ?").get(user.id);
  const attendancePayload = getUserAttendancePayload(user.id);
  res.render("admin/profile", {
    title: req.t("profileTitle"),
    user: attachWorkerPhoto(refreshed),
    error: null,
    success: req.t("profileSaved"),
    profileAction: "/records/profile",
    attendanceAction: "/records/profile/attendance",
    ...attendancePayload
  });
});

router.post("/profile/attendance", (req, res) => {
  const user = res.locals.currentUser;
  if (!user) return res.redirect("/login");
  const attendanceDate = dayjs().format("YYYY-MM-DD");
  const status = req.body.status === "PRESENT" ? "PRESENT" : "ABSENT";
  db.prepare(
    `INSERT INTO user_attendance (user_id, attendance_date, status, recorded_by, created_at, updated_at)
     VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))
     ON CONFLICT(user_id, attendance_date)
     DO UPDATE SET status = excluded.status, recorded_by = excluded.recorded_by, updated_at = datetime('now')`
  ).run(user.id, attendanceDate, status, user.id);
  logActivity({
    userId: user.id,
    action: "update",
    entityType: "user_attendance",
    entityId: attendanceDate,
    details: `user=${user.id}, status=${status}`
  });
  res.redirect(`/records/profile?saved=1`);
});

router.get("/company-purchases", (req, res) => {
  const from = req.query.from || dayjs().startOf("month").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const q = String(req.query.q || "").trim();
  const searchClause = q
    ? "AND (company_purchases.item_name LIKE ? OR COALESCE(company_purchases.seller_name, '') LIKE ? OR company_purchases.machinery_name LIKE ? OR company_purchases.technician_name LIKE ? OR company_purchases.technician_phone LIKE ? OR company_purchases.work_details LIKE ?)"
    : "";
  const params = q
    ? [from, to, `%${q}%`, `%${q}%`, `%${q}%`, `%${q}%`, `%${q}%`, `%${q}%`]
    : [from, to];
  const rows = db.prepare(
    `SELECT company_purchases.*, users.full_name as recorded_by
     FROM company_purchases
     LEFT JOIN users ON company_purchases.created_by = users.id
     WHERE company_purchases.purchase_date BETWEEN ? AND ?
     ${searchClause}
     ORDER BY company_purchases.purchase_date DESC, company_purchases.created_at DESC`
  ).all(...params);
  const totals = rows.reduce(
    (acc, row) => {
      acc.total += Number(row.amount || 0);
      acc.paid += Number(row.paid_amount || 0);
      acc.due += Math.max(0, Number(row.amount || 0) - Number(row.paid_amount || 0));
      if (Math.max(0, Number(row.amount || 0) - Number(row.paid_amount || 0)) > 0) acc.open_count += 1;
      if (Number(row.is_machinery) === 1) acc.machinery += Number(row.amount || 0);
      else acc.general += Number(row.amount || 0);
      return acc;
    },
    { total: 0, paid: 0, due: 0, open_count: 0, machinery: 0, general: 0 }
  );

  const paymentRows = db.prepare(
    `SELECT company_purchase_payments.*, company_purchases.item_name, company_purchases.seller_name,
            users.full_name as recorded_by
     FROM company_purchase_payments
     JOIN company_purchases ON company_purchase_payments.company_purchase_id = company_purchases.id
     LEFT JOIN users ON company_purchase_payments.created_by = users.id
     WHERE company_purchase_payments.payment_date BETWEEN ? AND ?
       ${q ? "AND (company_purchases.item_name LIKE ? OR COALESCE(company_purchases.seller_name, '') LIKE ? OR COALESCE(company_purchase_payments.note, '') LIKE ?)" : ""}
     ORDER BY company_purchase_payments.payment_date DESC, company_purchase_payments.id DESC`
  ).all(...(q ? [from, to, `%${q}%`, `%${q}%`, `%${q}%`] : [from, to]));

  const status = req.query.status || "";
  const errorKey = req.query.error || "";
  const error = errorKey ? req.t(errorKey) : null;
  const success = status === "saved"
    ? req.t("purchaseSaved")
    : status === "updated"
      ? req.t("purchaseUpdated")
      : status === "deleted"
        ? req.t("purchaseDeleted")
        : status === "payment_saved"
          ? req.t("paymentSaved")
          : status === "payment_deleted"
            ? req.t("paymentDeleted")
        : null;

  res.render("records/company_purchases", {
    title: req.t("companyPurchasesTitle"),
    from,
    to,
    q,
    rows,
    totals,
    paymentRows,
    error,
    success
  });
});

router.post("/company-purchases", (req, res) => {
  const {
    purchase_date,
    item_name,
    amount,
    seller_name,
    paid_amount,
    is_credit,
    payment_method,
    payment_source,
    is_machinery,
    machinery_name,
    technician_name,
    technician_phone,
    work_details,
    note
  } = req.body;

  const amountNum = parseMoneyValue(amount);
  const paymentMethod = normalizePaymentMethod(payment_method);
  const paymentSource = normalizeSalaryPaymentSource(payment_source);
  const isMachinery = is_machinery === "1" || is_machinery === "on";
  const sellerName = String(seller_name || "").trim();
  let paidAmount = parseMoneyValue(paid_amount);
  const wantsCredit = parseCheckbox(is_credit);
  if (!purchase_date || !item_name || Number.isNaN(amountNum) || amountNum <= 0) {
    return res.redirect(`/records/company-purchases?error=purchaseRequired`);
  }
  if (isMachinery && !String(technician_name || "").trim()) {
    return res.redirect(`/records/company-purchases?error=purchaseTechnicianRequired`);
  }
  if (paidAmount > amountNum) {
    return res.redirect(`/records/company-purchases?error=paidMoreThanTotal`);
  }
  if (!wantsCredit && paidAmount < amountNum) {
    paidAmount = amountNum;
  }
  const isCreditFinal = amountNum > paidAmount ? 1 : 0;
  if (isCreditFinal === 1 && !sellerName) {
    return res.redirect(`/records/company-purchases?error=sellerRequiredForCredit`);
  }

  const purchaseId = db.prepare(
    `INSERT INTO company_purchases (
       purchase_date, item_name, seller_name, amount, paid_amount, is_credit, is_machinery, machinery_name, technician_name,
       technician_phone, work_details, note, created_by
     ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
  ).run(
    purchase_date,
    String(item_name).trim(),
    sellerName || null,
    amountNum,
    paidAmount,
    isCreditFinal,
    isMachinery ? 1 : 0,
    isMachinery ? (machinery_name ? String(machinery_name).trim() : null) : null,
    isMachinery ? String(technician_name).trim() : null,
    isMachinery ? (technician_phone ? String(technician_phone).trim() : null) : null,
    isMachinery ? (work_details ? String(work_details).trim() : null) : null,
    note ? String(note).trim() : null,
    req.session.userId || null
  ).lastInsertRowid;

  if (paidAmount > 0) {
    db.prepare(
      `INSERT INTO company_purchase_payments (company_purchase_id, payment_date, amount, payment_method, payment_source, note, created_by)
       VALUES (?, ?, ?, ?, ?, ?, ?)`
    ).run(
      purchaseId,
      purchase_date,
      paidAmount,
      paymentMethod,
      paymentSource,
      req.t("openingPaymentNote"),
      req.session.userId || null
    );
  }

  logActivity({
    userId: req.session.userId,
    action: "create",
    entityType: "company_purchase",
    entityId: purchaseId,
    details: `item=${String(item_name).trim()}, amount=${amountNum}, paid=${paidAmount}, method=${paymentMethod}, source=${paymentSource}, machinery=${isMachinery ? 1 : 0}`
  });

  res.redirect(`/records/company-purchases?from=${purchase_date}&to=${purchase_date}&status=saved`);
});

router.get("/company-purchases/:id/edit", (req, res) => {
  const row = db.prepare("SELECT * FROM company_purchases WHERE id = ?").get(req.params.id);
  if (!row) return res.redirect("/records/company-purchases");
  const payments = db.prepare(
    `SELECT company_purchase_payments.*, users.full_name as recorded_by
     FROM company_purchase_payments
     LEFT JOIN users ON company_purchase_payments.created_by = users.id
     WHERE company_purchase_payments.company_purchase_id = ?
     ORDER BY company_purchase_payments.payment_date DESC, company_purchase_payments.id DESC`
  ).all(req.params.id);
  const paymentTotals = payments.reduce(
    (acc, entry) => {
      acc.paid += parseMoneyValue(entry.amount || 0);
      return acc;
    },
    { paid: 0 }
  );
  const dueAmount = Math.max(0, parseMoneyValue(row.amount || 0) - paymentTotals.paid);
  const errorKey = String(req.query.error || "").trim();
  const status = String(req.query.status || "").trim();
  const error = errorKey ? req.t(errorKey) : null;
  const success = status === "payment_saved"
    ? req.t("paymentSaved")
    : status === "payment_deleted"
      ? req.t("paymentDeleted")
      : null;
  res.render("records/company_purchase_form", {
    title: req.t("editPurchaseTitle"),
    record: row,
    error,
    success,
    payments,
    paymentTotals,
    dueAmount
  });
});

router.post("/company-purchases/:id", (req, res) => {
  const existing = db.prepare("SELECT * FROM company_purchases WHERE id = ?").get(req.params.id);
  if (!existing) return res.redirect("/records/company-purchases");
  const {
    purchase_date,
    item_name,
    amount,
    seller_name,
    is_credit,
    payment_method,
    payment_source,
    is_machinery,
    machinery_name,
    technician_name,
    technician_phone,
    work_details,
    note
  } = req.body;
  const amountNum = parseMoneyValue(amount);
  const paymentMethod = normalizePaymentMethod(payment_method);
  const paymentSource = normalizeSalaryPaymentSource(payment_source);
  const isMachinery = is_machinery === "1" || is_machinery === "on";
  const sellerName = String(seller_name || "").trim();
  const wantsCredit = parseCheckbox(is_credit);
  if (!purchase_date || !item_name || Number.isNaN(amountNum) || amountNum <= 0) {
    return res.redirect(`/records/company-purchases/${req.params.id}/edit?error=purchaseRequired`);
  }
  if (isMachinery && !String(technician_name || "").trim()) {
    return res.redirect(`/records/company-purchases/${req.params.id}/edit?error=purchaseTechnicianRequired`);
  }

  const paidRow = db.prepare(
    "SELECT COALESCE(SUM(amount), 0) as paid FROM company_purchase_payments WHERE company_purchase_id = ?"
  ).get(req.params.id);
  const existingPaid = parseMoneyValue(paidRow?.paid || 0);
  if (amountNum < existingPaid) {
    return res.redirect(`/records/company-purchases/${req.params.id}/edit?error=totalBelowPaid`);
  }
  let finalPaidAmount = existingPaid;
  if (!wantsCredit && amountNum > existingPaid) {
    const remaining = parseMoneyValue(amountNum - existingPaid);
    if (remaining > 0) {
      db.prepare(
        `INSERT INTO company_purchase_payments (company_purchase_id, payment_date, amount, payment_method, payment_source, note, created_by)
         VALUES (?, ?, ?, ?, ?, ?, ?)`
      ).run(
        req.params.id,
        purchase_date,
        remaining,
        paymentMethod,
        paymentSource,
        req.t("autoSettledOnEdit"),
        req.session.userId || null
      );
      finalPaidAmount = parseMoneyValue(existingPaid + remaining);
    }
  }
  const isCreditFinal = amountNum > finalPaidAmount ? 1 : 0;
  if (isCreditFinal === 1 && !sellerName) {
    return res.redirect(`/records/company-purchases/${req.params.id}/edit?error=sellerRequiredForCredit`);
  }

  db.prepare(
    `UPDATE company_purchases
     SET purchase_date = ?, item_name = ?, seller_name = ?, amount = ?, paid_amount = ?, is_credit = ?, is_machinery = ?, machinery_name = ?, technician_name = ?,
         technician_phone = ?, work_details = ?, note = ?, updated_at = datetime('now')
     WHERE id = ?`
  ).run(
    purchase_date,
    String(item_name).trim(),
    sellerName || null,
    amountNum,
    finalPaidAmount,
    isCreditFinal,
    isMachinery ? 1 : 0,
    isMachinery ? (machinery_name ? String(machinery_name).trim() : null) : null,
    isMachinery ? String(technician_name).trim() : null,
    isMachinery ? (technician_phone ? String(technician_phone).trim() : null) : null,
    isMachinery ? (work_details ? String(work_details).trim() : null) : null,
    note ? String(note).trim() : null,
    req.params.id
  );

  logActivity({
    userId: req.session.userId,
    action: "update",
    entityType: "company_purchase",
    entityId: req.params.id,
    details: buildDiffDetails(
      existing,
      {
        purchase_date,
        item_name: String(item_name).trim(),
        seller_name: sellerName || null,
        amount: amountNum,
        paid_amount: finalPaidAmount,
        is_credit: isCreditFinal,
        is_machinery: isMachinery ? 1 : 0,
        machinery_name: isMachinery ? (machinery_name ? String(machinery_name).trim() : null) : null,
        technician_name: isMachinery ? String(technician_name).trim() : null,
        technician_phone: isMachinery ? (technician_phone ? String(technician_phone).trim() : null) : null,
        work_details: isMachinery ? (work_details ? String(work_details).trim() : null) : null,
        note: note ? String(note).trim() : null
      },
      [
        "purchase_date",
        "item_name",
        "seller_name",
        "amount",
        "paid_amount",
        "is_credit",
        "is_machinery",
        "machinery_name",
        "technician_name",
        "technician_phone",
        "work_details",
        "note"
      ]
    )
  });

  res.redirect(`/records/company-purchases?from=${purchase_date}&to=${purchase_date}&status=updated`);
});

router.post("/company-purchases/:id/payments", (req, res) => {
  const purchase = db.prepare("SELECT * FROM company_purchases WHERE id = ?").get(req.params.id);
  if (!purchase) return res.redirect("/records/company-purchases");
  const paymentDate = String(req.body.payment_date || "").trim() || dayjs().format("YYYY-MM-DD");
  const amount = parseMoneyValue(req.body.amount);
  const paymentMethod = normalizePaymentMethod(req.body.payment_method);
  const paymentSource = normalizeSalaryPaymentSource(req.body.payment_source);
  const note = String(req.body.note || "").trim();
  if (!paymentDate || amount <= 0) {
    return res.redirect(`/records/company-purchases/${req.params.id}/edit?error=invalidPaymentAmount`);
  }
  const totalAmount = parseMoneyValue(purchase.amount || 0);
  const paidAmount = parseMoneyValue(purchase.paid_amount || 0);
  const remaining = Math.max(0, totalAmount - paidAmount);
  if (remaining <= 0) {
    return res.redirect(`/records/company-purchases/${req.params.id}/edit?error=noBalanceDue`);
  }
  if (amount > remaining) {
    return res.redirect(`/records/company-purchases/${req.params.id}/edit?error=paidMoreThanDue`);
  }
  const paymentId = db.prepare(
    `INSERT INTO company_purchase_payments (company_purchase_id, payment_date, amount, payment_method, payment_source, note, created_by)
     VALUES (?, ?, ?, ?, ?, ?, ?)`
  ).run(
    req.params.id,
    paymentDate,
    amount,
    paymentMethod,
    paymentSource,
    note || null,
    req.session.userId || null
  ).lastInsertRowid;
  const updatedPaid = parseMoneyValue(paidAmount + amount);
  const updatedCredit = updatedPaid < totalAmount ? 1 : 0;
  db.prepare("UPDATE company_purchases SET paid_amount = ?, is_credit = ? WHERE id = ?").run(
    updatedPaid,
    updatedCredit,
    req.params.id
  );
  logActivity({
    userId: req.session.userId,
    action: "create",
    entityType: "company_purchase_payment",
    entityId: paymentId,
    details: `purchase=${req.params.id}, amount=${amount}, date=${paymentDate}, method=${paymentMethod}, source=${paymentSource}`
  });
  return res.redirect(`/records/company-purchases/${req.params.id}/edit?status=payment_saved`);
});

router.post("/company-purchases/payments/:id/delete", (req, res) => {
  const payment = db.prepare("SELECT * FROM company_purchase_payments WHERE id = ?").get(req.params.id);
  if (!payment) return res.redirect("/records/company-purchases");
  const purchase = db.prepare("SELECT * FROM company_purchases WHERE id = ?").get(payment.company_purchase_id);
  if (!purchase) {
    db.prepare("DELETE FROM company_purchase_payments WHERE id = ?").run(req.params.id);
    return res.redirect("/records/company-purchases");
  }

  db.prepare("DELETE FROM company_purchase_payments WHERE id = ?").run(req.params.id);
  const paidRow = db.prepare(
    "SELECT COALESCE(SUM(amount), 0) as paid FROM company_purchase_payments WHERE company_purchase_id = ?"
  ).get(payment.company_purchase_id);
  const newPaid = parseMoneyValue(paidRow?.paid || 0);
  const totalAmount = parseMoneyValue(purchase.amount || 0);
  db.prepare("UPDATE company_purchases SET paid_amount = ?, is_credit = ? WHERE id = ?").run(
    newPaid,
    newPaid < totalAmount ? 1 : 0,
    payment.company_purchase_id
  );

  logActivity({
    userId: req.session.userId,
    action: "delete",
    entityType: "company_purchase_payment",
    entityId: req.params.id,
    details: `purchase=${payment.company_purchase_id}, amount=${payment.amount || 0}`
  });

  return res.redirect(`/records/company-purchases/${payment.company_purchase_id}/edit?status=payment_deleted`);
});

router.post("/company-purchases/:id/delete", (req, res) => {
  const existing = db.prepare("SELECT * FROM company_purchases WHERE id = ?").get(req.params.id);
  if (!existing) return res.redirect("/records/company-purchases");
  const payments = db.prepare(
    "SELECT * FROM company_purchase_payments WHERE company_purchase_id = ? ORDER BY payment_date ASC, id ASC"
  ).all(req.params.id);
  const recycleId = createRecycleEntry({
    entityType: "company_purchase",
    entityId: req.params.id,
    payload: { company_purchase: existing, company_purchase_payments: payments },
    deletedBy: req.session.userId,
    note: `date=${existing.purchase_date}; item=${existing.item_name || ""}`
  });
  db.prepare("DELETE FROM company_purchases WHERE id = ?").run(req.params.id);
  logActivity({
    userId: req.session.userId,
    action: "delete",
    entityType: "company_purchase",
    entityId: req.params.id,
    details: `item=${existing.item_name || ""}, amount=${existing.amount || 0}, recycle_id=${recycleId}`
  });
  db.prepare("DELETE FROM company_purchase_payments WHERE company_purchase_id = ?").run(req.params.id);
  res.redirect(`/records/company-purchases?from=${existing.purchase_date}&to=${existing.purchase_date}&status=deleted`);
});

router.get("/company-purchases/export", (req, res) => {
  const from = req.query.from || dayjs().startOf("month").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const q = String(req.query.q || "").trim();
  const searchClause = q
    ? "AND (company_purchases.item_name LIKE ? OR COALESCE(company_purchases.seller_name, '') LIKE ? OR company_purchases.machinery_name LIKE ? OR company_purchases.technician_name LIKE ? OR company_purchases.technician_phone LIKE ? OR company_purchases.work_details LIKE ?)"
    : "";
  const params = q
    ? [from, to, `%${q}%`, `%${q}%`, `%${q}%`, `%${q}%`, `%${q}%`, `%${q}%`]
    : [from, to];
  const rows = db.prepare(
    `SELECT company_purchases.*, users.full_name as recorded_by
     FROM company_purchases
     LEFT JOIN users ON company_purchases.created_by = users.id
     WHERE company_purchases.purchase_date BETWEEN ? AND ?
     ${searchClause}
     ORDER BY company_purchases.purchase_date ASC, company_purchases.created_at ASC`
  ).all(...params);
  const header = "Date,Item Name,Seller,Amount,Paid Amount,Due Amount,Is Credit,Is Machinery,Machinery,Technician,Technician Phone,Work Details,Note,Recorded By";
  const lines = rows.map((row) => [
    row.purchase_date,
    row.item_name,
    row.seller_name || "",
    row.amount,
    row.paid_amount || 0,
    Math.max(0, Number(row.amount || 0) - Number(row.paid_amount || 0)),
    Number(row.is_credit) === 1 ? "Yes" : "No",
    Number(row.is_machinery) === 1 ? "Yes" : "No",
    row.machinery_name || "",
    row.technician_name || "",
    row.technician_phone || "",
    row.work_details || "",
    row.note || "",
    row.recorded_by || ""
  ].map((val) => `"${String(val ?? "").replace(/"/g, '""')}"`).join(","));
  res.setHeader("Content-Type", "text/csv");
  res.setHeader("Content-Disposition", `attachment; filename="company_purchases_${from}_to_${to}.csv"`);
  res.send([header, ...lines].join("\n"));
});

router.get("/company-purchases/print", (req, res) => {
  const from = req.query.from || dayjs().startOf("month").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const q = String(req.query.q || "").trim();
  const searchClause = q
    ? "AND (company_purchases.item_name LIKE ? OR COALESCE(company_purchases.seller_name, '') LIKE ? OR company_purchases.machinery_name LIKE ? OR company_purchases.technician_name LIKE ? OR company_purchases.technician_phone LIKE ? OR company_purchases.work_details LIKE ?)"
    : "";
  const params = q
    ? [from, to, `%${q}%`, `%${q}%`, `%${q}%`, `%${q}%`, `%${q}%`, `%${q}%`]
    : [from, to];

  const rows = db.prepare(
    `SELECT company_purchases.*, users.full_name as recorded_by
     FROM company_purchases
     LEFT JOIN users ON company_purchases.created_by = users.id
     WHERE company_purchases.purchase_date BETWEEN ? AND ?
     ${searchClause}
     ORDER BY company_purchases.purchase_date DESC, company_purchases.created_at DESC`
  ).all(...params);

  const totals = rows.reduce(
    (acc, row) => {
      const amount = Number(row.amount || 0);
      const paid = Number(row.paid_amount || 0);
      acc.total += amount;
      acc.paid += paid;
      acc.due += Math.max(0, amount - paid);
      if (Math.max(0, amount - paid) > 0) acc.open_count += 1;
      if (Number(row.is_machinery) === 1) acc.machinery += amount;
      else acc.general += amount;
      return acc;
    },
    { total: 0, paid: 0, due: 0, open_count: 0, machinery: 0, general: 0 }
  );

  res.render("records/company_purchases_print", {
    title: req.t("companyPurchasesTitle"),
    from,
    to,
    q,
    rows,
    totals
  });
});

router.get("/vehicle-expenses", (req, res) => {
  const from = req.query.from || dayjs().startOf("month").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const vehicleIdRaw = Number(req.query.vehicle_id || 0);
  const vehicleId = Number.isInteger(vehicleIdRaw) && vehicleIdRaw > 0 ? vehicleIdRaw : null;
  const complianceVehicleRaw = Number(req.query.compliance_vehicle_id || 0);
  const complianceVehicleId = Number.isInteger(complianceVehicleRaw) && complianceVehicleRaw > 0 ? complianceVehicleRaw : null;
  const selectedType = String(req.query.expense_type || "").trim().toUpperCase();
  const expenseType = vehicleExpenseTypes.includes(selectedType) ? selectedType : "ALL";
  const q = String(req.query.q || "").trim();
  const todayDate = dayjs().format("YYYY-MM-DD");
  const vehicles = db.prepare(
    "SELECT id, vehicle_number, owner_name, is_company FROM vehicles WHERE is_company = 1 ORDER BY vehicle_number"
  ).all();

  const clauses = ["vehicle_expenses.expense_date BETWEEN ? AND ?", "vehicles.is_company = 1"];
  const params = [from, to];
  if (vehicleId) {
    clauses.push("vehicle_expenses.vehicle_id = ?");
    params.push(vehicleId);
  }
  if (expenseType !== "ALL") {
    clauses.push("vehicle_expenses.expense_type = ?");
    params.push(expenseType);
  }
  if (q) {
    clauses.push("(vehicles.vehicle_number LIKE ? OR vehicles.owner_name LIKE ? OR COALESCE(vehicle_expenses.note, '') LIKE ?)");
    params.push(`%${q}%`, `%${q}%`, `%${q}%`);
  }

  const rows = db.prepare(
    `SELECT vehicle_expenses.*, vehicles.vehicle_number, vehicles.owner_name, vehicles.is_company,
            users.full_name as recorded_by,
            CASE
              WHEN vehicle_expenses.amount - vehicle_expenses.paid_amount < 0 THEN 0
              ELSE vehicle_expenses.amount - vehicle_expenses.paid_amount
            END as due_amount
     FROM vehicle_expenses
     JOIN vehicles ON vehicle_expenses.vehicle_id = vehicles.id
     LEFT JOIN users ON vehicle_expenses.created_by = users.id
     WHERE ${clauses.join(" AND ")}
     ORDER BY vehicle_expenses.expense_date DESC, vehicle_expenses.created_at DESC`
  ).all(...params);

  const totals = rows.reduce((acc, row) => {
    const amount = parseMoneyValue(row.amount || 0);
    const paid = parseMoneyValue(row.paid_amount || 0);
    const due = computeRemainingMoney(amount, paid);
    const safeType = normalizeVehicleExpenseType(row.expense_type);
    acc.total = parseMoneyValue(acc.total + amount);
    acc.paid = parseMoneyValue(acc.paid + paid);
    acc.due = parseMoneyValue(acc.due + due);
    if (safeType === "FUEL") acc.fuel = parseMoneyValue(acc.fuel + amount);
    if (safeType === "REPAIR") acc.repair = parseMoneyValue(acc.repair + amount);
    if (safeType === "SERVICE") acc.service = parseMoneyValue(acc.service + amount);
    if (safeType === "OTHER") acc.other = parseMoneyValue(acc.other + amount);
    return acc;
  }, { total: 0, paid: 0, due: 0, fuel: 0, repair: 0, service: 0, other: 0 });

  const complianceRows = db.prepare(
    `SELECT vehicles.id as vehicle_id,
            vehicles.vehicle_number,
            vehicles.owner_name,
            vehicle_compliance.insurance_expiry,
            vehicle_compliance.tax_expiry,
            vehicle_compliance.permit_expiry,
            vehicle_compliance.fitness_expiry,
            vehicle_compliance.pollution_expiry,
            vehicle_compliance.note,
            vehicle_compliance.updated_at,
            users.full_name as updated_by_name
     FROM vehicles
     LEFT JOIN vehicle_compliance ON vehicle_compliance.vehicle_id = vehicles.id
     LEFT JOIN users ON users.id = vehicle_compliance.updated_by
     WHERE vehicles.is_company = 1
     ORDER BY vehicles.vehicle_number ASC`
  ).all().map((row) => getComplianceSummaryRow(row, todayDate));
  const complianceCounts = complianceRows.reduce((acc, row) => {
    if (row.overall_status === "EXPIRED") acc.expired += 1;
    else if (row.overall_status === "DUE_SOON") acc.dueSoon += 1;
    else if (row.overall_status === "NOT_SET") acc.notSet += 1;
    else acc.valid += 1;
    return acc;
  }, { expired: 0, dueSoon: 0, notSet: 0, valid: 0 });
  const selectedCompliance = complianceRows.find((row) => row.vehicle_id === complianceVehicleId)
    || complianceRows[0]
    || null;

  const status = req.query.status || "";
  const errorKey = req.query.error || "";
  const error = errorKey ? req.t(errorKey) : null;
  const success = status === "saved"
    ? req.t("vehicleExpenseSaved")
    : status === "compliance_saved"
      ? req.t("vehicleComplianceSaved")
    : status === "payment_saved"
      ? req.t("vehicleExpensePaymentSaved")
      : status === "payment_deleted"
        ? req.t("vehicleExpensePaymentDeleted")
    : status === "deleted"
      ? req.t("vehicleExpenseDeleted")
      : null;

  res.render("records/vehicle_expenses", {
    title: req.t("vehicleExpensesTitle"),
    from,
    to,
    q,
    vehicleId: vehicleId ? String(vehicleId) : "all",
    expenseType,
    vehicles,
    rows,
    totals,
    todayDate,
    complianceRows,
    complianceCounts,
    selectedCompliance,
    error,
    success,
    resolveExpenseTypeLabel: (value) => getVehicleExpenseTypeLabel(value, req.t)
  });
});

router.post("/vehicle-compliance/:vehicleId", (req, res) => {
  const vehicleId = Number(req.params.vehicleId || 0);
  const from = String(req.body.from || "").trim();
  const to = String(req.body.to || "").trim();
  if (!vehicleId) {
    return res.redirect(`/records/vehicle-expenses?from=${from || dayjs().startOf("month").format("YYYY-MM-DD")}&to=${to || dayjs().format("YYYY-MM-DD")}&error=vehicleComplianceVehicleRequired`);
  }
  const vehicle = db.prepare("SELECT id, is_company FROM vehicles WHERE id = ?").get(vehicleId);
  if (!vehicle || Number(vehicle.is_company) !== 1) {
    return res.redirect(`/records/vehicle-expenses?from=${from || dayjs().startOf("month").format("YYYY-MM-DD")}&to=${to || dayjs().format("YYYY-MM-DD")}&error=vehicleExpenseCompanyOnly`);
  }
  const insuranceExpiry = parseOptionalDate(req.body.insurance_expiry);
  const taxExpiry = parseOptionalDate(req.body.tax_expiry);
  const permitExpiry = parseOptionalDate(req.body.permit_expiry);
  const fitnessExpiry = parseOptionalDate(req.body.fitness_expiry);
  const pollutionExpiry = parseOptionalDate(req.body.pollution_expiry);
  const note = String(req.body.compliance_note || "").trim();

  db.prepare(
    `INSERT INTO vehicle_compliance (
      vehicle_id,
      insurance_expiry,
      tax_expiry,
      permit_expiry,
      fitness_expiry,
      pollution_expiry,
      note,
      updated_by,
      updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
    ON CONFLICT(vehicle_id) DO UPDATE SET
      insurance_expiry = excluded.insurance_expiry,
      tax_expiry = excluded.tax_expiry,
      permit_expiry = excluded.permit_expiry,
      fitness_expiry = excluded.fitness_expiry,
      pollution_expiry = excluded.pollution_expiry,
      note = excluded.note,
      updated_by = excluded.updated_by,
      updated_at = datetime('now')`
  ).run(
    vehicleId,
    insuranceExpiry,
    taxExpiry,
    permitExpiry,
    fitnessExpiry,
    pollutionExpiry,
    note || null,
    req.session.userId || null
  );

  logActivity({
    userId: req.session.userId,
    action: "update",
    entityType: "vehicle_compliance",
    entityId: vehicleId,
    details: `insurance=${insuranceExpiry || ""}; tax=${taxExpiry || ""}; permit=${permitExpiry || ""}; fitness=${fitnessExpiry || ""}; pollution=${pollutionExpiry || ""}`
  });

  return res.redirect(`/records/vehicle-expenses?from=${from || dayjs().startOf("month").format("YYYY-MM-DD")}&to=${to || dayjs().format("YYYY-MM-DD")}&status=compliance_saved&compliance_vehicle_id=${vehicleId}`);
});

router.post("/vehicle-expenses", (req, res) => {
  const {
    vehicle_id,
    expense_date,
    expense_type,
    amount,
    paid_amount,
    payment_method,
    payment_source,
    payment_note,
    note
  } = req.body;

  const vehicleId = Number(vehicle_id || 0);
  const amountNum = parseMoneyValue(amount || 0);
  let paidAmountNum = paid_amount === undefined ? amountNum : parseMoneyValue(paid_amount || 0);
  const paymentMethod = normalizePaymentMethod(payment_method);
  const paymentSource = normalizeSalaryPaymentSource(payment_source);
  const normalizedType = normalizeVehicleExpenseType(expense_type);
  if (!vehicleId || !expense_date || Number.isNaN(amountNum) || amountNum <= 0) {
    return res.redirect("/records/vehicle-expenses?error=vehicleExpenseRequired");
  }
  if (Number.isNaN(paidAmountNum) || paidAmountNum < 0 || paidAmountNum > amountNum) {
    return res.redirect(`/records/vehicle-expenses?from=${expense_date}&to=${expense_date}&error=paidMoreThanTotal`);
  }
  const vehicleCompany = db.prepare("SELECT id, is_company FROM vehicles WHERE id = ?").get(vehicleId);
  if (!vehicleCompany || Number(vehicleCompany.is_company) !== 1) {
    return res.redirect("/records/vehicle-expenses?error=vehicleExpenseCompanyOnly");
  }
  const dueAmount = computeRemainingMoney(amountNum, paidAmountNum);
  const isCredit = dueAmount > 0 ? 1 : 0;

  db.exec("BEGIN;");
  let expenseId = null;
  try {
    expenseId = db.prepare(
      `INSERT INTO vehicle_expenses (vehicle_id, expense_date, expense_type, amount, paid_amount, is_credit, note, created_by)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
    ).run(
      vehicleId,
      expense_date,
      normalizedType,
      amountNum,
      paidAmountNum,
      isCredit,
      note ? String(note).trim() : null,
      req.session.userId || null
    ).lastInsertRowid;

    if (paidAmountNum > 0) {
      db.prepare(
        `INSERT INTO vehicle_expense_payments (vehicle_expense_id, payment_date, amount, payment_method, payment_source, note, created_by)
         VALUES (?, ?, ?, ?, ?, ?, ?)`
      ).run(
        expenseId,
        expense_date,
        paidAmountNum,
        paymentMethod,
        paymentSource,
        payment_note ? String(payment_note).trim() : req.t("openingPaymentNote"),
        req.session.userId || null
      );
    }
    db.exec("COMMIT;");
  } catch (err) {
    db.exec("ROLLBACK;");
    throw err;
  }

  logActivity({
    userId: req.session.userId,
    action: "create",
    entityType: "vehicle_expense",
    entityId: expenseId,
    details: `vehicle_id=${vehicleId}, type=${normalizedType}, amount=${amountNum}, paid=${paidAmountNum}, due=${dueAmount}, method=${paymentMethod}, source=${paymentSource}`
  });

  res.redirect(`/records/vehicle-expenses?from=${expense_date}&to=${expense_date}&vehicle_id=${vehicleId}&status=saved`);
});

router.get("/vehicle-expenses/:id/payments", (req, res) => {
  const record = db.prepare(
    `SELECT vehicle_expenses.*, vehicles.vehicle_number, vehicles.owner_name, vehicles.is_company
     FROM vehicle_expenses
     JOIN vehicles ON vehicle_expenses.vehicle_id = vehicles.id
     WHERE vehicle_expenses.id = ?`
  ).get(req.params.id);
  if (!record) return res.redirect("/records/vehicle-expenses");
  if (Number(record.is_company) !== 1) {
    return res.redirect("/records/vehicle-expenses?error=vehicleExpenseCompanyOnly");
  }
  const payments = db.prepare(
    `SELECT vehicle_expense_payments.*, users.full_name as recorded_by
     FROM vehicle_expense_payments
     LEFT JOIN users ON vehicle_expense_payments.created_by = users.id
     WHERE vehicle_expense_payments.vehicle_expense_id = ?
     ORDER BY vehicle_expense_payments.payment_date DESC, vehicle_expense_payments.id DESC`
  ).all(req.params.id);
  const dueAmount = computeRemainingMoney(record.amount || 0, record.paid_amount || 0);
  const errorKey = String(req.query.error || "").trim();
  const status = String(req.query.status || "").trim();
  const error = errorKey ? req.t(errorKey) : null;
  const success = status === "payment_saved"
    ? req.t("vehicleExpensePaymentSaved")
    : status === "payment_deleted"
      ? req.t("vehicleExpensePaymentDeleted")
      : null;
  res.render("records/vehicle_expense_payments", {
    title: req.t("vehicleExpensePaymentsTitle"),
    record,
    payments,
    dueAmount,
    error,
    success
  });
});

router.post("/vehicle-expenses/:id/payments", (req, res) => {
  const record = db.prepare(
    `SELECT vehicle_expenses.*, vehicles.is_company
     FROM vehicle_expenses
     JOIN vehicles ON vehicles.id = vehicle_expenses.vehicle_id
     WHERE vehicle_expenses.id = ?`
  ).get(req.params.id);
  if (!record) return res.redirect("/records/vehicle-expenses");
  if (Number(record.is_company) !== 1) {
    return res.redirect("/records/vehicle-expenses?error=vehicleExpenseCompanyOnly");
  }
  const paymentDate = req.body.payment_date || dayjs().format("YYYY-MM-DD");
  const amount = parseMoneyValue(req.body.amount || 0);
  const paymentMethod = normalizePaymentMethod(req.body.payment_method);
  const paymentSource = normalizeSalaryPaymentSource(req.body.payment_source);
  const note = String(req.body.note || "").trim();
  if (Number.isNaN(amount) || amount <= 0) {
    return res.redirect(`/records/vehicle-expenses/${req.params.id}/payments?error=invalidPaymentAmount`);
  }
  const dueAmount = computeRemainingMoney(record.amount || 0, record.paid_amount || 0);
  if (dueAmount <= 0) {
    return res.redirect(`/records/vehicle-expenses/${req.params.id}/payments?error=noBalanceDue`);
  }
  if (amount > dueAmount) {
    return res.redirect(`/records/vehicle-expenses/${req.params.id}/payments?error=paidMoreThanDue`);
  }

  const applied = parseMoneyValue(amount);
  const newPaid = parseMoneyValue(parseMoneyValue(record.paid_amount || 0) + applied);
  const newDue = computeRemainingMoney(record.amount || 0, newPaid);

  db.exec("BEGIN;");
  try {
    db.prepare(
      "INSERT INTO vehicle_expense_payments (vehicle_expense_id, payment_date, amount, payment_method, payment_source, note, created_by) VALUES (?, ?, ?, ?, ?, ?, ?)"
    ).run(req.params.id, paymentDate, applied, paymentMethod, paymentSource, note || null, req.session.userId || null);
    db.prepare("UPDATE vehicle_expenses SET paid_amount = ?, is_credit = ? WHERE id = ?").run(
      newPaid,
      newDue > 0 ? 1 : 0,
      req.params.id
    );
    db.exec("COMMIT;");
  } catch (err) {
    db.exec("ROLLBACK;");
    throw err;
  }

  logActivity({
    userId: req.session.userId,
    action: "payment",
    entityType: "vehicle_expense",
    entityId: req.params.id,
    details: `payment=${applied}; method=${paymentMethod}; source=${paymentSource}; paid_amount=${newPaid}; due=${newDue}`
  });

  return res.redirect(`/records/vehicle-expenses/${req.params.id}/payments?status=payment_saved`);
});

router.post("/vehicle-expenses/payments/:id/delete", (req, res) => {
  const payment = db.prepare("SELECT * FROM vehicle_expense_payments WHERE id = ?").get(req.params.id);
  if (!payment) return res.redirect("/records/vehicle-expenses");
  const record = db.prepare(
    `SELECT vehicle_expenses.*, vehicles.is_company
     FROM vehicle_expenses
     JOIN vehicles ON vehicles.id = vehicle_expenses.vehicle_id
     WHERE vehicle_expenses.id = ?`
  ).get(payment.vehicle_expense_id);
  if (!record) return res.redirect("/records/vehicle-expenses");
  if (Number(record.is_company) !== 1) {
    return res.redirect("/records/vehicle-expenses?error=vehicleExpenseCompanyOnly");
  }
  const revertedPaid = parseMoneyValue(Math.max(0, parseMoneyValue(record.paid_amount || 0) - parseMoneyValue(payment.amount || 0)));
  const newDue = computeRemainingMoney(record.amount || 0, revertedPaid);
  db.exec("BEGIN;");
  try {
    db.prepare("DELETE FROM vehicle_expense_payments WHERE id = ?").run(req.params.id);
    db.prepare("UPDATE vehicle_expenses SET paid_amount = ?, is_credit = ? WHERE id = ?").run(
      revertedPaid,
      newDue > 0 ? 1 : 0,
      record.id
    );
    db.exec("COMMIT;");
  } catch (err) {
    db.exec("ROLLBACK;");
    throw err;
  }

  logActivity({
    userId: req.session.userId,
    action: "delete",
    entityType: "vehicle_expense_payment",
    entityId: req.params.id,
    details: `vehicle_expense_id=${record.id}; amount=${payment.amount || 0}`
  });

  return res.redirect(`/records/vehicle-expenses/${record.id}/payments?status=payment_deleted`);
});

router.post("/vehicle-expenses/:id/delete", (req, res) => {
  const existing = db.prepare(
    `SELECT vehicle_expenses.*, vehicles.is_company
     FROM vehicle_expenses
     JOIN vehicles ON vehicles.id = vehicle_expenses.vehicle_id
     WHERE vehicle_expenses.id = ?`
  ).get(req.params.id);
  if (!existing) return res.redirect("/records/vehicle-expenses");
  if (Number(existing.is_company) !== 1) {
    return res.redirect("/records/vehicle-expenses?error=vehicleExpenseCompanyOnly");
  }
  const recycleId = createRecycleEntry({
    entityType: "vehicle_expense",
    entityId: req.params.id,
    payload: { vehicle_expense: existing },
    deletedBy: req.session.userId,
    note: `date=${existing.expense_date || ""}; amount=${existing.amount || 0}`
  });
  db.prepare("DELETE FROM vehicle_expenses WHERE id = ?").run(req.params.id);
  logActivity({
    userId: req.session.userId,
    action: "delete",
    entityType: "vehicle_expense",
    entityId: req.params.id,
    details: `type=${existing.expense_type || ""}, amount=${existing.amount || 0}, recycle_id=${recycleId}`
  });
  res.redirect(`/records/vehicle-expenses?from=${existing.expense_date}&to=${existing.expense_date}&vehicle_id=${existing.vehicle_id}&status=deleted`);
});

router.get("/rentals", (req, res) => {
  const from = req.query.from || dayjs().startOf("month").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const q = String(req.query.q || "").trim();
  const searchClause = q
    ? "AND (rent_entries.renter_name LIKE ? OR rent_entries.item_name LIKE ? OR COALESCE(rent_entries.note, '') LIKE ?)"
    : "";
  const params = q ? [from, to, `%${q}%`, `%${q}%`, `%${q}%`] : [from, to];
  const rows = db.prepare(
    `SELECT rent_entries.*, users.full_name as recorded_by
     FROM rent_entries
     LEFT JOIN users ON users.id = rent_entries.created_by
     WHERE rent_entries.rent_date BETWEEN ? AND ?
     ${searchClause}
     ORDER BY rent_entries.rent_date DESC, rent_entries.created_at DESC`
  ).all(...params);
  const totals = rows.reduce(
    (acc, row) => {
      const amount = parseMoneyValue(row.amount || 0);
      acc.total = parseMoneyValue(acc.total + amount);
      if (Number(row.add_to_collection) === 1) {
        acc.collection = parseMoneyValue(acc.collection + amount);
      }
      return acc;
    },
    { total: 0, collection: 0, count: rows.length }
  );
  const status = String(req.query.status || "").trim();
  const errorKey = String(req.query.error || "").trim();
  const error = errorKey ? req.t(errorKey) : null;
  const success = status === "saved"
    ? req.t("rentSaved")
    : status === "deleted"
      ? req.t("rentDeleted")
      : null;

  res.render("records/rentals", {
    title: req.t("rentalsTitle"),
    from,
    to,
    q,
    rows,
    totals,
    error,
    success
  });
});

router.post("/rentals", (req, res) => {
  const {
    rent_date,
    renter_name,
    item_name,
    amount,
    payment_method,
    add_to_collection,
    note
  } = req.body;
  const amountNum = parseMoneyValue(amount || 0);
  const method = normalizePaymentMethod(payment_method);
  const addToCollection = parseCheckbox(add_to_collection) ? 1 : 0;
  const rentDate = String(rent_date || "").trim() || dayjs().format("YYYY-MM-DD");
  const renterName = String(renter_name || "").trim();
  const itemName = String(item_name || "").trim();
  if (!rentDate || !renterName || !itemName || Number.isNaN(amountNum) || amountNum <= 0) {
    return res.redirect(`/records/rentals?from=${rentDate}&to=${rentDate}&error=rentRequired`);
  }

  const rentId = db.prepare(
    `INSERT INTO rent_entries (rent_date, renter_name, item_name, amount, payment_method, add_to_collection, note, created_by)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
  ).run(
    rentDate,
    renterName,
    itemName,
    amountNum,
    method,
    addToCollection,
    note ? String(note).trim() : null,
    req.session.userId || null
  ).lastInsertRowid;

  logActivity({
    userId: req.session.userId,
    action: "create",
    entityType: "rent_entry",
    entityId: rentId,
    details: `rent_date=${rentDate}, renter=${renterName}, item=${itemName}, amount=${amountNum}, method=${method}, collection=${addToCollection}`
  });

  return res.redirect(`/records/rentals?from=${rentDate}&to=${rentDate}&status=saved`);
});

router.post("/rentals/:id/delete", (req, res) => {
  const row = db.prepare("SELECT * FROM rent_entries WHERE id = ?").get(req.params.id);
  if (!row) return res.redirect("/records/rentals");
  const recycleId = createRecycleEntry({
    entityType: "rent_entry",
    entityId: req.params.id,
    payload: { rent_entry: row },
    deletedBy: req.session.userId,
    note: `date=${row.rent_date || ""}; renter=${row.renter_name || ""}`
  });
  db.prepare("DELETE FROM rent_entries WHERE id = ?").run(req.params.id);
  logActivity({
    userId: req.session.userId,
    action: "delete",
    entityType: "rent_entry",
    entityId: req.params.id,
    details: `recycle_id=${recycleId}`
  });
  return res.redirect(`/records/rentals?from=${row.rent_date}&to=${row.rent_date}&status=deleted`);
});

router.get("/reconciliation", (req, res) => {
  const today = dayjs().format("YYYY-MM-DD");
  const from = req.query.from || dayjs().startOf("month").format("YYYY-MM-DD");
  const to = req.query.to || today;
  const requestedDate = String(req.query.date || "").trim();
  const businessDate = requestedDate && dayjs(requestedDate).isValid()
    ? dayjs(requestedDate).format("YYYY-MM-DD")
    : to;
  const snapshot = getDailyReconciliationSnapshot(businessDate);
  const saved = db.prepare(
    `SELECT day_reconciliations.*, users.full_name as reconciled_by_name
     FROM day_reconciliations
     LEFT JOIN users ON users.id = day_reconciliations.reconciled_by
     WHERE business_date = ?`
  ).get(businessDate);
  const rows = db.prepare(
    `SELECT day_reconciliations.*, users.full_name as reconciled_by_name
     FROM day_reconciliations
     LEFT JOIN users ON users.id = day_reconciliations.reconciled_by
     WHERE business_date BETWEEN ? AND ?
     ORDER BY business_date DESC`
  ).all(from, to);
  const status = String(req.query.status || "").trim();
  const errorKey = String(req.query.error || "").trim();
  const success = status === "saved" ? req.t("reconciliationSaved") : null;
  const error = errorKey ? req.t(errorKey) : null;
  return res.render("records/reconciliation", {
    title: req.t("reconciliationTitle"),
    from,
    to,
    businessDate,
    snapshot,
    saved,
    rows,
    success,
    error
  });
});

router.post("/reconciliation", (req, res) => {
  const from = String(req.body.from || "").trim();
  const to = String(req.body.to || "").trim();
  const dateRaw = String(req.body.business_date || "").trim();
  if (!dateRaw || !dayjs(dateRaw).isValid()) {
    return res.redirect(`/records/reconciliation?from=${from || dayjs().startOf("month").format("YYYY-MM-DD")}&to=${to || dayjs().format("YYYY-MM-DD")}&error=reconciliationDateRequired`);
  }
  const businessDate = dayjs(dateRaw).format("YYYY-MM-DD");
  const actualCash = parseMoneyValue(req.body.actual_cash || 0);
  const actualBank = parseMoneyValue(req.body.actual_bank || 0);
  const actualEwallet = parseMoneyValue(req.body.actual_ewallet || 0);
  const actualTotal = parseMoneyValue(actualCash + actualBank + actualEwallet);
  const note = String(req.body.note || "").trim();
  const snapshot = getDailyReconciliationSnapshot(businessDate);
  const differenceTotal = roundMoneySigned(actualTotal - Number(snapshot.expected.netAfterDeductions || 0));

  db.prepare(
    `INSERT INTO day_reconciliations (
      business_date,
      expected_cash, expected_bank, expected_ewallet, expected_total,
      deducted_from_collection, expected_net,
      actual_cash, actual_bank, actual_ewallet, actual_total, difference_total,
      note, reconciled_by, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
    ON CONFLICT(business_date) DO UPDATE SET
      expected_cash = excluded.expected_cash,
      expected_bank = excluded.expected_bank,
      expected_ewallet = excluded.expected_ewallet,
      expected_total = excluded.expected_total,
      deducted_from_collection = excluded.deducted_from_collection,
      expected_net = excluded.expected_net,
      actual_cash = excluded.actual_cash,
      actual_bank = excluded.actual_bank,
      actual_ewallet = excluded.actual_ewallet,
      actual_total = excluded.actual_total,
      difference_total = excluded.difference_total,
      note = excluded.note,
      reconciled_by = excluded.reconciled_by,
      updated_at = datetime('now')`
  ).run(
    businessDate,
    snapshot.inflow.expectedCash,
    snapshot.inflow.expectedBank,
    snapshot.inflow.expectedEwallet,
    snapshot.inflow.expectedTotal,
    snapshot.deductions.total,
    snapshot.expected.netAfterDeductions,
    actualCash,
    actualBank,
    actualEwallet,
    actualTotal,
    differenceTotal,
    note || null,
    req.session.userId || null
  );

  logActivity({
    userId: req.session.userId,
    action: "reconcile",
    entityType: "day_reconciliation",
    entityId: businessDate,
    details: `date=${businessDate}; expected=${snapshot.expected.netAfterDeductions}; actual=${actualTotal}; diff=${differenceTotal}`
  });

  return res.redirect(`/records/reconciliation?from=${from || businessDate}&to=${to || businessDate}&date=${businessDate}&status=saved`);
});

router.get("/payment-ledger", (req, res) => {
  const from = req.query.from || dayjs().startOf("month").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const channel = normalizeLedgerChannel(req.query.channel || "BANK");
  const settingKey = getLedgerOpeningSettingKey(channel);
  const openingBalance = parseMoneyValue(getSetting(settingKey, 0));
  const data = buildPaymentLedgerData({ channel, from, to, openingBalance });
  const status = String(req.query.status || "").trim();
  const errorKey = String(req.query.error || "").trim();
  const success = status === "opening_saved" ? req.t("openingBalanceSaved") : null;
  const error = errorKey ? req.t(errorKey) : null;

  return res.render("records/payment_ledger", {
    title: req.t("paymentLedgerTitle"),
    from,
    to,
    channel,
    openingBalance,
    rows: data.rows,
    totals: data.totals,
    success,
    error
  });
});

router.post("/payment-ledger/opening", (req, res) => {
  const from = String(req.body.from || "").trim();
  const to = String(req.body.to || "").trim();
  const channel = normalizeLedgerChannel(req.body.channel || "BANK");
  const currentUser = res.locals.currentUser;
  if (!currentUser || currentUser.role === "WORKER") {
    return res.redirect(`/records/payment-ledger?from=${from || dayjs().startOf("month").format("YYYY-MM-DD")}&to=${to || dayjs().format("YYYY-MM-DD")}&channel=${channel}&error=openingBalanceAdminOnly`);
  }
  const openingBalance = parseMoneyValue(req.body.opening_balance || 0);
  const settingKey = getLedgerOpeningSettingKey(channel);
  setSetting(settingKey, openingBalance);

  logActivity({
    userId: req.session.userId,
    action: "update",
    entityType: "payment_ledger_opening",
    entityId: channel,
    details: `channel=${channel}; opening=${openingBalance}`
  });

  return res.redirect(`/records/payment-ledger?from=${from || dayjs().startOf("month").format("YYYY-MM-DD")}&to=${to || dayjs().format("YYYY-MM-DD")}&channel=${channel}&status=opening_saved`);
});

router.get("/payment-ledger/export", (req, res) => {
  const from = req.query.from || dayjs().startOf("month").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const channel = normalizeLedgerChannel(req.query.channel || "BANK");
  const openingBalance = parseMoneyValue(getSetting(getLedgerOpeningSettingKey(channel), 0));
  const data = buildPaymentLedgerData({ channel, from, to, openingBalance });
  const header = [
    "Date",
    "Source",
    "Reference",
    "Party",
    "Inflow",
    "Outflow",
    "Running Balance",
    "Note"
  ];
  const lines = data.rows.map((row) => ([
    row.date,
    row.source,
    row.reference,
    row.party,
    row.inflow,
    row.outflow,
    row.balance,
    row.note || ""
  ].map((val) => `"${String(val ?? "").replace(/"/g, '""')}"`).join(",")));
  const summary = [
    `"Opening Balance","","","",${openingBalance},,,""`,
    `"Totals","","","",${data.totals.inflow},${data.totals.outflow},${data.totals.closing},""`
  ];
  res.setHeader("Content-Type", "text/csv");
  res.setHeader("Content-Disposition", `attachment; filename="${channel.toLowerCase()}_ledger_${from}_to_${to}.csv"`);
  return res.send([header.join(","), ...lines, ...summary].join("\n"));
});

router.get("/vendor-aging", (req, res) => {
  const today = dayjs().format("YYYY-MM-DD");
  const from = req.query.from || dayjs().subtract(90, "day").format("YYYY-MM-DD");
  const to = req.query.to || today;
  const q = String(req.query.q || "").trim();
  const like = `%${q}%`;
  const imports = db.prepare(
    `SELECT import_entries.id,
            import_entries.entry_date as bill_date,
            import_entries.seller_name as vendor_name,
            import_entries.item_type as item_name,
            import_entries.total_amount as total_amount,
            import_entries.paid_amount as paid_amount,
            CASE WHEN import_entries.total_amount - import_entries.paid_amount < 0 THEN 0
                 ELSE import_entries.total_amount - import_entries.paid_amount END as due_amount,
            'IMPORT' as source
     FROM import_entries
     WHERE import_entries.entry_date BETWEEN ? AND ?
       AND TRIM(COALESCE(import_entries.seller_name, '')) <> ''
       AND (import_entries.total_amount - import_entries.paid_amount) > 0
       ${q ? "AND import_entries.seller_name LIKE ?" : ""}`
  ).all(...(q ? [from, to, like] : [from, to]));
  const purchases = db.prepare(
    `SELECT company_purchases.id,
            company_purchases.purchase_date as bill_date,
            company_purchases.seller_name as vendor_name,
            company_purchases.item_name as item_name,
            company_purchases.amount as total_amount,
            company_purchases.paid_amount as paid_amount,
            CASE WHEN company_purchases.amount - company_purchases.paid_amount < 0 THEN 0
                 ELSE company_purchases.amount - company_purchases.paid_amount END as due_amount,
            'PURCHASE' as source
     FROM company_purchases
     WHERE company_purchases.purchase_date BETWEEN ? AND ?
       AND TRIM(COALESCE(company_purchases.seller_name, '')) <> ''
       AND (company_purchases.amount - company_purchases.paid_amount) > 0
       ${q ? "AND company_purchases.seller_name LIKE ?" : ""}`
  ).all(...(q ? [from, to, like] : [from, to]));

  const detailRows = [...imports, ...purchases].map((row) => {
    const days = Math.max(0, dayjs(today).diff(dayjs(row.bill_date), "day"));
    return {
      ...row,
      vendor_name: String(row.vendor_name || "").trim(),
      due_amount: parseMoneyValue(row.due_amount || 0),
      days_overdue: days,
      bucket: getVendorAgingBucket(days)
    };
  }).sort((a, b) => b.days_overdue - a.days_overdue || b.due_amount - a.due_amount);

  const vendorMap = new Map();
  detailRows.forEach((row) => {
    if (!vendorMap.has(row.vendor_name)) {
      vendorMap.set(row.vendor_name, {
        vendor_name: row.vendor_name,
        total_due: 0,
        bucket_0_7: 0,
        bucket_8_30: 0,
        bucket_30_plus: 0,
        record_count: 0,
        oldest_days: 0
      });
    }
    const vendor = vendorMap.get(row.vendor_name);
    vendor.total_due = parseMoneyValue(vendor.total_due + row.due_amount);
    vendor.record_count += 1;
    vendor.oldest_days = Math.max(vendor.oldest_days, row.days_overdue);
    if (row.bucket === "0_7") vendor.bucket_0_7 = parseMoneyValue(vendor.bucket_0_7 + row.due_amount);
    if (row.bucket === "8_30") vendor.bucket_8_30 = parseMoneyValue(vendor.bucket_8_30 + row.due_amount);
    if (row.bucket === "30_plus") vendor.bucket_30_plus = parseMoneyValue(vendor.bucket_30_plus + row.due_amount);
  });

  const vendorRows = Array.from(vendorMap.values()).map((row) => {
    let priority = "LOW";
    if (row.bucket_30_plus > 0) priority = "HIGH";
    else if (row.bucket_8_30 > 0) priority = "MEDIUM";
    return {
      ...row,
      priority,
      priority_rank: priority === "HIGH" ? 1 : priority === "MEDIUM" ? 2 : 3
    };
  }).sort((a, b) => a.priority_rank - b.priority_rank || b.total_due - a.total_due || a.vendor_name.localeCompare(b.vendor_name));

  const totals = vendorRows.reduce((acc, row) => {
    acc.total_due = parseMoneyValue(acc.total_due + row.total_due);
    acc.bucket_0_7 = parseMoneyValue(acc.bucket_0_7 + row.bucket_0_7);
    acc.bucket_8_30 = parseMoneyValue(acc.bucket_8_30 + row.bucket_8_30);
    acc.bucket_30_plus = parseMoneyValue(acc.bucket_30_plus + row.bucket_30_plus);
    acc.vendor_count += 1;
    return acc;
  }, { total_due: 0, bucket_0_7: 0, bucket_8_30: 0, bucket_30_plus: 0, vendor_count: 0 });

  return res.render("records/vendor_aging", {
    title: req.t("vendorAgingTitle"),
    from,
    to,
    q,
    today,
    vendorRows,
    detailRows,
    totals
  });
});

router.get("/vendor-aging/export", (req, res) => {
  const from = req.query.from || dayjs().subtract(90, "day").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const q = String(req.query.q || "").trim();
  const like = `%${q}%`;
  const imports = db.prepare(
    `SELECT import_entries.entry_date as bill_date,
            import_entries.seller_name as vendor_name,
            import_entries.item_type as item_name,
            CASE WHEN import_entries.total_amount - import_entries.paid_amount < 0 THEN 0
                 ELSE import_entries.total_amount - import_entries.paid_amount END as due_amount,
            'IMPORT' as source
     FROM import_entries
     WHERE import_entries.entry_date BETWEEN ? AND ?
       AND TRIM(COALESCE(import_entries.seller_name, '')) <> ''
       AND (import_entries.total_amount - import_entries.paid_amount) > 0
       ${q ? "AND import_entries.seller_name LIKE ?" : ""}`
  ).all(...(q ? [from, to, like] : [from, to]));
  const purchases = db.prepare(
    `SELECT company_purchases.purchase_date as bill_date,
            company_purchases.seller_name as vendor_name,
            company_purchases.item_name as item_name,
            CASE WHEN company_purchases.amount - company_purchases.paid_amount < 0 THEN 0
                 ELSE company_purchases.amount - company_purchases.paid_amount END as due_amount,
            'PURCHASE' as source
     FROM company_purchases
     WHERE company_purchases.purchase_date BETWEEN ? AND ?
       AND TRIM(COALESCE(company_purchases.seller_name, '')) <> ''
       AND (company_purchases.amount - company_purchases.paid_amount) > 0
       ${q ? "AND company_purchases.seller_name LIKE ?" : ""}`
  ).all(...(q ? [from, to, like] : [from, to]));

  const rows = [...imports, ...purchases].map((row) => {
    const days = Math.max(0, dayjs().diff(dayjs(row.bill_date), "day"));
    return {
      ...row,
      due_amount: parseMoneyValue(row.due_amount || 0),
      days,
      bucket: getVendorAgingBucket(days)
    };
  });
  const header = ["Vendor", "Source", "Date", "Item", "Due Amount", "Days Outstanding", "Bucket"];
  const lines = rows.map((row) => ([
    row.vendor_name,
    row.source,
    row.bill_date,
    row.item_name,
    row.due_amount,
    row.days,
    row.bucket
  ].map((val) => `"${String(val ?? "").replace(/"/g, '""')}"`).join(",")));
  res.setHeader("Content-Type", "text/csv");
  res.setHeader("Content-Disposition", `attachment; filename="vendor_aging_${from}_to_${to}.csv"`);
  return res.send([header.join(","), ...lines].join("\n"));
});

router.get("/leakage-guard", requireRole(["SUPER_ADMIN", "ADMIN"]), (req, res) => {
  const from = req.query.from || dayjs().subtract(14, "day").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const overpayments = [];

  db.prepare(
    `SELECT id, export_date as record_date, total_amount as expected_amount, paid_amount as paid_amount, payment_method,
            ('EXPORT#' || id) as reference
     FROM exports
     WHERE export_date BETWEEN ? AND ?
       AND paid_amount > total_amount`
  ).all(from, to).forEach((row) => {
    overpayments.push({
      source: "EXPORT",
      date: row.record_date,
      reference: row.reference,
      expected_amount: parseMoneyValue(row.expected_amount || 0),
      paid_amount: parseMoneyValue(row.paid_amount || 0),
      overpaid_by: parseMoneyValue((row.paid_amount || 0) - (row.expected_amount || 0))
    });
  });

  db.prepare(
    `SELECT id, credit_date as record_date, amount as expected_amount, paid_amount as paid_amount,
            ('CREDIT#' || id) as reference
     FROM credits
     WHERE credit_date BETWEEN ? AND ?
       AND paid_amount > amount`
  ).all(from, to).forEach((row) => {
    overpayments.push({
      source: "CUSTOMER_CREDIT",
      date: row.record_date,
      reference: row.reference,
      expected_amount: parseMoneyValue(row.expected_amount || 0),
      paid_amount: parseMoneyValue(row.paid_amount || 0),
      overpaid_by: parseMoneyValue((row.paid_amount || 0) - (row.expected_amount || 0))
    });
  });

  db.prepare(
    `SELECT id, entry_date as record_date, total_amount as expected_amount, paid_amount as paid_amount,
            ('IMPORT#' || id) as reference
     FROM import_entries
     WHERE entry_date BETWEEN ? AND ?
       AND paid_amount > total_amount`
  ).all(from, to).forEach((row) => {
    overpayments.push({
      source: "IMPORT",
      date: row.record_date,
      reference: row.reference,
      expected_amount: parseMoneyValue(row.expected_amount || 0),
      paid_amount: parseMoneyValue(row.paid_amount || 0),
      overpaid_by: parseMoneyValue((row.paid_amount || 0) - (row.expected_amount || 0))
    });
  });

  db.prepare(
    `SELECT id, purchase_date as record_date, amount as expected_amount, paid_amount as paid_amount,
            ('PURCHASE#' || id) as reference
     FROM company_purchases
     WHERE purchase_date BETWEEN ? AND ?
       AND paid_amount > amount`
  ).all(from, to).forEach((row) => {
    overpayments.push({
      source: "COMPANY_PURCHASE",
      date: row.record_date,
      reference: row.reference,
      expected_amount: parseMoneyValue(row.expected_amount || 0),
      paid_amount: parseMoneyValue(row.paid_amount || 0),
      overpaid_by: parseMoneyValue((row.paid_amount || 0) - (row.expected_amount || 0))
    });
  });

  db.prepare(
    `SELECT id, expense_date as record_date, amount as expected_amount, paid_amount as paid_amount,
            ('VEXP#' || id) as reference
     FROM vehicle_expenses
     WHERE expense_date BETWEEN ? AND ?
       AND paid_amount > amount`
  ).all(from, to).forEach((row) => {
    overpayments.push({
      source: "VEHICLE_EXPENSE",
      date: row.record_date,
      reference: row.reference,
      expected_amount: parseMoneyValue(row.expected_amount || 0),
      paid_amount: parseMoneyValue(row.paid_amount || 0),
      overpaid_by: parseMoneyValue((row.paid_amount || 0) - (row.expected_amount || 0))
    });
  });

  overpayments.sort((a, b) => b.overpaid_by - a.overpaid_by || String(b.date).localeCompare(String(a.date)));

  const negativeMarginTrips = db.prepare(
    `SELECT exports.id, exports.export_date, vehicles.vehicle_number, vehicles.owner_name,
            exports.collection_amount, exports.expense_amount,
            (exports.collection_amount - exports.expense_amount) as margin
     FROM exports
     JOIN vehicles ON vehicles.id = exports.vehicle_id
     WHERE exports.export_date BETWEEN ? AND ?
       AND vehicles.is_company = 1
       AND (exports.collection_amount - exports.expense_amount) < 0
     ORDER BY exports.export_date DESC, margin ASC`
  ).all(from, to).map((row) => ({
    ...row,
    margin: roundMoneySigned(row.margin || 0)
  }));

  const quantityIssues = db.prepare(
    `SELECT exports.id, exports.export_date, vehicles.vehicle_number, vehicles.owner_name,
            exports.jar_count, exports.return_jar_count, exports.leakage_jar_count,
            exports.bottle_case_count, exports.return_bottle_case_count
     FROM exports
     JOIN vehicles ON vehicles.id = exports.vehicle_id
     WHERE exports.export_date BETWEEN ? AND ?
       AND (
         (exports.return_jar_count + exports.leakage_jar_count) > exports.jar_count
         OR exports.return_bottle_case_count > exports.bottle_case_count
       )
     ORDER BY exports.export_date DESC, exports.id DESC`
  ).all(from, to);

  const repeatedEdits = db.prepare(
    `SELECT date(activity_logs.created_at) as edit_date,
            activity_logs.entity_type,
            activity_logs.entity_id,
            COUNT(*) as edit_count,
            GROUP_CONCAT(DISTINCT COALESCE(users.full_name, '-')) as editors,
            MAX(activity_logs.created_at) as last_edit_at
     FROM activity_logs
     LEFT JOIN users ON users.id = activity_logs.user_id
     WHERE activity_logs.action = 'update'
       AND activity_logs.entity_id IS NOT NULL
       AND date(activity_logs.created_at) BETWEEN ? AND ?
     GROUP BY date(activity_logs.created_at), activity_logs.entity_type, activity_logs.entity_id
     HAVING COUNT(*) >= 2
     ORDER BY edit_count DESC, edit_date DESC`
  ).all(from, to);

  const summary = {
    overpayments: overpayments.length,
    negativeMargins: negativeMarginTrips.length,
    quantityIssues: quantityIssues.length,
    repeatedEdits: repeatedEdits.length
  };

  return res.render("records/leakage_guard", {
    title: req.t("leakageGuardTitle"),
    from,
    to,
    summary,
    overpayments,
    negativeMarginTrips,
    quantityIssues,
    repeatedEdits
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

  res.render("records/savings", {
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

router.get("/savings/export-csv", (req, res) => {
  const from = req.query.from || dayjs().startOf("month").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const vehicleId = req.query.vehicle_id || "all";
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

  const header = "Date,Vehicle,Type,Source,Amount,Note,Recorded By";
  const lines = entries.map((row) => {
    const entryType = row.amount < 0 ? "withdraw" : "deposit";
    const paymentSource = row.payment_source || "DAILY_COLLECTION";
    const safe = [
      row.entry_date,
      `${row.owner_name} • ${row.vehicle_number}`,
      entryType,
      paymentSource,
      Math.abs(row.amount),
      row.note || "",
      row.recorded_by || ""
    ].map((val) => {
      const str = String(val ?? "").replace(/\"/g, "\"\"");
      return `"${str}"`;
    });
    return safe.join(",");
  });

  const csv = [header, ...lines].join("\n");
  res.setHeader("Content-Type", "text/csv");
  res.setHeader("Content-Disposition", `attachment; filename="savings_${from}_to_${to}.csv"`);
  res.send(csv);
});

router.get("/savings/print", (req, res) => {
  const from = req.query.from || dayjs().startOf("month").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const vehicleId = req.query.vehicle_id || "all";
  const vehicleClause = vehicleId === "all" ? "" : "AND vehicle_savings.vehicle_id = ?";
  const params = vehicleId === "all" ? [from, to] : [from, to, vehicleId];
  const entries = db.prepare(
    `SELECT vehicle_savings.*, vehicles.vehicle_number, vehicles.owner_name
     FROM vehicle_savings
     JOIN vehicles ON vehicle_savings.vehicle_id = vehicles.id
     WHERE entry_date BETWEEN ? AND ?
     ${vehicleClause}
     ORDER BY entry_date DESC, created_at DESC`
  ).all(...params);

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

  res.render("records/savings_print", {
    title: req.t("savingsTitle"),
    from,
    to,
    printedAt: dayjs().format("YYYY-MM-DD HH:mm"),
    entries,
    totals
  });
});

router.get("/savings/:id/edit", (req, res) => {
  const entry = db.prepare(
    `SELECT vehicle_savings.*, vehicles.vehicle_number, vehicles.owner_name
     FROM vehicle_savings
     JOIN vehicles ON vehicle_savings.vehicle_id = vehicles.id
     WHERE vehicle_savings.id = ?`
  ).get(req.params.id);
  if (!entry) return res.redirect("/records/savings");
  const vehicles = db.prepare(
    "SELECT id, vehicle_number, owner_name FROM vehicles WHERE is_company = 0 ORDER BY vehicle_number"
  ).all();
  const entryType = entry.amount < 0 ? "withdraw" : "deposit";
  res.render("records/savings_edit", {
    title: req.t("editSavingsEntryTitle"),
    entry,
    vehicles,
    entryType
  });
});

router.post("/savings", (req, res) => {
  const { vehicle_id, entry_date, amount, entry_type, payment_source, note } = req.body;
  if (!vehicle_id || !entry_date) {
    return res.redirect("/records/savings");
  }
  let amt = Number(amount || 0);
  if (Number.isNaN(amt) || amt <= 0) {
    return res.redirect("/records/savings");
  }
  const type = entry_type === "withdraw" ? "withdraw" : "deposit";
  if (type === "withdraw") amt = -Math.abs(amt);
  const source = type === "withdraw" ? normalizeSalaryPaymentSource(payment_source) : "DAILY_COLLECTION";

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

  res.redirect(`/records/savings?from=${entry_date}&to=${entry_date}&vehicle_id=${vehicle_id}`);
});

router.post("/savings/:id", (req, res) => {
  const entry = db.prepare("SELECT * FROM vehicle_savings WHERE id = ?").get(req.params.id);
  if (!entry) return res.redirect("/records/savings");
  const { vehicle_id, entry_date, amount, entry_type, payment_source, note } = req.body;
  if (!vehicle_id || !entry_date) {
    return res.redirect(`/records/savings/${req.params.id}/edit`);
  }
  let amt = Number(amount || 0);
  if (Number.isNaN(amt) || amt <= 0) {
    return res.redirect(`/records/savings/${req.params.id}/edit`);
  }
  const type = entry_type === "withdraw" ? "withdraw" : "deposit";
  if (type === "withdraw") amt = -Math.abs(amt);
  const source = type === "withdraw" ? normalizeSalaryPaymentSource(payment_source) : "DAILY_COLLECTION";

  db.prepare(
    "UPDATE vehicle_savings SET vehicle_id = ?, entry_date = ?, amount = ?, payment_source = ?, note = ? WHERE id = ?"
  ).run(vehicle_id, entry_date, amt, source, note || null, req.params.id);

  logActivity({
    userId: req.session.userId,
    action: "update",
    entityType: "vehicle_savings",
    entityId: req.params.id,
    details: buildDiffDetails(
      entry,
      {
        vehicle_id: Number(vehicle_id),
        entry_date,
        amount: amt,
        payment_source: source,
        note: note || null
      },
      ["vehicle_id", "entry_date", "amount", "payment_source", "note"]
    )
  });

  res.redirect(`/records/savings?from=${entry_date}&to=${entry_date}&vehicle_id=${vehicle_id}`);
});

router.post("/savings/:id/delete", (req, res) => {
  const entry = db.prepare("SELECT * FROM vehicle_savings WHERE id = ?").get(req.params.id);
  if (!entry) return res.redirect("/records/savings");
  const recycleId = createRecycleEntry({
    entityType: "vehicle_savings",
    entityId: req.params.id,
    payload: { vehicle_savings: entry },
    deletedBy: req.session.userId,
    note: `date=${entry.entry_date || ""}; amount=${entry.amount || 0}`
  });
  logActivity({
    userId: req.session.userId,
    action: "delete",
    entityType: "vehicle_savings",
    entityId: req.params.id,
    details: `recycle_id=${recycleId}`
  });
  db.prepare("DELETE FROM vehicle_savings WHERE id = ?").run(req.params.id);
  res.redirect(`/records/savings?from=${entry.entry_date}&to=${entry.entry_date}&vehicle_id=${entry.vehicle_id}`);
});

router.get("/jar-sales", (req, res) => {
  const from = req.query.from || dayjs().subtract(7, "day").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const q = (req.query.q || "").trim();
  const status = String(req.query.status || "").trim();
  const errorKey = String(req.query.error || "").trim();
  const searchClause = q ? "AND (jar_types.name LIKE ? OR jar_sales.customer_name LIKE ? OR jar_sales.vehicle_number LIKE ? OR vehicles.vehicle_number LIKE ? OR vehicles.owner_name LIKE ?)" : "";
  const params = q ? [from, to, `%${q}%`, `%${q}%`, `%${q}%`, `%${q}%`, `%${q}%`] : [from, to];

  const rows = db.prepare(
    `SELECT jar_sales.*, jar_types.name as jar_name, users.full_name as recorded_by,
            vehicles.vehicle_number as vehicle_number_ref, vehicles.owner_name as owner_name, vehicles.is_company as is_company,
            CASE
              WHEN jar_sales.total_amount - jar_sales.paid_amount < 0 THEN 0
              ELSE jar_sales.total_amount - jar_sales.paid_amount
            END as remaining_amount
     FROM jar_sales
     JOIN jar_types ON jar_sales.jar_type_id = jar_types.id
     LEFT JOIN vehicles ON jar_sales.vehicle_id = vehicles.id
     LEFT JOIN users ON jar_sales.created_by = users.id
     WHERE jar_sales.sale_date BETWEEN ? AND ?
     ${searchClause}
     ORDER BY jar_sales.sale_date DESC, jar_sales.created_at DESC`
  ).all(...params);

  const totals = rows.reduce(
    (acc, row) => {
      const total = parseMoneyValue(row.total_amount || 0);
      const paid = parseMoneyValue(row.paid_amount || 0);
      const remaining = computeRemainingMoney(total, paid);
      acc.total = parseMoneyValue(acc.total + total);
      acc.paid = parseMoneyValue(acc.paid + paid);
      acc.credit = parseMoneyValue(acc.credit + remaining);
      acc.qty += Number(row.quantity || 0);
      return acc;
    },
    { total: 0, paid: 0, credit: 0, qty: 0 }
  );

  res.render("records/jar_sales", {
    title: req.t("jarSalesTitle"),
    from,
    to,
    q,
    rows,
    totals,
    success: status === "payment_saved" ? req.t("jarSalePaymentSaved") : status === "payment_deleted" ? req.t("jarSalePaymentDeleted") : null,
    error: errorKey ? req.t(errorKey) : null
  });
});

router.get("/jar-sales/:id/payments", (req, res) => {
  const record = db.prepare(
    `SELECT jar_sales.*, jar_types.name as jar_name,
            vehicles.vehicle_number as vehicle_number_ref, vehicles.owner_name, vehicles.is_company
     FROM jar_sales
     JOIN jar_types ON jar_sales.jar_type_id = jar_types.id
     LEFT JOIN vehicles ON jar_sales.vehicle_id = vehicles.id
     WHERE jar_sales.id = ?`
  ).get(req.params.id);
  if (!record) return res.redirect("/records/jar-sales");
  const payments = db.prepare(
    `SELECT jar_sale_payments.*, users.full_name as recorded_by
     FROM jar_sale_payments
     LEFT JOIN users ON jar_sale_payments.created_by = users.id
     WHERE jar_sale_payments.jar_sale_id = ?
     ORDER BY jar_sale_payments.payment_date DESC, jar_sale_payments.id DESC`
  ).all(req.params.id);
  const remaining = computeRemainingMoney(record.total_amount || 0, record.paid_amount || 0);
  const errorKey = String(req.query.error || "").trim();
  const status = String(req.query.status || "").trim();
  res.render("records/jar_sale_payments", {
    title: req.t("jarSalePaymentsTitle"),
    record,
    payments,
    remaining,
    error: errorKey ? req.t(errorKey) : null,
    success: status === "payment_saved" ? req.t("jarSalePaymentSaved") : status === "payment_deleted" ? req.t("jarSalePaymentDeleted") : null
  });
});

router.post("/jar-sales/:id/payments", (req, res) => {
  const record = db.prepare("SELECT * FROM jar_sales WHERE id = ?").get(req.params.id);
  if (!record) return res.redirect("/records/jar-sales");
  const paymentDate = req.body.payment_date || dayjs().format("YYYY-MM-DD");
  const amount = parseMoneyValue(req.body.amount || 0);
  const note = String(req.body.note || "").trim();
  if (Number.isNaN(amount) || amount <= 0) {
    return res.redirect(`/records/jar-sales/${req.params.id}/payments?error=invalidPaymentAmount`);
  }
  const remaining = computeRemainingMoney(record.total_amount || 0, record.paid_amount || 0);
  if (remaining <= 0) {
    return res.redirect(`/records/jar-sales/${req.params.id}/payments?error=noBalanceDue`);
  }
  if (amount > remaining) {
    return res.redirect(`/records/jar-sales/${req.params.id}/payments?error=paidMoreThanDue`);
  }

  const newPaid = parseMoneyValue(parseMoneyValue(record.paid_amount || 0) + amount);
  const newCredit = computeRemainingMoney(record.total_amount || 0, newPaid);
  db.exec("BEGIN;");
  try {
    db.prepare(
      "INSERT INTO jar_sale_payments (jar_sale_id, payment_date, amount, note, created_by) VALUES (?, ?, ?, ?, ?)"
    ).run(req.params.id, paymentDate, amount, note || null, req.session.userId || null);
    db.prepare("UPDATE jar_sales SET paid_amount = ?, credit_amount = ? WHERE id = ?").run(
      newPaid,
      newCredit,
      req.params.id
    );
    db.exec("COMMIT;");
  } catch (err) {
    db.exec("ROLLBACK;");
    throw err;
  }

  logActivity({
    userId: req.session.userId,
    action: "payment",
    entityType: "jar_sale",
    entityId: req.params.id,
    details: `payment=${amount}; paid_amount=${newPaid}; credit_amount=${newCredit}`
  });

  return res.redirect(`/records/jar-sales/${req.params.id}/payments?status=payment_saved`);
});

router.post("/jar-sales/payments/:id/delete", (req, res) => {
  const payment = db.prepare("SELECT * FROM jar_sale_payments WHERE id = ?").get(req.params.id);
  if (!payment) return res.redirect("/records/jar-sales");
  const record = db.prepare("SELECT * FROM jar_sales WHERE id = ?").get(payment.jar_sale_id);
  if (!record) return res.redirect("/records/jar-sales");
  const revertedPaid = parseMoneyValue(Math.max(0, parseMoneyValue(record.paid_amount || 0) - parseMoneyValue(payment.amount || 0)));
  const newCredit = computeRemainingMoney(record.total_amount || 0, revertedPaid);
  db.exec("BEGIN;");
  try {
    db.prepare("DELETE FROM jar_sale_payments WHERE id = ?").run(req.params.id);
    db.prepare("UPDATE jar_sales SET paid_amount = ?, credit_amount = ? WHERE id = ?").run(
      revertedPaid,
      newCredit,
      record.id
    );
    db.exec("COMMIT;");
  } catch (err) {
    db.exec("ROLLBACK;");
    throw err;
  }

  logActivity({
    userId: req.session.userId,
    action: "delete",
    entityType: "jar_sale_payment",
    entityId: req.params.id,
    details: `jar_sale_id=${record.id}; amount=${payment.amount || 0}`
  });

  return res.redirect(`/records/jar-sales/${record.id}/payments?status=payment_deleted`);
});

router.get("/jar-sales/new", (req, res) => {
  const jarTypes = db.prepare("SELECT id, name FROM jar_types WHERE active = 1 ORDER BY name").all();
  const vehicles = db.prepare("SELECT id, vehicle_number, owner_name, is_company FROM vehicles ORDER BY vehicle_number").all();
  const importTotals = db.prepare(
    `SELECT jar_type_id, COALESCE(SUM(quantity), 0) as qty
     FROM import_entries
     WHERE item_type = 'JAR_CONTAINER' AND direction = 'IN' AND jar_type_id IS NOT NULL
     GROUP BY jar_type_id`
  ).all();
  const salesTotals = db.prepare(
    `SELECT jar_type_id, COALESCE(SUM(quantity), 0) as qty
     FROM jar_sales
     GROUP BY jar_type_id`
  ).all();
  const importMap = importTotals.reduce((acc, row) => {
    acc[row.jar_type_id] = Number(row.qty || 0);
    return acc;
  }, {});
  const salesMap = salesTotals.reduce((acc, row) => {
    acc[row.jar_type_id] = Number(row.qty || 0);
    return acc;
  }, {});
  const jarTypeBalances = jarTypes.reduce((acc, type) => {
    acc[type.id] = (importMap[type.id] || 0) - (salesMap[type.id] || 0);
    return acc;
  }, {});
  res.render("records/jar_sale_form", {
    title: req.t("addJarSaleTitle"),
    record: null,
    jarTypes,
    vehicles,
    jarTypeBalances,
    error: null,
    defaultDate: dayjs().format("YYYY-MM-DD")
  });
});

router.post("/jar-sales", (req, res) => {
  const { jar_type_id, sale_date, quantity, unit_price, paid_amount, note, customer_name, vehicle_id, vehicle_number } = req.body;
  const jarTypes = db.prepare("SELECT id, name FROM jar_types WHERE active = 1 ORDER BY name").all();
  const vehicles = db.prepare("SELECT id, vehicle_number, owner_name, is_company FROM vehicles ORDER BY vehicle_number").all();
  const importTotals = db.prepare(
    `SELECT jar_type_id, COALESCE(SUM(quantity), 0) as qty
     FROM import_entries
     WHERE item_type = 'JAR_CONTAINER' AND direction = 'IN' AND jar_type_id IS NOT NULL
     GROUP BY jar_type_id`
  ).all();
  const salesTotals = db.prepare(
    `SELECT jar_type_id, COALESCE(SUM(quantity), 0) as qty
     FROM jar_sales
     GROUP BY jar_type_id`
  ).all();
  const importMap = importTotals.reduce((acc, row) => {
    acc[row.jar_type_id] = Number(row.qty || 0);
    return acc;
  }, {});
  const salesMap = salesTotals.reduce((acc, row) => {
    acc[row.jar_type_id] = Number(row.qty || 0);
    return acc;
  }, {});
  const jarTypeBalances = jarTypes.reduce((acc, type) => {
    acc[type.id] = (importMap[type.id] || 0) - (salesMap[type.id] || 0);
    return acc;
  }, {});
  if (!jar_type_id || !sale_date) {
    return res.render("records/jar_sale_form", {
      title: req.t("addJarSaleTitle"),
      record: null,
      jarTypes,
      vehicles,
      jarTypeBalances,
      error: req.t("jarSaleRequired"),
      defaultDate: sale_date || dayjs().format("YYYY-MM-DD")
    });
  }
  const type = db.prepare("SELECT id FROM jar_types WHERE id = ?").get(jar_type_id);
  if (!type) {
    return res.render("records/jar_sale_form", {
      title: req.t("addJarSaleTitle"),
      record: null,
      jarTypes,
      vehicles,
      jarTypeBalances,
      error: req.t("jarTypeRequired"),
      defaultDate: sale_date || dayjs().format("YYYY-MM-DD")
    });
  }
  const qty = Number(quantity || 0);
  const available = (jarTypeBalances[jar_type_id] || 0);
  if (available <= 0) {
    return res.render("records/jar_sale_form", {
      title: req.t("addJarSaleTitle"),
      record: null,
      jarTypes,
      vehicles,
      jarTypeBalances,
      error: req.t("jarContainerOutOfStock"),
      defaultDate: sale_date || dayjs().format("YYYY-MM-DD")
    });
  }
  if (qty > available) {
    return res.render("records/jar_sale_form", {
      title: req.t("addJarSaleTitle"),
      record: null,
      jarTypes,
      vehicles,
      jarTypeBalances,
      error: req.t("jarContainerInsufficient"),
      defaultDate: sale_date || dayjs().format("YYYY-MM-DD")
    });
  }
  let unitPrice = Number(unit_price || 0);
  if (Number.isNaN(unitPrice) || unitPrice < 0) {
    return res.render("records/jar_sale_form", {
      title: req.t("addJarSaleTitle"),
      record: null,
      jarTypes,
      vehicles,
      jarTypeBalances,
      error: req.t("jarSalePriceInvalid"),
      defaultDate: sale_date || dayjs().format("YYYY-MM-DD")
    });
  }
  const selectedVehicleId = vehicle_id ? Number(vehicle_id) : null;
  const vehicleRow = selectedVehicleId
    ? db.prepare("SELECT vehicle_number, is_company FROM vehicles WHERE id = ?").get(selectedVehicleId)
    : null;
  const isCompany = vehicleRow && Number(vehicleRow.is_company) === 1;
  if (isCompany) unitPrice = 0;
  const totalAmount = qty * unitPrice;
  let paidAmount = Number(paid_amount || 0);
  if (Number.isNaN(paidAmount) || paidAmount < 0) paidAmount = 0;
  if (paidAmount > totalAmount) paidAmount = totalAmount;
  if (isCompany) paidAmount = 0;
  const creditAmount = totalAmount - paidAmount;
  const vehicleNumberValue = vehicleRow ? vehicleRow.vehicle_number : (vehicle_number ? vehicle_number.trim() : null);
  db.prepare(
    "INSERT INTO jar_sales (jar_type_id, customer_name, vehicle_id, vehicle_number, sale_date, quantity, unit_price, total_amount, paid_amount, credit_amount, note, created_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
  ).run(
    jar_type_id,
    customer_name ? customer_name.trim() : null,
    selectedVehicleId || null,
    vehicleNumberValue,
    sale_date,
    qty,
    unitPrice,
    totalAmount,
    paidAmount,
    creditAmount,
    note || null,
    req.session.userId
  );

  logActivity({
    userId: req.session.userId,
    action: "create",
    entityType: "jar_sale",
    entityId: `${jar_type_id}_${sale_date}`,
    details: `qty=${qty}, price=${unitPrice}`
  });

  res.redirect(`/records/jar-sales?from=${sale_date}&to=${sale_date}`);
});

router.get("/jar-sales/:id/edit", (req, res) => {
  const record = db.prepare("SELECT * FROM jar_sales WHERE id = ?").get(req.params.id);
  if (!record) return res.redirect("/records/jar-sales");
  const jarTypes = db.prepare("SELECT id, name FROM jar_types ORDER BY name").all();
  const vehicles = db.prepare("SELECT id, vehicle_number, owner_name, is_company FROM vehicles ORDER BY vehicle_number").all();
  const importTotals = db.prepare(
    `SELECT jar_type_id, COALESCE(SUM(quantity), 0) as qty
     FROM import_entries
     WHERE item_type = 'JAR_CONTAINER' AND direction = 'IN' AND jar_type_id IS NOT NULL
     GROUP BY jar_type_id`
  ).all();
  const salesTotals = db.prepare(
    `SELECT jar_type_id, COALESCE(SUM(quantity), 0) as qty
     FROM jar_sales
     GROUP BY jar_type_id`
  ).all();
  const importMap = importTotals.reduce((acc, row) => {
    acc[row.jar_type_id] = Number(row.qty || 0);
    return acc;
  }, {});
  const salesMap = salesTotals.reduce((acc, row) => {
    acc[row.jar_type_id] = Number(row.qty || 0);
    return acc;
  }, {});
  const jarTypeBalances = jarTypes.reduce((acc, type) => {
    acc[type.id] = (importMap[type.id] || 0) - (salesMap[type.id] || 0);
    return acc;
  }, {});
  if (record.jar_type_id && jarTypeBalances[record.jar_type_id] !== undefined) {
    jarTypeBalances[record.jar_type_id] += Number(record.quantity || 0);
  }
  res.render("records/jar_sale_form", {
    title: req.t("editJarSaleTitle"),
    record,
    jarTypes,
    vehicles,
    jarTypeBalances,
    error: null,
    defaultDate: record.sale_date
  });
});

router.post("/jar-sales/:id", (req, res) => {
  const { jar_type_id, sale_date, quantity, unit_price, paid_amount, note, customer_name, vehicle_id, vehicle_number } = req.body;
  const record = db.prepare("SELECT * FROM jar_sales WHERE id = ?").get(req.params.id);
  const jarTypes = db.prepare("SELECT id, name FROM jar_types ORDER BY name").all();
  const vehicles = db.prepare("SELECT id, vehicle_number, owner_name, is_company FROM vehicles ORDER BY vehicle_number").all();
  const importTotals = db.prepare(
    `SELECT jar_type_id, COALESCE(SUM(quantity), 0) as qty
     FROM import_entries
     WHERE item_type = 'JAR_CONTAINER' AND direction = 'IN' AND jar_type_id IS NOT NULL
     GROUP BY jar_type_id`
  ).all();
  const salesTotals = db.prepare(
    `SELECT jar_type_id, COALESCE(SUM(quantity), 0) as qty
     FROM jar_sales
     GROUP BY jar_type_id`
  ).all();
  const importMap = importTotals.reduce((acc, row) => {
    acc[row.jar_type_id] = Number(row.qty || 0);
    return acc;
  }, {});
  const salesMap = salesTotals.reduce((acc, row) => {
    acc[row.jar_type_id] = Number(row.qty || 0);
    return acc;
  }, {});
  const jarTypeBalances = jarTypes.reduce((acc, type) => {
    acc[type.id] = (importMap[type.id] || 0) - (salesMap[type.id] || 0);
    return acc;
  }, {});
  if (record && record.jar_type_id && jarTypeBalances[record.jar_type_id] !== undefined) {
    jarTypeBalances[record.jar_type_id] += Number(record.quantity || 0);
  }
  if (!record) return res.redirect("/records/jar-sales");
  if (!jar_type_id || !sale_date) {
    return res.render("records/jar_sale_form", {
      title: req.t("editJarSaleTitle"),
      record,
      jarTypes,
      vehicles,
      jarTypeBalances,
      error: req.t("jarSaleRequired"),
      defaultDate: sale_date || record.sale_date
    });
  }
  const type = db.prepare("SELECT id FROM jar_types WHERE id = ?").get(jar_type_id);
  if (!type) {
    return res.render("records/jar_sale_form", {
      title: req.t("editJarSaleTitle"),
      record,
      jarTypes,
      vehicles,
      jarTypeBalances,
      error: req.t("jarTypeRequired"),
      defaultDate: sale_date || record.sale_date
    });
  }
  const qty = Number(quantity || 0);
  const available = (jarTypeBalances[jar_type_id] || 0);
  if (available <= 0) {
    return res.render("records/jar_sale_form", {
      title: req.t("editJarSaleTitle"),
      record,
      jarTypes,
      vehicles,
      jarTypeBalances,
      error: req.t("jarContainerOutOfStock"),
      defaultDate: sale_date || record.sale_date
    });
  }
  if (qty > available) {
    return res.render("records/jar_sale_form", {
      title: req.t("editJarSaleTitle"),
      record,
      jarTypes,
      vehicles,
      jarTypeBalances,
      error: req.t("jarContainerInsufficient"),
      defaultDate: sale_date || record.sale_date
    });
  }
  let unitPrice = Number(unit_price || 0);
  if (Number.isNaN(unitPrice) || unitPrice < 0) {
    return res.render("records/jar_sale_form", {
      title: req.t("editJarSaleTitle"),
      record,
      jarTypes,
      vehicles,
      jarTypeBalances,
      error: req.t("jarSalePriceInvalid"),
      defaultDate: sale_date || record.sale_date
    });
  }
  const selectedVehicleId = vehicle_id ? Number(vehicle_id) : null;
  const vehicleRow = selectedVehicleId
    ? db.prepare("SELECT vehicle_number, is_company FROM vehicles WHERE id = ?").get(selectedVehicleId)
    : null;
  const isCompany = vehicleRow && Number(vehicleRow.is_company) === 1;
  if (isCompany) unitPrice = 0;
  const totalAmount = qty * unitPrice;
  let paidAmount = Number(paid_amount || 0);
  if (Number.isNaN(paidAmount) || paidAmount < 0) paidAmount = 0;
  if (paidAmount > totalAmount) paidAmount = totalAmount;
  if (isCompany) paidAmount = 0;
  const creditAmount = totalAmount - paidAmount;
  const vehicleNumberValue = vehicleRow ? vehicleRow.vehicle_number : (vehicle_number ? vehicle_number.trim() : null);

  db.prepare(
    "UPDATE jar_sales SET jar_type_id = ?, customer_name = ?, vehicle_id = ?, vehicle_number = ?, sale_date = ?, quantity = ?, unit_price = ?, total_amount = ?, paid_amount = ?, credit_amount = ?, note = ? WHERE id = ?"
  ).run(
    jar_type_id,
    customer_name ? customer_name.trim() : null,
    selectedVehicleId || null,
    vehicleNumberValue,
    sale_date,
    qty,
    unitPrice,
    totalAmount,
    paidAmount,
    creditAmount,
    note || null,
    req.params.id
  );

  logActivity({
    userId: req.session.userId,
    action: "update",
    entityType: "jar_sale",
    entityId: req.params.id,
    details: buildDiffDetails(
      record,
      {
        jar_type_id: Number(jar_type_id),
        customer_name: customer_name ? customer_name.trim() : null,
        vehicle_id: selectedVehicleId || null,
        vehicle_number: vehicleNumberValue,
        sale_date,
        quantity: qty,
        unit_price: unitPrice,
        total_amount: totalAmount,
        paid_amount: paidAmount,
        credit_amount: creditAmount,
        note: note || null
      },
      [
        "jar_type_id",
        "customer_name",
        "vehicle_id",
        "vehicle_number",
        "sale_date",
        "quantity",
        "unit_price",
        "total_amount",
        "paid_amount",
        "credit_amount",
        "note"
      ]
    )
  });

  res.redirect(`/records/jar-sales?from=${sale_date}&to=${sale_date}`);
});

router.post("/jar-sales/:id/delete", (req, res) => {
  const record = db.prepare("SELECT * FROM jar_sales WHERE id = ?").get(req.params.id);
  if (!record) return res.redirect("/records/jar-sales");
  const recycleId = createRecycleEntry({
    entityType: "jar_sale",
    entityId: req.params.id,
    payload: { jar_sale: record },
    deletedBy: req.session.userId,
    note: `date=${record.sale_date || ""}; qty=${record.quantity || 0}`
  });
  logActivity({
    userId: req.session.userId,
    action: "delete",
    entityType: "jar_sale",
    entityId: req.params.id,
    details: `recycle_id=${recycleId}`
  });
  db.prepare("DELETE FROM jar_sales WHERE id = ?").run(req.params.id);
  res.redirect("/records/jar-sales");
});

router.get("/jar-sales/:id/print", (req, res) => {
  const record = db.prepare(
    `SELECT jar_sales.*, jar_types.name as jar_name, vehicles.vehicle_number as vehicle_number_ref, vehicles.owner_name, vehicles.is_company
     FROM jar_sales
     JOIN jar_types ON jar_sales.jar_type_id = jar_types.id
     LEFT JOIN vehicles ON jar_sales.vehicle_id = vehicles.id
     WHERE jar_sales.id = ?`
  ).get(req.params.id);
  if (!record) return res.redirect("/records/jar-sales");
  res.render("records/jar_sale_print", { title: req.t("jarSalesTitle"), record });
});

router.get("/jar-sales/export", (req, res) => {
  const from = req.query.from || dayjs().subtract(7, "day").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const q = (req.query.q || "").trim();
  const searchClause = q ? "AND (jar_types.name LIKE ? OR jar_sales.customer_name LIKE ? OR jar_sales.vehicle_number LIKE ? OR vehicles.vehicle_number LIKE ? OR vehicles.owner_name LIKE ?)" : "";
  const params = q ? [from, to, `%${q}%`, `%${q}%`, `%${q}%`, `%${q}%`, `%${q}%`] : [from, to];

  const rows = db.prepare(
    `SELECT jar_sales.sale_date, jar_types.name as jar_name, jar_sales.customer_name,
            COALESCE(vehicles.vehicle_number, jar_sales.vehicle_number) as vehicle_number,
            vehicles.owner_name as owner_name, vehicles.is_company as is_company,
            jar_sales.quantity, jar_sales.unit_price,
            jar_sales.total_amount, jar_sales.paid_amount, jar_sales.credit_amount, jar_sales.note
     FROM jar_sales
     JOIN jar_types ON jar_sales.jar_type_id = jar_types.id
     LEFT JOIN vehicles ON jar_sales.vehicle_id = vehicles.id
     WHERE jar_sales.sale_date BETWEEN ? AND ?
     ${searchClause}
     ORDER BY jar_sales.sale_date ASC`
  ).all(...params);

  const header = "Date,Jar Type,Person Name,Vehicle Number,Owner Name,Company Vehicle,Quantity,Unit Price,Total Amount,Paid Amount,Credit Amount,Note";
  const lines = rows.map((row) => {
    const safe = [
      row.sale_date,
      row.jar_name,
      row.customer_name || "",
      row.vehicle_number || "",
      row.owner_name || "",
      row.is_company ? "Yes" : "No",
      row.quantity,
      row.unit_price,
      row.total_amount,
      row.paid_amount,
      row.credit_amount,
      row.note || ""
    ].map((val) => `"${String(val ?? "").replace(/\"/g, "\"\"")}"`);
    return safe.join(",");
  });
  res.setHeader("Content-Type", "text/csv");
  res.setHeader("Content-Disposition", `attachment; filename="jar_sales_${from}_to_${to}.csv"`);
  res.send([header, ...lines].join("\n"));
});

router.get("/credits", (req, res) => {
  const from = req.query.from || dayjs().subtract(7, "day").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const customerCreditFrom = req.query.customer_credit_from || from;
  const customerCreditTo = req.query.customer_credit_to || to;
  const q = (req.query.q || "").trim();
  const statusRaw = req.query.status || "all";
  const status = ["all", "paid", "unpaid", "partial"].includes(statusRaw) ? statusRaw : "all";
  const notice = String(req.query.notice || "").trim();
  const error = String(req.query.error || "").trim();
  const sortRaw = req.query.sort || "date_desc";
  const sortMap = {
    date_desc: "credit_date DESC, credits.created_at DESC",
    date_asc: "credit_date ASC, credits.created_at ASC",
    amount_desc: "credits.amount DESC",
    amount_asc: "credits.amount ASC",
    jars_desc: "credits.credit_jars DESC",
    paid_first: "credits.paid DESC, credits.created_at DESC",
    unpaid_first: "credits.paid ASC, credits.created_at DESC",
    paid_amount_desc: "credits.paid_amount DESC",
    remaining_desc: "(credits.amount - credits.paid_amount) DESC"
  };
  const sort = sortMap[sortRaw] ? sortRaw : "date_desc";
  const orderBy = sortMap[sort];

  let statusClause = "";
  if (status === "paid") statusClause = "AND credits.paid_amount >= credits.amount";
  if (status === "unpaid") statusClause = "AND credits.paid_amount <= 0";
  if (status === "partial") statusClause = "AND credits.paid_amount > 0 AND credits.paid_amount < credits.amount";
  const searchClause = q ? "AND (vehicles.vehicle_number LIKE ? OR credits.customer_name LIKE ?)" : "";

  const creditsParams = q ? [from, to, `%${q}%`, `%${q}%`] : [from, to];
  const creditsRows = db.prepare(
    `SELECT credits.*, vehicles.vehicle_number, vehicles.owner_name,
            users.full_name as recorded_by,
            credit_export.receipt_no as trip_receipt_no,
            COALESCE(credits.trip_date, credit_export.export_date) as trip_date,
            checked_staff.full_name as checked_by_staff_name,
            COALESCE(cps.paid_cash_amount, 0) as paid_cash_amount,
            COALESCE(cps.paid_bank_amount, 0) as paid_bank_amount,
            COALESCE(cps.paid_ewallet_amount, 0) as paid_ewallet_amount,
            CASE WHEN credits.amount - credits.paid_amount < 0 THEN 0 ELSE credits.amount - credits.paid_amount END AS remaining_amount
     FROM credits
     JOIN vehicles ON credits.vehicle_id = vehicles.id
     LEFT JOIN users ON credits.created_by = users.id
     LEFT JOIN exports as credit_export ON credits.export_id = credit_export.id
     LEFT JOIN staff as checked_staff ON credits.checked_by_staff_id = checked_staff.id
     LEFT JOIN (
       SELECT credit_id,
              COALESCE(SUM(CASE WHEN payment_method = 'CASH' THEN amount ELSE 0 END), 0) as paid_cash_amount,
              COALESCE(SUM(CASE WHEN payment_method = 'BANK' THEN amount ELSE 0 END), 0) as paid_bank_amount,
              COALESCE(SUM(CASE WHEN payment_method = 'E_WALLET' THEN amount ELSE 0 END), 0) as paid_ewallet_amount
       FROM credit_payments
       GROUP BY credit_id
     ) as cps ON cps.credit_id = credits.id
     WHERE credit_date BETWEEN ? AND ?
     AND vehicles.is_company = 0
     ${statusClause}
     ${searchClause}
     ORDER BY ${orderBy}`
  ).all(...creditsParams);

  const creditTotals = db.prepare(
    `SELECT
        COALESCE(SUM(credits.amount), 0) AS total_amount,
        COALESCE(SUM(credits.paid_amount), 0) AS total_paid,
        COALESCE(SUM(credits.amount - credits.paid_amount), 0) AS total_remaining,
        COALESCE(SUM(credits.credit_jars), 0) AS total_jars,
        COALESCE(SUM(credits.credit_bottle_cases), 0) AS total_bottle_cases,
        COALESCE(SUM(credits.credit_dispensers), 0) AS total_dispensers,
        COALESCE(SUM(credits.credit_jar_containers), 0) AS total_jar_containers
     FROM credits
     JOIN vehicles ON credits.vehicle_id = vehicles.id
     WHERE credit_date BETWEEN ? AND ?
     AND vehicles.is_company = 0
     ${statusClause}
     ${searchClause}`
  ).get(...creditsParams);

  const customerCumulativeSearchClause = q ? "AND (credits.customer_name LIKE ? OR vehicles.vehicle_number LIKE ?)" : "";
  const customerCumulativeParams = q
    ? [customerCreditFrom, customerCreditTo, `%${q}%`, `%${q}%`]
    : [customerCreditFrom, customerCreditTo];
  const customerCumulativeTotals = db.prepare(
    `SELECT credits.customer_name,
            COALESCE(SUM(credits.amount), 0) AS total_amount,
            COALESCE(SUM(credits.paid_amount), 0) AS total_paid,
            COALESCE(SUM(CASE WHEN credits.amount - credits.paid_amount < 0 THEN 0 ELSE credits.amount - credits.paid_amount END), 0) AS total_remaining,
            MAX(credits.credit_date) AS last_credit_date,
            COUNT(*) AS total_entries
     FROM credits
     JOIN vehicles ON credits.vehicle_id = vehicles.id
     WHERE vehicles.is_company = 0
     AND credits.credit_date BETWEEN ? AND ?
     ${statusClause}
     ${customerCumulativeSearchClause}
     GROUP BY credits.customer_name
     ORDER BY total_remaining DESC, credits.customer_name ASC`
  ).all(...customerCumulativeParams);
  const customerCumulativeSummary = customerCumulativeTotals.reduce(
    (acc, row) => {
      acc.customer_count += 1;
      acc.total_amount = parseMoneyValue(acc.total_amount + Number(row.total_amount || 0));
      acc.total_paid = parseMoneyValue(acc.total_paid + Number(row.total_paid || 0));
      acc.total_remaining = parseMoneyValue(acc.total_remaining + Number(row.total_remaining || 0));
      return acc;
    },
    { customer_count: 0, total_amount: 0, total_paid: 0, total_remaining: 0 }
  );
  const customerCumulativeAllTimeParams = q ? [`%${q}%`, `%${q}%`] : [];
  const customerCumulativeAllTimeRow = db.prepare(
    `SELECT COALESCE(SUM(CASE
      WHEN credits.amount - credits.paid_amount < 0 THEN 0
      ELSE credits.amount - credits.paid_amount
    END), 0) AS total_remaining
     FROM credits
     JOIN vehicles ON credits.vehicle_id = vehicles.id
     WHERE vehicles.is_company = 0
     ${statusClause}
     ${customerCumulativeSearchClause}`
  ).get(...customerCumulativeAllTimeParams);

  const today = dayjs().format("YYYY-MM-DD");
  const monthStart = dayjs().startOf("month").format("YYYY-MM-DD");
  const yearStart = dayjs().startOf("year").format("YYYY-MM-DD");

  res.render("records/credits", {
    title: req.t("creditsTitle"),
    from,
    to,
    status,
    q,
    sort,
    notice,
    error,
    creditsRows,
    customerCumulativeTotals,
    customerCumulativeSummary,
    customerCumulativeAllTime: Number(customerCumulativeAllTimeRow?.total_remaining || 0),
    customerCreditFrom,
    customerCreditTo,
    creditTotals,
    today,
    monthStart,
    yearStart
  });
});

router.post("/credits/pay/customer-total", (req, res) => {
  const customerName = String(req.body.customer_name || "").trim();
  const customerCreditFrom = String(req.body.customer_credit_from || req.body.from || "").trim();
  const customerCreditTo = String(req.body.customer_credit_to || req.body.to || "").trim();
  const hasCustomerRange = Boolean(customerCreditFrom && customerCreditTo);
  if (!customerName) {
    return res.redirect(buildCreditsListUrl({ ...req.body, error: "creditSettlementCustomerRequired" }));
  }
  const paymentParsed = parsePaymentBreakdownFromBody(req.body);
  if (paymentParsed.total <= 0) {
    return res.redirect(buildCreditsListUrl({ ...req.body, error: "creditSettlementInvalid" }));
  }

  const rangeClause = hasCustomerRange ? "AND credits.credit_date BETWEEN ? AND ?" : "";
  const rowParams = hasCustomerRange
    ? [customerName, customerCreditFrom, customerCreditTo]
    : [customerName];
  const rows = db.prepare(
    `SELECT credits.id, credits.amount, credits.paid_amount
     FROM credits
     JOIN vehicles ON credits.vehicle_id = vehicles.id
     WHERE lower(trim(credits.customer_name)) = lower(trim(?))
       AND vehicles.is_company = 0
       AND (credits.amount - credits.paid_amount) > 0
       ${rangeClause}
     ORDER BY credits.credit_date ASC, credits.id ASC`
  ).all(...rowParams);

  if (!rows.length) {
    return res.redirect(buildCreditsListUrl({ ...req.body, error: "creditSettlementNoOutstandingCustomer" }));
  }

  const result = applyCreditSettlementPayment({
    creditRows: rows,
    paymentAmount: paymentParsed.total,
    paymentBreakdown: paymentParsed.breakdown,
    note: `Customer settlement (${customerName})`,
    userId: req.session.userId,
    paymentMethod: paymentParsed.primaryMethod
  });
  if (result.applied <= 0) {
    return res.redirect(buildCreditsListUrl({ ...req.body, error: "creditSettlementInvalid" }));
  }

  logActivity({
    userId: req.session.userId,
    action: "payment",
    entityType: "credit_customer_settlement",
    entityId: customerName,
    details: `customer=${customerName}; payment=${result.applied}; method=${paymentParsed.splitEntered ? 'MIXED' : paymentParsed.primaryMethod}; cash=${paymentParsed.breakdown.cash || 0}; bank=${paymentParsed.breakdown.bank || 0}; ewallet=${paymentParsed.breakdown.eWallet || 0}; credits=${result.count}`
  });

  return res.redirect(buildCreditsListUrl({ ...req.body, notice: "creditSettlementCustomerSaved" }));
});

router.get("/credits/all", (req, res) => {
  const from = req.query.from || dayjs().subtract(30, "day").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const q = (req.query.q || "").trim();
  const { rows, totals } = getCombinedCredits({ from, to, q });

  res.render("records/credits_all", {
    title: req.t("allCreditsTitle"),
    from,
    to,
    q,
    rows,
    totals
  });
});

router.get("/credits/all/export", (req, res) => {
  const from = req.query.from || dayjs().subtract(30, "day").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const q = (req.query.q || "").trim();
  const { rows } = getCombinedCredits({ from, to, q });

  const header = ["Date (AD)", "Date (BS)", "Source", "Vehicle", "Owner", "Customer", "Credit Amount", "Paid Amount", "Remaining Amount"];
  const body = rows.map((row) => ([
    row.credit_date,
    adToBs(row.credit_date) || "",
    row.source,
    row.vehicle_number,
    row.owner_name || "",
    row.customer_name || "",
    row.credit_amount,
    row.paid_amount,
    row.remaining_amount
  ].map((value) => `"${String(value ?? "").replace(/"/g, '""')}"`).join(",")));

  res.setHeader("Content-Type", "text/csv");
  res.setHeader("Content-Disposition", `attachment; filename="aqua_msk_all_credits_${from}_to_${to}.csv"`);
  res.send([header.join(","), ...body].join("\n"));
});

router.get("/credits/all/print", (req, res) => {
  const from = req.query.from || dayjs().subtract(30, "day").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const q = (req.query.q || "").trim();
  const { rows, totals } = getCombinedCredits({ from, to, q });

  res.render("records/credits_all_print", {
    title: req.t("allCreditsTitle"),
    from,
    to,
    q,
    rows,
    totals,
    printedAt: dayjs().format("YYYY-MM-DD HH:mm")
  });
});

router.get("/credits/new", (req, res) => {
  const vehicles = getCreditVehicles();
  const customers = db.prepare("SELECT DISTINCT customer_name FROM credits ORDER BY customer_name").all().map((row) => row.customer_name);
  const staffOptions = getStaffOptions();
  const requestedExportId = parseOptionalId(req.query.export_id);
  const exportRow = requestedExportId
    ? db.prepare(
      `SELECT exports.id, exports.vehicle_id, exports.export_date
       FROM exports
       JOIN vehicles ON exports.vehicle_id = vehicles.id
       WHERE exports.id = ? AND vehicles.is_company = 0`
    ).get(requestedExportId)
    : null;
  const selectedVehicleIdRaw = Number(req.query.vehicle_id || (exportRow ? exportRow.vehicle_id : 0));
  const selectedVehicleId = Number.isNaN(selectedVehicleIdRaw) || selectedVehicleIdRaw <= 0 ? null : selectedVehicleIdRaw;
  const defaultDate = req.query.credit_date || (exportRow ? exportRow.export_date : dayjs().format("YYYY-MM-DD"));
  const defaultTripDate = req.query.trip_date || (exportRow ? exportRow.export_date : "");
  res.render("records/credit_form", {
    title: req.t("addCreditTitle"),
    record: null,
    vehicles,
    customers,
    error: null,
    defaultDate,
    selectedVehicleId,
    defaultTripDate,
    staffOptions,
    selectedStaffId: null,
    forceWashDefault: false
  });
});

router.post("/credits", (req, res) => {
  const {
    vehicle_id,
    export_id,
    customer_name,
    amount,
    credit_jars,
    credit_bottle_cases,
    credit_dispensers,
    credit_jar_containers,
    jar_price,
    bottle_case_price,
    dispenser_price,
    jar_container_price,
    credit_date,
    trip_date,
    checked_by_staff_id,
    force_wash_required,
    paid,
    paid_amount,
    payment_method,
    allow_duplicate_entry
  } = req.body;
  const vehicles = getCreditVehicles();
  const staffOptions = getStaffOptions();
  const selectedExportId = parseOptionalId(export_id);
  const tripDateValue = parseOptionalDate(trip_date);
  const selectedStaffId = parseOptionalId(checked_by_staff_id);
  const forceWashDefault = force_wash_required === "on";
  const allowDuplicateEntry = parseCheckbox(allow_duplicate_entry);
  if (!vehicle_id || !customer_name || !credit_date) {
    return res.render("records/credit_form", {
      title: req.t("addCreditTitle"),
      record: null,
      formValues: req.body,
      vehicles,
      staffOptions,
      customers: db.prepare("SELECT DISTINCT customer_name FROM credits ORDER BY customer_name").all().map((row) => row.customer_name),
      error: req.t("creditRequired"),
      defaultDate: credit_date || dayjs().format("YYYY-MM-DD"),
      selectedVehicleId: vehicle_id || null,
      defaultTripDate: tripDateValue || "",
      selectedStaffId,
      forceWashDefault
    });
  }
  const vehicleRow = db.prepare("SELECT id, is_company FROM vehicles WHERE id = ?").get(vehicle_id);
  if (!vehicleRow || Number(vehicleRow.is_company) === 1) {
    return res.render("records/credit_form", {
      title: req.t("addCreditTitle"),
      record: null,
      formValues: req.body,
      vehicles,
      staffOptions,
      customers: db.prepare("SELECT DISTINCT customer_name FROM credits ORDER BY customer_name").all().map((row) => row.customer_name),
      error: req.t("companyVehicleNoCredit"),
      defaultDate: credit_date || dayjs().format("YYYY-MM-DD"),
      selectedVehicleId: vehicle_id || null,
      defaultTripDate: tripDateValue || "",
      selectedStaffId,
      forceWashDefault
    });
  }
  let linkedExportId = null;
  if (selectedExportId) {
    const linkedExport = db.prepare("SELECT id, vehicle_id FROM exports WHERE id = ?").get(selectedExportId);
    if (!linkedExport || Number(linkedExport.vehicle_id) !== Number(vehicle_id)) {
      return res.render("records/credit_form", {
        title: req.t("addCreditTitle"),
        record: null,
        formValues: req.body,
        vehicles,
        staffOptions,
        customers: db.prepare("SELECT DISTINCT customer_name FROM credits ORDER BY customer_name").all().map((row) => row.customer_name),
        error: req.t("creditTripInvalid"),
        defaultDate: credit_date || dayjs().format("YYYY-MM-DD"),
        selectedVehicleId: vehicle_id || null,
        defaultTripDate: tripDateValue || "",
        selectedStaffId,
        forceWashDefault
      });
    }
    linkedExportId = linkedExport.id;
  }
  const staffRow = selectedStaffId
    ? db.prepare("SELECT id FROM staff WHERE id = ? AND COALESCE(is_active, 1) = 1").get(selectedStaffId)
    : null;
  const checkedByStaffId = staffRow ? staffRow.id : null;
  const forceWashRequired = forceWashDefault ? 1 : 0;

  const jarCreditRaw = Number(credit_jars || 0);
  const bottleCreditRaw = Number(credit_bottle_cases || 0);
  const dispenserCreditRaw = Number(credit_dispensers || 0);
  const containerCreditRaw = Number(credit_jar_containers || 0);
  const jarCreditCount = Number.isNaN(jarCreditRaw) || jarCreditRaw < 0 ? 0 : jarCreditRaw;
  const bottleCreditCount = Number.isNaN(bottleCreditRaw) || bottleCreditRaw < 0 ? 0 : bottleCreditRaw;
  const dispenserCreditCount = Number.isNaN(dispenserCreditRaw) || dispenserCreditRaw < 0 ? 0 : dispenserCreditRaw;
  const jarContainerCreditCount = Number.isNaN(containerCreditRaw) || containerCreditRaw < 0 ? 0 : containerCreditRaw;
  const jarPriceRaw = Number(jar_price || 0);
  const bottlePriceRaw = Number(bottle_case_price || 0);
  const dispenserPriceRaw = Number(dispenser_price || 0);
  const jarContainerPriceRaw = Number(jar_container_price || 0);
  const jarPrice = Number.isNaN(jarPriceRaw) || jarPriceRaw < 0 ? 0 : jarPriceRaw;
  const bottlePrice = Number.isNaN(bottlePriceRaw) || bottlePriceRaw < 0 ? 0 : bottlePriceRaw;
  const dispenserPrice = Number.isNaN(dispenserPriceRaw) || dispenserPriceRaw < 0 ? 0 : dispenserPriceRaw;
  const jarContainerPrice = Number.isNaN(jarContainerPriceRaw) || jarContainerPriceRaw < 0 ? 0 : jarContainerPriceRaw;
  const derivedAmount = (jarCreditCount * jarPrice)
    + (bottleCreditCount * bottlePrice)
    + (dispenserCreditCount * dispenserPrice)
    + (jarContainerCreditCount * jarContainerPrice);
  const amountNumRaw = Number(amount || 0);
  const fallbackAmount = Number.isNaN(amountNumRaw) || amountNumRaw < 0 ? 0 : amountNumRaw;
  const hasPriceInputs = Object.prototype.hasOwnProperty.call(req.body, "jar_price")
    || Object.prototype.hasOwnProperty.call(req.body, "bottle_case_price")
    || Object.prototype.hasOwnProperty.call(req.body, "dispenser_price")
    || Object.prototype.hasOwnProperty.call(req.body, "jar_container_price");
  const shouldUseDerived = hasPriceInputs
    && (derivedAmount > 0 || jarPrice > 0 || bottlePrice > 0 || dispenserPrice > 0 || jarContainerPrice > 0 || fallbackAmount === 0);
  const amountNum = shouldUseDerived ? derivedAmount : fallbackAmount;
  const paymentParsed = parsePaymentBreakdownFromBody(req.body, {
    cashField: "paid_cash_amount",
    bankField: "paid_bank_amount",
    ewalletField: "paid_ewallet_amount",
    amountField: "paid_amount",
    methodField: "payment_method",
    maxTotal: amountNum
  });
  let openingBreakdown = paymentParsed.breakdown;
  if (typeof paid !== "undefined") {
    const markMethod = normalizePaymentMethod(payment_method);
    openingBreakdown = {
      cash: markMethod === "CASH" ? amountNum : 0,
      bank: markMethod === "BANK" ? amountNum : 0,
      eWallet: markMethod === "E_WALLET" ? amountNum : 0
    };
  }
  const paidAmount = sumPaymentBreakdown(openingBreakdown);
  const paymentMethod = getPrimaryMethodFromBreakdown(openingBreakdown, normalizePaymentMethod(payment_method));
  const paidFlag = amountNum === 0 ? 1 : paidAmount >= amountNum ? 1 : 0;
  const duplicateCredits = findDuplicateCreditEntries({
    vehicleId: vehicle_id,
    creditDate: credit_date,
    customerName: customer_name,
    amount: amountNum,
    excludeId: null
  });
  if (duplicateCredits.length > 0 && !allowDuplicateEntry) {
    return res.render("records/credit_form", {
      title: req.t("addCreditTitle"),
      record: null,
      formValues: req.body,
      vehicles,
      staffOptions,
      customers: db.prepare("SELECT DISTINCT customer_name FROM credits ORDER BY customer_name").all().map((row) => row.customer_name),
      error: null,
      duplicateWarning: {
        type: "credit",
        rows: duplicateCredits,
        creditDate: credit_date,
        amount: amountNum
      },
      defaultDate: credit_date || dayjs().format("YYYY-MM-DD"),
      selectedVehicleId: vehicle_id || null,
      defaultTripDate: tripDateValue || "",
      selectedStaffId,
      forceWashDefault
    });
  }
  if (duplicateCredits.length > 0 && allowDuplicateEntry) {
    const duplicateIds = duplicateCredits.map((row) => row.id).join(",");
    logActivity({
      userId: req.session.userId,
      action: "duplicate_override",
      entityType: "credit",
      entityId: `vehicle:${vehicle_id}`,
      details: `duplicate_ids=${duplicateIds}, customer=${customer_name.trim()}, date=${credit_date}, amount=${amountNum}`
    });
  }

  const result = db.prepare(
    "INSERT INTO credits (vehicle_id, export_id, customer_name, amount, paid_amount, payment_method, credit_jars, credit_bottle_cases, credit_dispensers, credit_jar_containers, jar_price, bottle_case_price, dispenser_price, jar_container_price, credit_date, trip_date, checked_by_staff_id, force_wash_required, paid, created_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
  ).run(
    vehicle_id,
    linkedExportId,
    customer_name.trim(),
    amountNum,
    paidAmount,
    paymentMethod,
    jarCreditCount,
    bottleCreditCount,
    dispenserCreditCount,
    jarContainerCreditCount,
    jarPrice,
    bottlePrice,
    dispenserPrice,
    jarContainerPrice,
    credit_date,
    tripDateValue,
    checkedByStaffId,
    forceWashRequired,
    paidFlag,
    req.session.userId
  );
  const creditId = Number(result.lastInsertRowid);
  const creditReceiptNo = createReceiptNo(db, "CRD", credit_date || dayjs().format("YYYY-MM-DD"));
  db.prepare("UPDATE credits SET receipt_no = ? WHERE id = ?").run(creditReceiptNo, creditId);
  if (paidAmount > 0 && creditId) {
    paymentBreakdownOrder.forEach((key) => {
      const amountValue = parseMoneyValue(openingBreakdown[key] || 0);
      if (amountValue <= 0) return;
      insertCreditPayment({
        creditId,
        amount: amountValue,
        note: "Opening payment",
        paidAt: credit_date,
        userId: req.session.userId,
        paymentMethod: paymentMethodByBreakdownKey[key]
      });
    });
  }
  logActivity({
    userId: req.session.userId,
    action: "create",
    entityType: "credit",
    entityId: creditId,
    details: `receipt=${creditReceiptNo}, export_id=${linkedExportId || ""}, credit_date=${credit_date}, trip_date=${tripDateValue || ""}, method=${paymentParsed.splitEntered ? 'MIXED' : paymentMethod}, cash=${openingBreakdown.cash || 0}, bank=${openingBreakdown.bank || 0}, ewallet=${openingBreakdown.eWallet || 0}, checked_staff=${checkedByStaffId || ""}, force_wash=${forceWashRequired}, amount=${amountNum}, paid=${paidAmount}, jars=${jarCreditCount}@${jarPrice}, bottles=${bottleCreditCount}@${bottlePrice}, dispensers=${dispenserCreditCount}@${dispenserPrice}, containers=${jarContainerCreditCount}@${jarContainerPrice}, customer=${customer_name.trim()}`
  });

  res.redirect(`/records/credits?from=${credit_date}&to=${credit_date}`);
});

router.get("/credits/:id/edit", (req, res) => {
  const record = db.prepare("SELECT * FROM credits WHERE id = ?").get(req.params.id);
  if (!record) return res.redirect("/records/credits");
  const vehicles = getCreditVehicles();
  const staffOptions = getStaffOptions();
  const customers = db.prepare("SELECT DISTINCT customer_name FROM credits ORDER BY customer_name").all().map((row) => row.customer_name);
  res.render("records/credit_form", {
    title: req.t("editCreditTitle"),
    record,
    vehicles,
    staffOptions,
    customers,
    error: null,
    defaultDate: record.credit_date,
    defaultTripDate: record.trip_date || ""
  });
});

router.post("/credits/:id", (req, res) => {
  const {
    vehicle_id,
    export_id,
    customer_name,
    amount,
    credit_jars,
    credit_bottle_cases,
    credit_dispensers,
    credit_jar_containers,
    jar_price,
    bottle_case_price,
    dispenser_price,
    jar_container_price,
    credit_date,
    trip_date,
    checked_by_staff_id,
    force_wash_required,
    paid,
    paid_amount,
    payment_method
  } = req.body;
  const record = db.prepare("SELECT * FROM credits WHERE id = ?").get(req.params.id);
  const existingPaymentBreakdownRow = db.prepare(
    `SELECT
        COALESCE(SUM(CASE WHEN payment_method = 'CASH' THEN amount ELSE 0 END), 0) as cash_amount,
        COALESCE(SUM(CASE WHEN payment_method = 'BANK' THEN amount ELSE 0 END), 0) as bank_amount,
        COALESCE(SUM(CASE WHEN payment_method = 'E_WALLET' THEN amount ELSE 0 END), 0) as ewallet_amount
     FROM credit_payments
     WHERE credit_id = ?`
  ).get(req.params.id) || { cash_amount: 0, bank_amount: 0, ewallet_amount: 0 };
  const vehicles = getCreditVehicles();
  const staffOptions = getStaffOptions();
  const hasExportIdField = Object.prototype.hasOwnProperty.call(req.body, "export_id");
  const selectedExportId = hasExportIdField ? parseOptionalId(export_id) : parseOptionalId(record ? record.export_id : null);
  const tripDateValue = parseOptionalDate(trip_date);
  const selectedStaffId = parseOptionalId(checked_by_staff_id);
  const forceWashDefault = force_wash_required === "on";
  if (!record) return res.redirect("/records/credits");

  if (!vehicle_id || !customer_name || !credit_date) {
    return res.render("records/credit_form", {
      title: req.t("editCreditTitle"),
      record,
      vehicles,
      staffOptions,
      customers: db.prepare("SELECT DISTINCT customer_name FROM credits ORDER BY customer_name").all().map((row) => row.customer_name),
      error: req.t("creditRequired"),
      defaultDate: credit_date || record.credit_date,
      defaultTripDate: tripDateValue || (record.trip_date || ""),
      selectedStaffId,
      forceWashDefault
    });
  }
  const vehicleRow = db.prepare("SELECT id, is_company FROM vehicles WHERE id = ?").get(vehicle_id);
  if (!vehicleRow || Number(vehicleRow.is_company) === 1) {
    return res.render("records/credit_form", {
      title: req.t("editCreditTitle"),
      record,
      vehicles,
      staffOptions,
      customers: db.prepare("SELECT DISTINCT customer_name FROM credits ORDER BY customer_name").all().map((row) => row.customer_name),
      error: req.t("companyVehicleNoCredit"),
      defaultDate: credit_date || record.credit_date,
      defaultTripDate: tripDateValue || (record.trip_date || ""),
      selectedStaffId,
      forceWashDefault
    });
  }
  let linkedExportId = null;
  if (selectedExportId) {
    const linkedExport = db.prepare("SELECT id, vehicle_id FROM exports WHERE id = ?").get(selectedExportId);
    if (!linkedExport || Number(linkedExport.vehicle_id) !== Number(vehicle_id)) {
      return res.render("records/credit_form", {
        title: req.t("editCreditTitle"),
        record,
        vehicles,
        staffOptions,
        customers: db.prepare("SELECT DISTINCT customer_name FROM credits ORDER BY customer_name").all().map((row) => row.customer_name),
        error: req.t("creditTripInvalid"),
        defaultDate: credit_date || record.credit_date,
        defaultTripDate: tripDateValue || (record.trip_date || ""),
        selectedStaffId,
        forceWashDefault
      });
    }
    linkedExportId = linkedExport.id;
  }
  const staffRow = selectedStaffId
    ? db.prepare("SELECT id FROM staff WHERE id = ? AND COALESCE(is_active, 1) = 1").get(selectedStaffId)
    : null;
  const checkedByStaffId = staffRow ? staffRow.id : null;
  const forceWashRequired = forceWashDefault ? 1 : 0;

  const jarCreditRaw = Number(credit_jars || 0);
  const bottleCreditRaw = Number(credit_bottle_cases || 0);
  const dispenserCreditRaw = Number(credit_dispensers || 0);
  const containerCreditRaw = Number(credit_jar_containers || 0);
  const jarCreditCount = Number.isNaN(jarCreditRaw) || jarCreditRaw < 0 ? 0 : jarCreditRaw;
  const bottleCreditCount = Number.isNaN(bottleCreditRaw) || bottleCreditRaw < 0 ? 0 : bottleCreditRaw;
  const dispenserCreditCount = Number.isNaN(dispenserCreditRaw) || dispenserCreditRaw < 0 ? 0 : dispenserCreditRaw;
  const jarContainerCreditCount = Number.isNaN(containerCreditRaw) || containerCreditRaw < 0 ? 0 : containerCreditRaw;
  const jarPriceRaw = Number(jar_price || 0);
  const bottlePriceRaw = Number(bottle_case_price || 0);
  const dispenserPriceRaw = Number(dispenser_price || 0);
  const jarContainerPriceRaw = Number(jar_container_price || 0);
  const jarPrice = Number.isNaN(jarPriceRaw) || jarPriceRaw < 0 ? 0 : jarPriceRaw;
  const bottlePrice = Number.isNaN(bottlePriceRaw) || bottlePriceRaw < 0 ? 0 : bottlePriceRaw;
  const dispenserPrice = Number.isNaN(dispenserPriceRaw) || dispenserPriceRaw < 0 ? 0 : dispenserPriceRaw;
  const jarContainerPrice = Number.isNaN(jarContainerPriceRaw) || jarContainerPriceRaw < 0 ? 0 : jarContainerPriceRaw;
  const derivedAmount = (jarCreditCount * jarPrice)
    + (bottleCreditCount * bottlePrice)
    + (dispenserCreditCount * dispenserPrice)
    + (jarContainerCreditCount * jarContainerPrice);
  const amountNumRaw = Number(amount || 0);
  const fallbackAmount = Number.isNaN(amountNumRaw) || amountNumRaw < 0 ? 0 : amountNumRaw;
  const hasPriceInputs = Object.prototype.hasOwnProperty.call(req.body, "jar_price")
    || Object.prototype.hasOwnProperty.call(req.body, "bottle_case_price")
    || Object.prototype.hasOwnProperty.call(req.body, "dispenser_price")
    || Object.prototype.hasOwnProperty.call(req.body, "jar_container_price");
  const shouldUseDerived = hasPriceInputs
    && (derivedAmount > 0 || jarPrice > 0 || bottlePrice > 0 || dispenserPrice > 0 || jarContainerPrice > 0 || fallbackAmount === 0);
  const amountNum = shouldUseDerived ? derivedAmount : fallbackAmount;
  const paymentParsed = parsePaymentBreakdownFromBody(req.body, {
    cashField: "paid_cash_amount",
    bankField: "paid_bank_amount",
    ewalletField: "paid_ewallet_amount",
    amountField: "paid_amount",
    methodField: "payment_method",
    maxTotal: amountNum
  });
  let openingBreakdown = paymentParsed.breakdown;
  if (typeof paid !== "undefined") {
    const markMethod = normalizePaymentMethod(payment_method);
    openingBreakdown = {
      cash: markMethod === "CASH" ? amountNum : 0,
      bank: markMethod === "BANK" ? amountNum : 0,
      eWallet: markMethod === "E_WALLET" ? amountNum : 0
    };
  }
  const paidAmount = sumPaymentBreakdown(openingBreakdown);
  const paymentMethod = getPrimaryMethodFromBreakdown(openingBreakdown, normalizePaymentMethod(payment_method));
  const paidFlag = amountNum === 0 ? 1 : paidAmount >= amountNum ? 1 : 0;

  db.prepare(
    "UPDATE credits SET vehicle_id = ?, export_id = ?, customer_name = ?, amount = ?, paid_amount = ?, payment_method = ?, credit_jars = ?, credit_bottle_cases = ?, credit_dispensers = ?, credit_jar_containers = ?, jar_price = ?, bottle_case_price = ?, dispenser_price = ?, jar_container_price = ?, credit_date = ?, trip_date = ?, checked_by_staff_id = ?, force_wash_required = ?, paid = ? WHERE id = ?"
  ).run(
    vehicle_id,
    linkedExportId,
    customer_name.trim(),
    amountNum,
    paidAmount,
    paymentMethod,
    jarCreditCount,
    bottleCreditCount,
    dispenserCreditCount,
    jarContainerCreditCount,
    jarPrice,
    bottlePrice,
    dispenserPrice,
    jarContainerPrice,
    credit_date,
    tripDateValue,
    checkedByStaffId,
    forceWashRequired,
    paidFlag,
    req.params.id
  );
  const desiredBreakdown = {
    cash: parseMoneyValue(openingBreakdown.cash || 0),
    bank: parseMoneyValue(openingBreakdown.bank || 0),
    eWallet: parseMoneyValue(openingBreakdown.eWallet || 0)
  };
  const existingBreakdown = {
    cash: parseMoneyValue(existingPaymentBreakdownRow.cash_amount || 0),
    bank: parseMoneyValue(existingPaymentBreakdownRow.bank_amount || 0),
    eWallet: parseMoneyValue(existingPaymentBreakdownRow.ewallet_amount || 0)
  };
  let insertedDeltaTotal = 0;
  paymentBreakdownOrder.forEach((key) => {
    const desired = parseMoneyValue(desiredBreakdown[key] || 0);
    const existing = parseMoneyValue(existingBreakdown[key] || 0);
    const delta = roundMoneySigned(desired - existing);
    if (Math.abs(delta) < 0.01) return;
    insertedDeltaTotal = roundMoneySigned(insertedDeltaTotal + delta);
    insertCreditPayment({
      creditId: req.params.id,
      amount: delta,
      note: "Adjustment",
      userId: req.session.userId,
      paymentMethod: paymentMethodByBreakdownKey[key]
    });
  });
  const existingTotal = sumPaymentBreakdown(existingBreakdown);
  const desiredTotal = sumPaymentBreakdown(desiredBreakdown);
  const residue = roundMoneySigned(desiredTotal - roundMoneySigned(existingTotal + insertedDeltaTotal));
  if (Math.abs(residue) >= 0.01) {
    insertCreditPayment({
      creditId: req.params.id,
      amount: residue,
      note: "Adjustment",
      userId: req.session.userId,
      paymentMethod
    });
  }
  logActivity({
    userId: req.session.userId,
    action: "update",
    entityType: "credit",
    entityId: req.params.id,
    details: buildDiffDetails(
      record,
      {
        vehicle_id: Number(vehicle_id),
        export_id: linkedExportId,
        customer_name: customer_name.trim(),
        amount: amountNum,
        paid_amount: paidAmount,
        payment_method: paymentMethod,
        credit_jars: jarCreditCount,
        credit_bottle_cases: bottleCreditCount,
        credit_dispensers: dispenserCreditCount,
        credit_jar_containers: jarContainerCreditCount,
        jar_price: jarPrice,
        bottle_case_price: bottlePrice,
        dispenser_price: dispenserPrice,
        jar_container_price: jarContainerPrice,
        credit_date,
        trip_date: tripDateValue,
        checked_by_staff_id: checkedByStaffId,
        force_wash_required: forceWashRequired,
        paid: paidFlag
      },
      [
        "vehicle_id",
        "export_id",
        "customer_name",
        "amount",
        "paid_amount",
        "payment_method",
        "credit_jars",
        "credit_bottle_cases",
        "credit_dispensers",
        "credit_jar_containers",
        "jar_price",
        "bottle_case_price",
        "dispenser_price",
        "jar_container_price",
        "credit_date",
        "trip_date",
        "checked_by_staff_id",
        "force_wash_required",
        "paid"
      ]
    )
  });

  res.redirect(`/records/credits?from=${credit_date}&to=${credit_date}`);
});

router.post("/credits/:id/paid", (req, res) => {
  const record = db.prepare("SELECT id, paid, amount, paid_amount, payment_method FROM credits WHERE id = ?").get(req.params.id);
  if (!record) return res.redirect("/records/credits");

  const newPaid = typeof req.body.paid !== "undefined" ? Number(req.body.paid) : record.paid ? 0 : 1;
  const desiredPaid = newPaid ? Number(record.amount || 0) : 0;
  const delta = desiredPaid - Number(record.paid_amount || 0);
  if (delta !== 0) {
    insertCreditPayment({
      creditId: req.params.id,
      amount: delta,
      note: newPaid ? "Marked paid" : "Marked unpaid",
      userId: req.session.userId,
      paymentMethod: record.payment_method || "CASH"
    });
  }
  db.prepare("UPDATE credits SET paid = ?, paid_amount = ? WHERE id = ?").run(newPaid, desiredPaid, req.params.id);
  logActivity({
    userId: req.session.userId,
    action: "update",
    entityType: "credit",
    entityId: req.params.id,
    details: buildDiffDetails(
      record,
      { paid: newPaid, paid_amount: desiredPaid },
      ["paid", "paid_amount"]
    )
  });

  res.redirect(req.get("Referrer") || "/records/credits");
});

router.post("/credits/:id/pay", (req, res) => {
  const record = db.prepare("SELECT id, amount, paid_amount, payment_method FROM credits WHERE id = ?").get(req.params.id);
  if (!record) return res.redirect("/records/credits");
  const paymentParsed = parsePaymentBreakdownFromBody(req.body);
  if (paymentParsed.total <= 0) {
    return res.redirect(req.get("Referrer") || "/records/credits");
  }
  const remaining = Math.max(0, Number(record.amount || 0) - Number(record.paid_amount || 0));
  const cappedBreakdown = clampBreakdownToTotal(paymentParsed.breakdown, remaining);
  const appliedPayment = sumPaymentBreakdown(cappedBreakdown);
  if (appliedPayment <= 0) {
    return res.redirect(req.get("Referrer") || "/records/credits");
  }
  let newPaidAmount = Number(record.paid_amount || 0) + appliedPayment;
  if (newPaidAmount > Number(record.amount || 0)) newPaidAmount = Number(record.amount || 0);
  const paidFlag = Number(record.amount || 0) === 0 ? 1 : newPaidAmount >= Number(record.amount || 0) ? 1 : 0;
  paymentBreakdownOrder.forEach((key) => {
    const amount = parseMoneyValue(cappedBreakdown[key] || 0);
    if (amount <= 0) return;
    insertCreditPayment({
      creditId: req.params.id,
      amount,
      note: "Payment",
      userId: req.session.userId,
      paymentMethod: paymentMethodByBreakdownKey[key]
    });
  });
  const nextMethod = Number(record.paid_amount || 0) > 0
    ? record.payment_method
    : getPrimaryMethodFromBreakdown(cappedBreakdown, paymentParsed.primaryMethod);
  db.prepare("UPDATE credits SET paid_amount = ?, paid = ?, payment_method = ? WHERE id = ?").run(newPaidAmount, paidFlag, nextMethod, req.params.id);
  logActivity({
    userId: req.session.userId,
    action: "payment",
    entityType: "credit",
    entityId: req.params.id,
    details: `payment=${appliedPayment}; method=${paymentParsed.splitEntered ? 'MIXED' : paymentParsed.primaryMethod}; cash=${cappedBreakdown.cash || 0}; bank=${cappedBreakdown.bank || 0}; ewallet=${cappedBreakdown.eWallet || 0}; paid_amount: ${formatDiffValue(record.paid_amount)} -> ${formatDiffValue(newPaidAmount)}`
  });
  res.redirect(req.get("Referrer") || "/records/credits");
});

router.post("/credits/:id/delete", (req, res) => {
  const credit = db.prepare("SELECT * FROM credits WHERE id = ?").get(req.params.id);
  if (!credit) return res.redirect("/records/credits");
  const payments = db.prepare("SELECT * FROM credit_payments WHERE credit_id = ?").all(req.params.id);
  const recycleId = createRecycleEntry({
    entityType: "credit",
    entityId: req.params.id,
    payload: { credit, payments },
    deletedBy: req.session.userId,
    note: `date=${credit.credit_date || ""}; customer=${credit.customer_name || ""}`
  });
  logActivity({
    userId: req.session.userId,
    action: "delete",
    entityType: "credit",
    entityId: req.params.id,
    details: `recycle_id=${recycleId}`
  });
  db.prepare("DELETE FROM credit_payments WHERE credit_id = ?").run(req.params.id);
  db.prepare("DELETE FROM credits WHERE id = ?").run(req.params.id);
  res.redirect("/records/credits");
});

router.get("/credits/:id/print", (req, res) => {
  const record = db.prepare(
    `SELECT credits.*, vehicles.vehicle_number, vehicles.owner_name, vehicles.phone,
            credit_export.receipt_no as trip_receipt_no,
            COALESCE(credits.trip_date, credit_export.export_date) as trip_date,
            checked_staff.full_name as checked_by_staff_name
     FROM credits
     JOIN vehicles ON credits.vehicle_id = vehicles.id
     LEFT JOIN exports as credit_export ON credits.export_id = credit_export.id
     LEFT JOIN staff as checked_staff ON credits.checked_by_staff_id = checked_staff.id
     WHERE credits.id = ?
       AND vehicles.is_company = 0`
  ).get(req.params.id);
  if (!record) return res.redirect("/records/credits");
  res.render("records/credit_print", { title: req.t("creditsTitle"), record });
});

router.get("/credits/:id/payments", (req, res) => {
  const record = db.prepare(
    `SELECT credits.*, vehicles.vehicle_number, vehicles.owner_name, vehicles.phone,
            credit_export.receipt_no as trip_receipt_no,
            COALESCE(credits.trip_date, credit_export.export_date) as trip_date,
            checked_staff.full_name as checked_by_staff_name
     FROM credits
     JOIN vehicles ON credits.vehicle_id = vehicles.id
     LEFT JOIN exports as credit_export ON credits.export_id = credit_export.id
     LEFT JOIN staff as checked_staff ON credits.checked_by_staff_id = checked_staff.id
     WHERE credits.id = ?
       AND vehicles.is_company = 0`
  ).get(req.params.id);
  if (!record) return res.redirect("/records/credits");

  const payments = db.prepare(
    `SELECT credit_payments.*, users.full_name
     FROM credit_payments
     LEFT JOIN users ON credit_payments.created_by = users.id
     WHERE credit_payments.credit_id = ?
     ORDER BY credit_payments.paid_at DESC, credit_payments.id DESC`
  ).all(req.params.id);

  const paymentTotals = db.prepare(
    "SELECT COALESCE(SUM(amount), 0) as total_paid FROM credit_payments WHERE credit_id = ?"
  ).get(req.params.id);

  res.render("records/credit_payments", {
    title: req.t("paymentHistoryTitle"),
    record,
    payments,
    paymentTotals
  });
});

router.get("/credits/export", (req, res) => {
  const from = req.query.from || dayjs().subtract(7, "day").format("YYYY-MM-DD");
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const statusRaw = req.query.status || "all";
  const status = ["all", "paid", "unpaid", "partial"].includes(statusRaw) ? statusRaw : "all";
  let statusClause = "";
  if (status === "paid") statusClause = "AND credits.paid_amount >= credits.amount";
  if (status === "unpaid") statusClause = "AND credits.paid_amount <= 0";
  if (status === "partial") statusClause = "AND credits.paid_amount > 0 AND credits.paid_amount < credits.amount";
  const q = (req.query.q || "").trim();
  const sortRaw = req.query.sort || "date_desc";
  const sortMap = {
    date_desc: "credit_date DESC, credits.created_at DESC",
    date_asc: "credit_date ASC, credits.created_at ASC",
    amount_desc: "credits.amount DESC",
    amount_asc: "credits.amount ASC",
    jars_desc: "credits.credit_jars DESC",
    paid_first: "credits.paid DESC, credits.created_at DESC",
    unpaid_first: "credits.paid ASC, credits.created_at DESC",
    paid_amount_desc: "credits.paid_amount DESC",
    remaining_desc: "(credits.amount - credits.paid_amount) DESC"
  };
  const sort = sortMap[sortRaw] ? sortRaw : "date_desc";
  const orderBy = sortMap[sort];
  const searchClause = q ? "AND (vehicles.vehicle_number LIKE ? OR credits.customer_name LIKE ?)" : "";
  const params = q ? [from, to, `%${q}%`, `%${q}%`] : [from, to];

  const creditsRows = db.prepare(
    `SELECT credits.credit_date, credits.receipt_no, vehicles.vehicle_number, vehicles.owner_name, credits.customer_name,
            credit_export.receipt_no as trip_receipt_no,
            COALESCE(credits.trip_date, credit_export.export_date) as trip_date,
            credits.amount, credits.paid_amount,
            CASE WHEN credits.amount - credits.paid_amount < 0 THEN 0 ELSE credits.amount - credits.paid_amount END AS remaining_amount,
            credits.credit_jars, credits.credit_bottle_cases, credits.credit_dispensers, credits.credit_jar_containers,
            credits.jar_price, credits.bottle_case_price, credits.dispenser_price, credits.jar_container_price
     FROM credits
     JOIN vehicles ON credits.vehicle_id = vehicles.id
     LEFT JOIN exports as credit_export ON credits.export_id = credit_export.id
     WHERE credit_date BETWEEN ? AND ?
     AND vehicles.is_company = 0
     ${statusClause}
     ${searchClause}
     ORDER BY ${orderBy}`
  ).all(...params);

  const header = "Date (AD),Date (BS),Receipt No,Trip Receipt,Trip Date (AD),Trip Date (BS),Vehicle Number,Owner Name,Customer,Amount,Paid Amount,Remaining Amount,Credit Jars,Bottle Cases,Credit Dispensers,Credit Jar Containers,Jar Price,Bottle Case Price,Dispenser Price,Jar Container Price,Status";
  const lines = creditsRows.map((row) => {
    const statusLabel = row.paid_amount >= row.amount ? "Paid" : row.paid_amount > 0 ? "Partial" : "Unpaid";
    const bsCreditDate = adToBs(row.credit_date) || "";
    const tripDate = row.trip_date || "";
    const bsTripDate = tripDate ? (adToBs(tripDate) || "") : "";
    const safe = [
      row.credit_date,
      bsCreditDate,
      row.receipt_no || "",
      row.trip_receipt_no || "",
      tripDate,
      bsTripDate,
      row.vehicle_number,
      row.owner_name,
      row.customer_name,
      row.amount,
      row.paid_amount,
      row.remaining_amount,
      row.credit_jars,
      row.credit_bottle_cases,
      row.credit_dispensers || 0,
      row.credit_jar_containers || 0,
      row.jar_price || 0,
      row.bottle_case_price || 0,
      row.dispenser_price || 0,
      row.jar_container_price || 0,
      statusLabel
    ].map((val) => {
      const str = String(val ?? "").replace(/\"/g, "\"\"");
      return `"${str}"`;
    });
    return safe.join(",");
  });

  const csv = [header, ...lines].join("\n");
  res.setHeader("Content-Type", "text/csv");
  res.setHeader("Content-Disposition", `attachment; filename="credits_${from}_to_${to}.csv"`);
  res.send(csv);
});

module.exports = router;
