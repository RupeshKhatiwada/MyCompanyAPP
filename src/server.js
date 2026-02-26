const path = require("path");
const fs = require("fs");
const express = require("express");
const dayjs = require("dayjs");
const crypto = require("crypto");
const session = require("express-session");
const FileStore = require("session-file-store")(session);
const { db } = require("./db");
const { attachUser, requireAuth } = require("./middleware/auth");
const { t } = require("./i18n");
const { createBackupFile, pruneOldBackups } = require("./utils/backup");
const { syncLocalToPostgres, shouldAutoSync } = require("./utils/hybridSync");
const authRoutes = require("./routes/auth");
const adminRoutes = require("./routes/admin");
const recordsRoutes = require("./routes/records");

const app = express();
const PORT = process.env.PORT || 3000;

fs.mkdirSync(path.join(__dirname, "..", "public", "uploads"), { recursive: true });
fs.mkdirSync(path.join(__dirname, "..", "data"), { recursive: true });
const sessionsDir = path.join(__dirname, "..", "data", "sessions");
fs.mkdirSync(sessionsDir, { recursive: true });

app.set("view engine", "ejs");
app.set("views", path.join(__dirname, "..", "views"));

const isProd = process.env.NODE_ENV === "production";

const readCookie = (req, name) => {
  const cookie = req.headers.cookie;
  if (!cookie) return null;
  const parts = cookie.split(";").map((part) => part.trim());
  const match = parts.find((part) => part.startsWith(`${name}=`));
  if (!match) return null;
  return decodeURIComponent(match.split("=").slice(1).join("="));
};

const getSetting = (key, fallback = "") => {
  const row = db.prepare("SELECT value FROM settings WHERE key = ?").get(key);
  return row ? row.value : fallback;
};

const setSetting = (key, value) => {
  const exists = db.prepare("SELECT key FROM settings WHERE key = ?").get(key);
  if (exists) {
    db.prepare("UPDATE settings SET value = ? WHERE key = ?").run(String(value), key);
  } else {
    db.prepare("INSERT INTO settings (key, value) VALUES (?, ?)").run(key, String(value));
  }
};

const getWorkerAlertSummary = () => {
  const itemLowThresholdRaw = Number(getSetting("alert_low_stock_items", 10));
  const itemLowThreshold = Number.isNaN(itemLowThresholdRaw) ? 10 : Math.max(0, Math.floor(itemLowThresholdRaw));

  const pendingCustomerCredits = db.prepare(
    `SELECT COUNT(*) as count,
            COALESCE(SUM(CASE WHEN amount - paid_amount < 0 THEN 0 ELSE amount - paid_amount END), 0) as remaining
     FROM credits
     WHERE amount - paid_amount > 0`
  ).get();

  const pendingVehicleCredits = db.prepare(
    `SELECT COUNT(*) as count,
            COALESCE(SUM(CASE WHEN exports.credit_amount - exports.paid_amount < 0 THEN 0 ELSE exports.credit_amount - exports.paid_amount END), 0) as remaining
     FROM exports
     JOIN vehicles ON vehicles.id = exports.vehicle_id
     WHERE vehicles.is_company = 0
       AND exports.credit_amount - exports.paid_amount > 0`
  ).get();

  const lowStock = db.prepare(
    `SELECT COUNT(*) as count
     FROM (
       SELECT import_entries.item_type
       FROM import_entries
       GROUP BY import_entries.item_type
       HAVING COALESCE(SUM(CASE WHEN import_entries.direction = 'OUT' THEN -import_entries.quantity ELSE import_entries.quantity END), 0) <= ?
     ) low_stock_rows`
  ).get(itemLowThreshold);

  return {
    lowStockCount: Number(lowStock.count || 0),
    pendingCustomerCount: Number(pendingCustomerCredits.count || 0),
    pendingCustomerAmount: Number(pendingCustomerCredits.remaining || 0),
    pendingVehicleCount: Number(pendingVehicleCredits.count || 0),
    pendingVehicleAmount: Number(pendingVehicleCredits.remaining || 0),
    totalAlerts:
      Number(lowStock.count || 0) +
      Number(pendingCustomerCredits.count || 0) +
      Number(pendingVehicleCredits.count || 0)
  };
};

app.use(express.json({ limit: "1mb" }));
app.use(express.urlencoded({ extended: true }));
app.use(
  express.static(path.join(__dirname, "..", "public"), {
    maxAge: isProd ? "7d" : 0,
    etag: true
  })
);
app.use("/vendor", express.static(path.join(__dirname, "..", "node_modules", "chart.js", "dist")));

app.use((req, res, next) => {
  if ((req.headers.accept || "").includes("text/html")) {
    res.setHeader("Cache-Control", "no-store");
    res.setHeader("Vary", "Cookie");
  }
  next();
});

app.use(
  session({
    store: new FileStore({
      path: sessionsDir,
      retries: 0,
      reapInterval: 3600,
      logFn: () => {}
    }),
    secret: process.env.SESSION_SECRET || require("crypto").randomBytes(32).toString("hex"),
    resave: false,
    saveUninitialized: false,
    cookie: { maxAge: 1000 * 60 * 60 * 8 }
  })
);

app.use((req, res, next) => {
  try {
    fs.mkdirSync(sessionsDir, { recursive: true });
  } catch (err) {
    // ignore transient session directory errors
  }
  next();
});

app.use((req, res, next) => {
  const cookieLang = readCookie(req, "lang");
  const lang = cookieLang === "ne" ? "ne" : "en";
  res.locals.lang = lang;
  res.locals.t = (key, vars) => t(lang, key, vars);
  req.t = res.locals.t;
  try {
    const row = db.prepare("SELECT value FROM settings WHERE key = 'logo_path'").get();
    res.locals.logoPath = row ? row.value : "";
  } catch (err) {
    res.locals.logoPath = "";
  }
  try {
    const row = db.prepare("SELECT value FROM settings WHERE key = 'brand_wordmark_path'").get();
    res.locals.brandWordmarkPath = row ? row.value : "";
  } catch (err) {
    res.locals.brandWordmarkPath = "";
  }
  if (!res.locals.assetVersion) {
    try {
      const cssPath = path.join(__dirname, "..", "public", "css", "styles.css");
      const stat = fs.statSync(cssPath);
      res.locals.assetVersion = String(stat.mtimeMs);
    } catch (err) {
      res.locals.assetVersion = String(Date.now());
    }
  }
  next();
});

app.use((req, res, next) => {
  const count = db.prepare("SELECT COUNT(*) as count FROM users").get().count;
  if (count === 0 && !req.path.startsWith("/setup") && !req.path.startsWith("/public")) {
    return res.redirect("/setup");
  }
  next();
});

app.use(attachUser);
app.use((req, res, next) => {
  res.locals.currentPath = req.path;
  next();
});

app.use(authRoutes);

const normalizeFingerprintId = (value) => {
  const safe = String(value || "").trim();
  return safe || null;
};

const isIotTokenValid = (provided, expected) => {
  const safeProvided = String(provided || "");
  const safeExpected = String(expected || "");
  if (!safeProvided || !safeExpected) return false;
  const a = Buffer.from(safeProvided);
  const b = Buffer.from(safeExpected);
  if (a.length !== b.length) return false;
  try {
    return crypto.timingSafeEqual(a, b);
  } catch (err) {
    return false;
  }
};

app.post("/iot/attendance/push", (req, res) => {
  const enabled = String(getSetting("iot_attendance_enabled", "0")) === "1";
  if (!enabled) {
    return res.status(403).json({ ok: false, error: "iot_attendance_disabled" });
  }
  const tokenRow = db.prepare("SELECT value FROM settings WHERE key = ?").get("iot_attendance_token");
  const expectedToken = tokenRow ? String(tokenRow.value || "") : "";
  const providedToken = req.headers["x-iot-token"] || req.body.token;
  if (!isIotTokenValid(providedToken, expectedToken)) {
    return res.status(401).json({ ok: false, error: "invalid_token" });
  }

  const fingerprintId = normalizeFingerprintId(req.body.fingerprint_id || req.body.fingerprintId);
  if (!fingerprintId) {
    return res.status(400).json({ ok: false, error: "fingerprint_id_required" });
  }
  const requestedType = String(req.body.person_type || "AUTO").trim().toUpperCase();
  const status = String(req.body.status || "").trim().toUpperCase() === "ABSENT" ? "ABSENT" : "PRESENT";
  const scannedInput = String(req.body.scanned_at || "").trim();
  const scannedMoment = scannedInput && dayjs(scannedInput).isValid() ? dayjs(scannedInput) : dayjs();
  const attendanceDateInput = String(req.body.attendance_date || "").trim();
  const attendanceDate = /^\d{4}-\d{2}-\d{2}$/.test(attendanceDateInput)
    ? attendanceDateInput
    : scannedMoment.format("YYYY-MM-DD");
  const note = String(req.body.note || "").trim();

  const staff = db.prepare(
    "SELECT id, full_name FROM staff WHERE lower(trim(fingerprint_id)) = lower(trim(?))"
  ).get(fingerprintId);
  const worker = db.prepare(
    "SELECT id, full_name FROM users WHERE role = 'WORKER' AND is_active = 1 AND lower(trim(fingerprint_id)) = lower(trim(?))"
  ).get(fingerprintId);

  let personType = null;
  let person = null;
  if (requestedType === "STAFF") {
    personType = "STAFF";
    person = staff;
  } else if (requestedType === "WORKER") {
    personType = "WORKER";
    person = worker;
  } else {
    const matches = [staff ? { type: "STAFF", row: staff } : null, worker ? { type: "WORKER", row: worker } : null]
      .filter(Boolean);
    if (matches.length === 1) {
      personType = matches[0].type;
      person = matches[0].row;
    } else if (matches.length > 1) {
      return res.status(409).json({ ok: false, error: "ambiguous_fingerprint_mapping" });
    }
  }

  if (!person || !personType) {
    return res.status(404).json({ ok: false, error: "fingerprint_not_mapped" });
  }

  if (personType === "STAFF") {
    db.prepare(
      `INSERT INTO staff_attendance (staff_id, attendance_date, status, recorded_by, created_at, updated_at)
       VALUES (?, ?, ?, NULL, datetime('now'), datetime('now'))
       ON CONFLICT(staff_id, attendance_date)
       DO UPDATE SET status = excluded.status, recorded_by = NULL, updated_at = datetime('now')`
    ).run(person.id, attendanceDate, status);
  } else {
    db.prepare(
      `INSERT INTO user_attendance (user_id, attendance_date, status, recorded_by, created_at, updated_at)
       VALUES (?, ?, ?, NULL, datetime('now'), datetime('now'))
       ON CONFLICT(user_id, attendance_date)
       DO UPDATE SET status = excluded.status, recorded_by = NULL, updated_at = datetime('now')`
    ).run(person.id, attendanceDate, status);
  }

  db.prepare(
    `INSERT INTO iot_attendance_logs (source, person_type, person_id, fingerprint_id, status, attendance_date, scanned_at, note, recorded_by)
     VALUES ('API', ?, ?, ?, ?, ?, ?, ?, NULL)`
  ).run(
    personType,
    person.id,
    fingerprintId,
    status,
    attendanceDate,
    scannedMoment.format("YYYY-MM-DD HH:mm:ss"),
    note || null
  );

  db.prepare(
    "INSERT INTO activity_logs (user_id, action, entity_type, entity_id, details) VALUES (NULL, ?, ?, ?, ?)"
  ).run(
    "iot_scan",
    personType === "STAFF" ? "staff_attendance_iot" : "worker_attendance_iot",
    attendanceDate,
    `person_id=${person.id}; fingerprint=${fingerprintId}; status=${status}; source=api`
  );

  return res.json({
    ok: true,
    person_type: personType,
    person_id: person.id,
    person_name: person.full_name,
    attendance_date: attendanceDate,
    status
  });
});

app.get("/lang/:lang", (req, res) => {
  const lang = req.params.lang === "ne" ? "ne" : "en";
  res.setHeader("Cache-Control", "no-store");
  res.setHeader("Vary", "Cookie");
  res.setHeader("Set-Cookie", `lang=${encodeURIComponent(lang)}; Path=/; SameSite=Lax; Max-Age=31536000`);
  res.redirect(req.get("Referrer") || "/");
});

app.get("/", requireAuth, (req, res) => {
  const user = res.locals.currentUser;
  if (!user) return res.redirect("/login");
  if (user.role === "WORKER") return res.redirect("/worker");
  return res.redirect("/admin");
});

app.get("/worker", requireAuth, (req, res) => {
  const user = res.locals.currentUser;
  if (!user) return res.redirect("/login");
  const today = dayjs().format("YYYY-MM-DD");
  const myExports = db.prepare(
    "SELECT COUNT(*) as count, COALESCE(SUM(total_amount), 0) as total FROM exports WHERE export_date = ? AND created_by = ?"
  ).get(today, user.id);
  const myCredits = db.prepare(
    "SELECT COUNT(*) as count, COALESCE(SUM(amount), 0) as total, COALESCE(SUM(amount - paid_amount), 0) as remaining FROM credits WHERE credit_date = ? AND created_by = ?"
  ).get(today, user.id);
  const myJarSales = db.prepare(
    "SELECT COUNT(*) as count, COALESCE(SUM(total_amount), 0) as total FROM jar_sales WHERE sale_date = ? AND created_by = ?"
  ).get(today, user.id);
  const myVehicleExpenses = db.prepare(
    "SELECT COUNT(*) as count, COALESCE(SUM(amount), 0) as total FROM vehicle_expenses WHERE expense_date = ? AND created_by = ?"
  ).get(today, user.id);
  const workerAlerts = getWorkerAlertSummary();
  const jarTypes = db.prepare("SELECT name, default_qty FROM jar_types WHERE active = 1 ORDER BY name").all();
  res.render("worker/dashboard", {
    title: req.t("workerDashboardTitle"),
    jarTypes,
    today,
    myExports,
    myCredits,
    myJarSales,
    myVehicleExpenses,
    workerAlerts
  });
});

app.get("/search", requireAuth, (req, res) => {
  const q = String(req.query.q || "").trim().slice(0, 120);
  const today = dayjs().format("YYYY-MM-DD");
  const basePayload = {
    title: req.t("searchTitle"),
    q,
    today,
    vehicles: [],
    customers: [],
    people: [],
    technicians: [],
    sellers: [],
    exportRows: [],
    creditRows: [],
    jarSaleRows: [],
    importRows: [],
    purchaseRows: [],
    expenseRows: [],
    totalMatches: 0
  };

  if (!q) {
    return res.render("search", basePayload);
  }
  const like = `%${q.replace(/\s+/g, "%")}%`;

  const vehicles = db.prepare(
    `SELECT id, vehicle_number, owner_name, phone, is_company
     FROM vehicles
     WHERE vehicle_number LIKE ? OR owner_name LIKE ? OR COALESCE(phone, '') LIKE ?
     ORDER BY vehicle_number
     LIMIT 20`
  ).all(like, like, like);

  const customers = db.prepare(
    `SELECT customer_name,
            COUNT(*) as credit_rows,
            COALESCE(SUM(CASE WHEN amount - paid_amount < 0 THEN 0 ELSE amount - paid_amount END), 0) as remaining
     FROM credits
     WHERE customer_name LIKE ?
     GROUP BY customer_name
     ORDER BY remaining DESC, customer_name ASC
     LIMIT 20`
  ).all(like);

  const staffRows = db.prepare(
    `SELECT id, full_name, phone, 'STAFF' as role
     FROM staff
     WHERE full_name LIKE ? OR COALESCE(phone, '') LIKE ?
     ORDER BY full_name
     LIMIT 15`
  ).all(like, like);
  const userRows = db.prepare(
    `SELECT id, full_name, phone, role
     FROM users
     WHERE full_name LIKE ? OR username LIKE ? OR COALESCE(phone, '') LIKE ?
     ORDER BY full_name
     LIMIT 15`
  ).all(like, like, like);
  const people = [...staffRows, ...userRows].slice(0, 25);

  const technicians = db.prepare(
    `SELECT DISTINCT technician_name, technician_phone, machinery_name
     FROM company_purchases
     WHERE COALESCE(technician_name, '') LIKE ?
        OR COALESCE(technician_phone, '') LIKE ?
        OR COALESCE(machinery_name, '') LIKE ?
        OR COALESCE(work_details, '') LIKE ?
     ORDER BY technician_name ASC
     LIMIT 20`
  ).all(like, like, like, like);

  const sellers = db.prepare(
    `SELECT source, seller_name
     FROM (
       SELECT 'imports' as source, seller_name
       FROM import_entries
       WHERE COALESCE(seller_name, '') LIKE ?
       UNION ALL
       SELECT 'purchases' as source, seller_name
       FROM company_purchases
       WHERE COALESCE(seller_name, '') LIKE ?
     )
     WHERE TRIM(COALESCE(seller_name, '')) <> ''
     LIMIT 25`
  ).all(like, like);

  const exportRows = db.prepare(
    `SELECT exports.id, exports.export_date, exports.total_amount, exports.credit_amount, exports.paid_amount,
            vehicles.vehicle_number, vehicles.owner_name
     FROM exports
     JOIN vehicles ON vehicles.id = exports.vehicle_id
     WHERE vehicles.vehicle_number LIKE ?
        OR vehicles.owner_name LIKE ?
        OR COALESCE(exports.note, '') LIKE ?
        OR COALESCE(exports.route, '') LIKE ?
        OR COALESCE(exports.checked_by_staff_name, '') LIKE ?
        OR COALESCE(exports.force_wash_staff_name, '') LIKE ?
     ORDER BY exports.export_date DESC, exports.id DESC
     LIMIT 15`
  ).all(like, like, like, like, like, like);

  const creditRows = db.prepare(
    `SELECT credits.id, credits.credit_date, credits.customer_name, credits.amount, credits.paid_amount,
            vehicles.vehicle_number, vehicles.owner_name
     FROM credits
     JOIN vehicles ON vehicles.id = credits.vehicle_id
     WHERE credits.customer_name LIKE ?
        OR vehicles.vehicle_number LIKE ?
        OR vehicles.owner_name LIKE ?
     ORDER BY credits.credit_date DESC, credits.id DESC
     LIMIT 15`
  ).all(like, like, like);

  const jarSaleRows = db.prepare(
    `SELECT jar_sales.id, jar_sales.sale_date, jar_sales.customer_name, jar_sales.quantity, jar_sales.total_amount,
            jar_types.name as jar_name,
            COALESCE(vehicles.vehicle_number, jar_sales.vehicle_number) as vehicle_number,
            vehicles.owner_name
     FROM jar_sales
     JOIN jar_types ON jar_types.id = jar_sales.jar_type_id
     LEFT JOIN vehicles ON vehicles.id = jar_sales.vehicle_id
     WHERE jar_types.name LIKE ?
        OR COALESCE(jar_sales.customer_name, '') LIKE ?
        OR COALESCE(jar_sales.vehicle_number, '') LIKE ?
        OR COALESCE(vehicles.vehicle_number, '') LIKE ?
        OR COALESCE(vehicles.owner_name, '') LIKE ?
     ORDER BY jar_sales.sale_date DESC, jar_sales.id DESC
     LIMIT 15`
  ).all(like, like, like, like, like);

  const importRows = db.prepare(
    `SELECT import_entries.id, import_entries.entry_date, import_entries.item_type, import_entries.quantity,
            import_entries.seller_name, import_entries.total_amount, import_entries.paid_amount,
            import_item_types.name as item_name
     FROM import_entries
     LEFT JOIN import_item_types ON import_item_types.code = import_entries.item_type
     WHERE import_entries.item_type LIKE ?
        OR COALESCE(import_item_types.name, '') LIKE ?
        OR COALESCE(import_entries.seller_name, '') LIKE ?
        OR COALESCE(import_entries.note, '') LIKE ?
     ORDER BY import_entries.entry_date DESC, import_entries.id DESC
     LIMIT 15`
  ).all(like, like, like, like);

  const purchaseRows = db.prepare(
    `SELECT id, purchase_date, item_name, seller_name, amount, paid_amount,
            machinery_name, technician_name, technician_phone
     FROM company_purchases
     WHERE item_name LIKE ?
        OR COALESCE(seller_name, '') LIKE ?
        OR COALESCE(machinery_name, '') LIKE ?
        OR COALESCE(technician_name, '') LIKE ?
        OR COALESCE(technician_phone, '') LIKE ?
        OR COALESCE(work_details, '') LIKE ?
     ORDER BY purchase_date DESC, id DESC
     LIMIT 15`
  ).all(like, like, like, like, like, like);

  const expenseRows = db.prepare(
    `SELECT vehicle_expenses.id, vehicle_expenses.expense_date, vehicle_expenses.expense_type, vehicle_expenses.amount,
            vehicles.vehicle_number, vehicles.owner_name
     FROM vehicle_expenses
     JOIN vehicles ON vehicles.id = vehicle_expenses.vehicle_id
     WHERE vehicles.vehicle_number LIKE ?
        OR vehicles.owner_name LIKE ?
        OR vehicle_expenses.expense_type LIKE ?
        OR COALESCE(vehicle_expenses.note, '') LIKE ?
     ORDER BY vehicle_expenses.expense_date DESC, vehicle_expenses.id DESC
     LIMIT 15`
  ).all(like, like, like, like);

  const totalMatches =
    vehicles.length +
    customers.length +
    people.length +
    technicians.length +
    sellers.length +
    exportRows.length +
    creditRows.length +
    jarSaleRows.length +
    importRows.length +
    purchaseRows.length +
    expenseRows.length;

  res.render("search", {
    ...basePayload,
    vehicles,
    customers,
    people,
    technicians,
    sellers,
    exportRows,
    creditRows,
    jarSaleRows,
    importRows,
    purchaseRows,
    expenseRows,
    totalMatches
  });
});

app.use("/admin", requireAuth, adminRoutes);
app.use("/records", requireAuth, recordsRoutes);

app.get("/health", (req, res) => {
  res.json({ status: "ok" });
});

app.use((req, res) => {
  res.status(404).render("not_found", { title: req.t("notFoundTitle") });
});

app.use((err, req, res, next) => {
  const status = err.status || 500;
  const message = err.message || "Internal Server Error";
  if (status !== 404) {
    console.error(`[${new Date().toISOString()}] ${req.method} ${req.url} - ${status}: ${message}`);
  }
  if (res.headersSent) return next(err);
  res.status(status).render("not_found", {
    title: status === 413 ? "File Too Large" : status === 400 ? "Bad Request" : "Error"
  });
});

const runAutoBackupIfDue = () => {
  try {
    const enabled = String(getSetting("auto_backup_enabled", "1")) !== "0";
    if (!enabled) return;

    const hourRaw = Number(getSetting("auto_backup_hour", "18"));
    const backupHour = Number.isNaN(hourRaw) ? 18 : Math.max(0, Math.min(23, Math.floor(hourRaw)));
    const keepRaw = Number(getSetting("auto_backup_keep", "30"));
    const keepCount = Number.isNaN(keepRaw) ? 30 : Math.max(3, Math.min(180, Math.floor(keepRaw)));

    const now = dayjs();
    const todayKey = now.format("YYYY-MM-DD");
    if (now.hour() !== backupHour) return;
    const lastAutoDate = String(getSetting("last_auto_backup_date", ""));
    if (lastAutoDate === todayKey) return;

    const backup = createBackupFile({ db, prefix: "aqua_msk_auto_backup" });
    pruneOldBackups(keepCount);
    setSetting("last_backup_at", new Date().toISOString());
    setSetting("last_auto_backup_date", todayKey);
    db.prepare(
      "INSERT INTO activity_logs (user_id, action, entity_type, entity_id, details) VALUES (?, ?, ?, ?, ?)"
    ).run(null, "backup", "system", "auto_backup", `file=${backup.filename}`);
  } catch (err) {
    // keep server running if backup fails
  }
};

let hybridSyncBusy = false;
const runAutoHybridSyncIfDue = async () => {
  if (hybridSyncBusy) return;
  if (!shouldAutoSync(db)) return;
  hybridSyncBusy = true;
  try {
    const result = await syncLocalToPostgres({ db, reason: "auto" });
    db.prepare(
      "INSERT INTO activity_logs (user_id, action, entity_type, entity_id, details) VALUES (?, ?, ?, ?, ?)"
    ).run(null, "sync", "hybrid", result.siteId, `rows=${result.syncedRows}, duration_ms=${result.durationMs}`);
  } catch (err) {
    db.prepare(
      "INSERT INTO activity_logs (user_id, action, entity_type, entity_id, details) VALUES (?, ?, ?, ?, ?)"
    ).run(null, "sync_failed", "hybrid", "auto", (err && err.message) ? err.message : "auto_sync_failed");
  } finally {
    hybridSyncBusy = false;
  }
};

setTimeout(runAutoBackupIfDue, 2500);
setInterval(runAutoBackupIfDue, 5 * 60 * 1000);
setTimeout(() => {
  runAutoHybridSyncIfDue();
}, 6000);
setInterval(() => {
  runAutoHybridSyncIfDue();
}, 60 * 1000);

app.listen(PORT, () => {
  console.log(`AQUA MSK app running on http://localhost:${PORT}`);
});
