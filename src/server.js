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
const { formatActivityRows } = require("./utils/activity");
const { createBackupFile, pruneOldBackups } = require("./utils/backup");
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
    secret: process.env.SESSION_SECRET || "aqua-msk-secret",
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
  const recentActivity = db.prepare(
    `SELECT action, entity_type, details, created_at
     FROM activity_logs
     WHERE user_id = ?
     ORDER BY created_at DESC
     LIMIT 5`
  ).all(user.id);
  const recentActivityRows = formatActivityRows(recentActivity, req.t);
  const jarTypes = db.prepare("SELECT name, price FROM jar_types WHERE active = 1 ORDER BY name").all();
  res.render("worker/dashboard", {
    title: req.t("workerDashboardTitle"),
    jarTypes,
    today,
    myExports,
    myCredits,
    myJarSales,
    myVehicleExpenses,
    recentActivity: recentActivityRows
  });
});

app.get("/search", requireAuth, (req, res) => {
  const q = (req.query.q || "").trim();
  const today = dayjs().format("YYYY-MM-DD");
  if (!q) {
    return res.render("search", { title: req.t("searchTitle"), q, vehicles: [], customers: [], today });
  }
  const like = `%${q}%`;
  const vehicles = db.prepare(
    "SELECT id, vehicle_number, owner_name, phone FROM vehicles WHERE vehicle_number LIKE ? OR owner_name LIKE ? OR phone LIKE ? ORDER BY vehicle_number"
  ).all(like, like, like);
  const customers = db.prepare(
    "SELECT DISTINCT customer_name FROM credits WHERE customer_name LIKE ? ORDER BY customer_name"
  ).all(like).map((row) => row.customer_name);

  res.render("search", {
    title: req.t("searchTitle"),
    q,
    vehicles,
    customers,
    today
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

setTimeout(runAutoBackupIfDue, 2500);
setInterval(runAutoBackupIfDue, 5 * 60 * 1000);

app.listen(PORT, () => {
  console.log(`AQUA MSK app running on http://localhost:${PORT}`);
});
