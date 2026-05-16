const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { DatabaseSync } = require("node:sqlite");
const { ensureRolePermissionRows } = require("./utils/modulePermissions");

const dbPath = path.join(__dirname, "..", "data", "aqua.db");
fs.mkdirSync(path.dirname(dbPath), { recursive: true });

const db = new DatabaseSync(dbPath);

db.exec("PRAGMA journal_mode = WAL;");
db.exec("PRAGMA busy_timeout = 5000;");

const quoteIdentifier = (value) => `"${String(value || "").replace(/"/g, "\"\"")}"`;
const toSafeLiteral = (value) => String(value || "").replace(/'/g, "''");
const toSafeName = (value) => String(value || "").replace(/[^a-zA-Z0-9_]/g, "_");
const normalizeSiteId = (value) => String(value || "")
  .trim()
  .toLowerCase()
  .replace(/[^a-z0-9_-]/g, "-")
  .replace(/-+/g, "-")
  .replace(/^-+|-+$/g, "");
const createLocalSiteId = () => {
  const seed = `${process.env.COMPUTERNAME || process.env.HOSTNAME || "aqua"}-${crypto.randomBytes(3).toString("hex")}`;
  return normalizeSiteId(seed) || `aqua-${crypto.randomBytes(4).toString("hex")}`;
};
const toMoney = (value) => {
  const num = Number(value || 0);
  if (!Number.isFinite(num)) return 0;
  return Math.round(num * 100) / 100;
};
const positiveMoney = (value) => {
  const num = toMoney(value);
  return num > 0 ? num : 0;
};
const HYBRID_SYNC_TABLES = [
  { name: "users", pk: "id" },
  { name: "vehicles", pk: "id" },
  { name: "customers", pk: "id" },
  { name: "staff", pk: "id" },
  { name: "exports", pk: "id" },
  { name: "export_credit_payments", pk: "id" },
  { name: "credits", pk: "id" },
  { name: "credit_payments", pk: "id" },
  { name: "jar_sales", pk: "id" },
  { name: "jar_sale_payments", pk: "id" },
  { name: "jar_container_lendings", pk: "id" },
  { name: "jar_container_lending_payments", pk: "id" },
  { name: "jar_container_lending_returns", pk: "id" },
  { name: "leakage_jar_sales", pk: "id" },
  { name: "leakage_jar_sale_payments", pk: "id" },
  { name: "daily_cleaning_routines", pk: "id" },
  { name: "import_entries", pk: "id" },
  { name: "import_payments", pk: "id" },
  { name: "company_purchases", pk: "id" },
  { name: "company_purchase_payments", pk: "id" },
  { name: "vehicle_expenses", pk: "id" },
  { name: "vehicle_expense_payments", pk: "id" },
  { name: "staff_salary_payments", pk: "id" },
  { name: "worker_salary_payments", pk: "id" },
  { name: "vehicle_savings", pk: "id" },
  { name: "rent_entries", pk: "id" },
  { name: "water_test_reports", pk: "id" }
];

const ensureColumn = (tableName, columnName, columnSql) => {
  const tableSql = quoteIdentifier(tableName);
  const cols = new Set(
    db.prepare(`PRAGMA table_info(${tableSql})`).all().map((row) => row.name)
  );
  if (!cols.has(columnName)) {
    db.exec(`ALTER TABLE ${tableSql} ADD COLUMN ${columnSql};`);
  }
};

const getOrCreateHybridSiteId = () => {
  const key = "hybrid_sync_site_id";
  const existing = db.prepare("SELECT value FROM settings WHERE key = ?").get(key);
  const normalized = normalizeSiteId(existing?.value || "");
  if (normalized) {
    if (normalized !== String(existing.value || "")) {
      db.prepare("UPDATE settings SET value = ? WHERE key = ?").run(normalized, key);
    }
    return normalized;
  }
  const next = createLocalSiteId();
  if (existing) {
    db.prepare("UPDATE settings SET value = ? WHERE key = ?").run(next, key);
  } else {
    db.prepare("INSERT INTO settings (key, value) VALUES (?, ?)").run(key, next);
  }
  return next;
};

const runBootstrapTransaction = (callback) => {
  db.exec("SAVEPOINT bootstrap_schema;");
  try {
    const result = callback();
    db.exec("RELEASE bootstrap_schema;");
    return result;
  } catch (err) {
    try {
      db.exec("ROLLBACK TO bootstrap_schema;");
      db.exec("RELEASE bootstrap_schema;");
    } catch (_) {
      // Preserve the original bootstrap error.
    }
    throw err;
  }
};

const ensureHybridSyncSchema = () => runBootstrapTransaction(() => {
  db.exec(`
    CREATE TABLE IF NOT EXISTS sync_queue (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      table_name TEXT NOT NULL,
      record_pk TEXT,
      record_global_id TEXT NOT NULL,
      operation TEXT NOT NULL CHECK (operation IN ('UPSERT','DELETE')),
      changed_at TEXT NOT NULL DEFAULT (datetime('now')),
      status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','PROCESSING','DONE','FAILED')),
      attempt_count INTEGER NOT NULL DEFAULT 0,
      last_error TEXT,
      payload TEXT
    );
  `);
  db.exec("CREATE INDEX IF NOT EXISTS idx_sync_queue_status_changed ON sync_queue(status, changed_at);");
  db.exec("CREATE INDEX IF NOT EXISTS idx_sync_queue_table_record ON sync_queue(table_name, record_global_id);");
  db.exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_sync_queue_pending_unique ON sync_queue(table_name, record_global_id, status);");

  const siteId = getOrCreateHybridSiteId();
  const setAppMode = db.prepare(
    `INSERT INTO settings (key, value)
     VALUES ('app_mode', 'OFFLINE')
     ON CONFLICT(key) DO NOTHING`
  );
  setAppMode.run();

  HYBRID_SYNC_TABLES.forEach((table) => {
    const tableSql = quoteIdentifier(table.name);
    const pkSql = quoteIdentifier(table.pk);
    const tableNameLiteral = toSafeLiteral(table.name);
    const siteIdLiteral = toSafeLiteral(siteId);
    const triggerNameBase = toSafeName(table.name);

    ensureColumn(table.name, "global_id", "global_id TEXT");
    ensureColumn(table.name, "site_id", "site_id TEXT");
    ensureColumn(table.name, "sync_status", "sync_status TEXT NOT NULL DEFAULT 'PENDING'");
    ensureColumn(table.name, "deleted_at", "deleted_at TEXT");
    ensureColumn(table.name, "created_at", "created_at TEXT");
    ensureColumn(table.name, "updated_at", "updated_at TEXT");

    db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS ${quoteIdentifier(`idx_${triggerNameBase}_global_id`)} ON ${tableSql}(global_id);`);
    db.exec(`CREATE INDEX IF NOT EXISTS ${quoteIdentifier(`idx_${triggerNameBase}_site_sync`)} ON ${tableSql}(site_id, sync_status);`);
    db.exec(`CREATE INDEX IF NOT EXISTS ${quoteIdentifier(`idx_${triggerNameBase}_updated_sync`)} ON ${tableSql}(updated_at, sync_status);`);

    db.prepare(
      `UPDATE ${tableSql}
       SET site_id = ?
       WHERE site_id IS NULL OR TRIM(site_id) = ''`
    ).run(siteId);
    db.exec(
      `UPDATE ${tableSql}
       SET created_at = COALESCE(NULLIF(created_at, ''), datetime('now'))
       WHERE created_at IS NULL OR TRIM(created_at) = ''`
    );
    db.exec(
      `UPDATE ${tableSql}
       SET updated_at = COALESCE(NULLIF(updated_at, ''), created_at, datetime('now'))
       WHERE updated_at IS NULL OR TRIM(updated_at) = ''`
    );
    db.exec(
      `UPDATE ${tableSql}
       SET sync_status = 'PENDING'
       WHERE sync_status IS NULL
          OR TRIM(sync_status) = ''
          OR sync_status NOT IN ('PENDING','SYNCED','FAILED')`
    );

    const missingRows = db.prepare(
      `SELECT ${pkSql} as record_pk
       FROM ${tableSql}
       WHERE global_id IS NULL OR TRIM(global_id) = ''`
    ).all();
    const setGlobalId = db.prepare(
      `UPDATE ${tableSql}
       SET global_id = ?, sync_status = 'PENDING', updated_at = datetime('now')
       WHERE ${pkSql} = ?`
    );
    missingRows.forEach((row) => {
      setGlobalId.run(crypto.randomUUID(), row.record_pk);
    });

    db.exec(
      `INSERT INTO sync_queue (table_name, record_pk, record_global_id, operation, changed_at, status, attempt_count, last_error, payload)
       SELECT '${tableNameLiteral}', CAST(${pkSql} AS TEXT), global_id, 'UPSERT',
              COALESCE(NULLIF(updated_at, ''), datetime('now')), 'PENDING', 0, NULL, NULL
       FROM ${tableSql}
       WHERE global_id IS NOT NULL
         AND TRIM(global_id) <> ''
         AND sync_status != 'SYNCED'
       ON CONFLICT(table_name, record_global_id, status) DO UPDATE SET
         record_pk = excluded.record_pk,
         operation = excluded.operation,
         changed_at = excluded.changed_at,
         attempt_count = 0,
         last_error = NULL`
    );

    db.exec(`DROP TRIGGER IF EXISTS ${quoteIdentifier(`trg_sync_${triggerNameBase}_ins`)};`);
    db.exec(`DROP TRIGGER IF EXISTS ${quoteIdentifier(`trg_sync_${triggerNameBase}_upd`)};`);
    db.exec(`DROP TRIGGER IF EXISTS ${quoteIdentifier(`trg_sync_${triggerNameBase}_del`)};`);

    db.exec(`
      CREATE TRIGGER IF NOT EXISTS ${quoteIdentifier(`trg_sync_${triggerNameBase}_ins`)}
      AFTER INSERT ON ${tableSql}
      FOR EACH ROW
      BEGIN
        UPDATE ${tableSql}
        SET global_id = COALESCE(NULLIF(global_id, ''), LOWER(HEX(RANDOMBLOB(16)))),
            site_id = COALESCE(NULLIF(site_id, ''), '${siteIdLiteral}'),
            sync_status = CASE
              WHEN sync_status IN ('PENDING','SYNCED','FAILED') THEN sync_status
              ELSE 'PENDING'
            END,
            created_at = COALESCE(NULLIF(created_at, ''), datetime('now')),
            updated_at = datetime('now')
        WHERE ${pkSql} = NEW.${pkSql};
        INSERT INTO sync_queue (table_name, record_pk, record_global_id, operation, changed_at, status, attempt_count, last_error, payload)
        SELECT '${tableNameLiteral}',
               CAST(${pkSql} AS TEXT),
               global_id,
               'UPSERT',
               datetime('now'),
               'PENDING',
               0,
               NULL,
               NULL
        FROM ${tableSql}
        WHERE ${pkSql} = NEW.${pkSql}
        ON CONFLICT(table_name, record_global_id, status) DO UPDATE SET
          record_pk = excluded.record_pk,
          operation = 'UPSERT',
          changed_at = datetime('now'),
          attempt_count = 0,
          last_error = NULL,
          payload = NULL;
      END;
    `);

    db.exec(`
      CREATE TRIGGER IF NOT EXISTS ${quoteIdentifier(`trg_sync_${triggerNameBase}_upd`)}
      AFTER UPDATE ON ${tableSql}
      FOR EACH ROW
      BEGIN
        UPDATE ${tableSql}
        SET global_id = COALESCE(NULLIF(global_id, ''), LOWER(HEX(RANDOMBLOB(16)))),
            site_id = COALESCE(NULLIF(site_id, ''), '${siteIdLiteral}'),
            sync_status = CASE
              WHEN sync_status IN ('PENDING','SYNCED','FAILED') THEN sync_status
              ELSE 'PENDING'
            END,
            created_at = COALESCE(NULLIF(created_at, ''), datetime('now')),
            updated_at = datetime('now')
        WHERE ${pkSql} = NEW.${pkSql};
        INSERT INTO sync_queue (table_name, record_pk, record_global_id, operation, changed_at, status, attempt_count, last_error, payload)
        SELECT '${tableNameLiteral}',
               CAST(${pkSql} AS TEXT),
               global_id,
               'UPSERT',
               datetime('now'),
               'PENDING',
               0,
               NULL,
               NULL
        FROM ${tableSql}
        WHERE ${pkSql} = NEW.${pkSql}
        ON CONFLICT(table_name, record_global_id, status) DO UPDATE SET
          record_pk = excluded.record_pk,
          operation = 'UPSERT',
          changed_at = datetime('now'),
          attempt_count = 0,
          last_error = NULL,
          payload = NULL;
      END;
    `);

    db.exec(`
      CREATE TRIGGER IF NOT EXISTS ${quoteIdentifier(`trg_sync_${triggerNameBase}_del`)}
      AFTER DELETE ON ${tableSql}
      FOR EACH ROW
      BEGIN
        INSERT INTO sync_queue (table_name, record_pk, record_global_id, operation, changed_at, status, attempt_count, last_error, payload)
        VALUES (
          '${tableNameLiteral}',
          CAST(OLD.${pkSql} AS TEXT),
          COALESCE(NULLIF(OLD.global_id, ''), 'legacy-${tableNameLiteral}-' || CAST(OLD.${pkSql} AS TEXT)),
          'DELETE',
          datetime('now'),
          'PENDING',
          0,
          NULL,
          NULL
        )
        ON CONFLICT(table_name, record_global_id, status) DO UPDATE SET
          record_pk = excluded.record_pk,
          operation = 'DELETE',
          changed_at = datetime('now'),
          attempt_count = 0,
          last_error = NULL,
          payload = NULL;
      END;
    `);
  });
});

db.exec(`
CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  username TEXT UNIQUE NOT NULL,
  password_hash TEXT NOT NULL,
  full_name TEXT NOT NULL,
  phone TEXT,
  fingerprint_id TEXT,
  role TEXT NOT NULL CHECK (role IN ('SUPER_ADMIN','ADMIN','WORKER')),
  start_date TEXT,
  monthly_salary REAL NOT NULL DEFAULT 0,
  is_active INTEGER NOT NULL DEFAULT 1,
  deactivated_at TEXT,
  deactivated_by INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS vehicles (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  vehicle_number TEXT UNIQUE NOT NULL,
  owner_name TEXT NOT NULL,
  phone TEXT,
  is_company INTEGER NOT NULL DEFAULT 0,
  profile_pic_path TEXT,
  is_active INTEGER NOT NULL DEFAULT 1,
  deactivated_at TEXT,
  deactivated_by INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (deactivated_by) REFERENCES users(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS vehicle_compliance (
  vehicle_id INTEGER PRIMARY KEY,
  insurance_expiry TEXT,
  tax_expiry TEXT,
  permit_expiry TEXT,
  fitness_expiry TEXT,
  pollution_expiry TEXT,
  note TEXT,
  updated_by INTEGER,
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (vehicle_id) REFERENCES vehicles(id) ON DELETE CASCADE,
  FOREIGN KEY (updated_by) REFERENCES users(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_vehicle_compliance_updated_at ON vehicle_compliance(updated_at);

CREATE TABLE IF NOT EXISTS daily_sales (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  vehicle_id INTEGER NOT NULL,
  sale_date TEXT NOT NULL,
  total_sales REAL NOT NULL DEFAULT 0,
  created_by INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (vehicle_id) REFERENCES vehicles(id) ON DELETE CASCADE,
  FOREIGN KEY (created_by) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS exports (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  vehicle_id INTEGER NOT NULL,
  export_date TEXT NOT NULL,
  jar_count INTEGER NOT NULL DEFAULT 0,
  jar_container_given_count INTEGER NOT NULL DEFAULT 0,
  bottle_case_count INTEGER NOT NULL DEFAULT 0,
  dispenser_count INTEGER NOT NULL DEFAULT 0,
  jar_unit_price REAL NOT NULL DEFAULT 0,
  bottle_case_unit_price REAL NOT NULL DEFAULT 0,
  dispenser_unit_price REAL NOT NULL DEFAULT 0,
  return_jar_count INTEGER NOT NULL DEFAULT 0,
  return_bottle_case_count INTEGER NOT NULL DEFAULT 0,
  damaged_bottle_case_count INTEGER NOT NULL DEFAULT 0,
  leakage_jar_count INTEGER NOT NULL DEFAULT 0,
  sold_jar_count INTEGER NOT NULL DEFAULT 0,
  sold_jar_price REAL NOT NULL DEFAULT 0,
  sold_jar_amount REAL NOT NULL DEFAULT 0,
  collection_amount REAL NOT NULL DEFAULT 0,
  expense_amount REAL NOT NULL DEFAULT 0,
  expense_note TEXT,
  total_amount REAL NOT NULL DEFAULT 0,
  paid_amount REAL NOT NULL DEFAULT 0,
  payment_method TEXT NOT NULL DEFAULT 'CASH',
  credit_amount REAL NOT NULL DEFAULT 0,
  receipt_no TEXT,
  checked_by_staff_id INTEGER,
  checked_by_staff_name TEXT,
  force_wash_required INTEGER NOT NULL DEFAULT 0,
  force_wash_staff_name TEXT,
  created_by INTEGER,
  note TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (vehicle_id) REFERENCES vehicles(id) ON DELETE CASCADE,
  FOREIGN KEY (checked_by_staff_id) REFERENCES staff(id) ON DELETE SET NULL,
  FOREIGN KEY (created_by) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS customers (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  phone TEXT,
  location TEXT,
  note TEXT,
  is_active INTEGER NOT NULL DEFAULT 1,
  merged_into_customer_id INTEGER,
  deactivated_at TEXT,
  deactivated_by INTEGER,
  created_by INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (merged_into_customer_id) REFERENCES customers(id) ON DELETE SET NULL,
  FOREIGN KEY (deactivated_by) REFERENCES users(id) ON DELETE SET NULL,
  FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS credits (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  vehicle_id INTEGER NOT NULL,
  export_id INTEGER,
  customer_id INTEGER,
  customer_name TEXT NOT NULL,
  customer_phone TEXT,
  customer_location TEXT,
  amount REAL NOT NULL DEFAULT 0,
  paid_amount REAL NOT NULL DEFAULT 0,
  payment_method TEXT NOT NULL DEFAULT 'CASH',
  credit_jars INTEGER NOT NULL DEFAULT 0,
  credit_bottle_cases INTEGER NOT NULL DEFAULT 0,
  credit_dispensers INTEGER NOT NULL DEFAULT 0,
  credit_jar_containers INTEGER NOT NULL DEFAULT 0,
  jar_price REAL NOT NULL DEFAULT 0,
  bottle_case_price REAL NOT NULL DEFAULT 0,
  dispenser_price REAL NOT NULL DEFAULT 0,
  jar_container_price REAL NOT NULL DEFAULT 0,
  credit_date TEXT NOT NULL,
  trip_date TEXT,
  checked_by_staff_id INTEGER,
  force_wash_required INTEGER NOT NULL DEFAULT 0,
  receipt_no TEXT,
  paid INTEGER NOT NULL DEFAULT 0,
  created_by INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (vehicle_id) REFERENCES vehicles(id) ON DELETE CASCADE,
  FOREIGN KEY (export_id) REFERENCES exports(id) ON DELETE SET NULL,
  FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE SET NULL,
  FOREIGN KEY (checked_by_staff_id) REFERENCES staff(id) ON DELETE SET NULL,
  FOREIGN KEY (created_by) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_sales_date ON daily_sales(sale_date);
CREATE INDEX IF NOT EXISTS idx_exports_date ON exports(export_date);
CREATE INDEX IF NOT EXISTS idx_customers_name ON customers(name);
CREATE INDEX IF NOT EXISTS idx_customers_active ON customers(is_active);
CREATE INDEX IF NOT EXISTS idx_customers_merged_into ON customers(merged_into_customer_id);
CREATE INDEX IF NOT EXISTS idx_credits_date ON credits(credit_date);

CREATE TABLE IF NOT EXISTS credit_payments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  credit_id INTEGER NOT NULL,
  amount REAL NOT NULL DEFAULT 0,
  payment_method TEXT NOT NULL DEFAULT 'CASH',
  note TEXT,
  created_by INTEGER,
  paid_at TEXT NOT NULL DEFAULT (datetime('now')),
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (credit_id) REFERENCES credits(id) ON DELETE CASCADE,
  FOREIGN KEY (created_by) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_credit_payments_credit ON credit_payments(credit_id);
CREATE INDEX IF NOT EXISTS idx_credit_payments_paid_at ON credit_payments(paid_at);

CREATE TABLE IF NOT EXISTS export_credit_payments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  export_id INTEGER,
  vehicle_id INTEGER,
  payment_date TEXT NOT NULL,
  amount REAL NOT NULL DEFAULT 0,
  cash_amount REAL NOT NULL DEFAULT 0,
  bank_amount REAL NOT NULL DEFAULT 0,
  ewallet_amount REAL NOT NULL DEFAULT 0,
  payment_method TEXT NOT NULL DEFAULT 'CASH',
  note TEXT,
  receipt_no TEXT,
  legacy_activity_id INTEGER UNIQUE,
  created_by INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (export_id) REFERENCES exports(id) ON DELETE SET NULL,
  FOREIGN KEY (vehicle_id) REFERENCES vehicles(id) ON DELETE SET NULL,
  FOREIGN KEY (created_by) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_export_credit_payments_export ON export_credit_payments(export_id);
CREATE INDEX IF NOT EXISTS idx_export_credit_payments_vehicle ON export_credit_payments(vehicle_id);
CREATE INDEX IF NOT EXISTS idx_export_credit_payments_date ON export_credit_payments(payment_date);
CREATE UNIQUE INDEX IF NOT EXISTS idx_export_credit_payments_receipt_no ON export_credit_payments(receipt_no);

CREATE TABLE IF NOT EXISTS settings (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS role_permissions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  role TEXT NOT NULL CHECK (role IN ('ADMIN','WORKER')),
  module_key TEXT NOT NULL,
  can_view INTEGER NOT NULL DEFAULT 1,
  can_edit INTEGER NOT NULL DEFAULT 1,
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  UNIQUE(role, module_key)
);

CREATE TABLE IF NOT EXISTS account_recovery (
  user_id INTEGER PRIMARY KEY,
  key_hash TEXT,
  key_created_at TEXT,
  q1 TEXT,
  a1_hash TEXT,
  q2 TEXT,
  a2_hash TEXT,
  q3 TEXT,
  a3_hash TEXT,
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS activity_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER,
  action TEXT NOT NULL,
  entity_type TEXT NOT NULL,
  entity_id TEXT,
  details TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_activity_logs_created_at ON activity_logs(created_at);
CREATE INDEX IF NOT EXISTS idx_activity_logs_user ON activity_logs(user_id);
CREATE INDEX IF NOT EXISTS idx_activity_logs_entity ON activity_logs(entity_type, entity_id);

CREATE TABLE IF NOT EXISTS recycle_bin (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  entity_type TEXT NOT NULL,
  entity_id TEXT,
  payload TEXT NOT NULL,
  note TEXT,
  deleted_by INTEGER,
  deleted_at TEXT NOT NULL DEFAULT (datetime('now')),
  restore_until TEXT NOT NULL,
  restored_at TEXT,
  restored_by INTEGER,
  FOREIGN KEY (deleted_by) REFERENCES users(id) ON DELETE SET NULL,
  FOREIGN KEY (restored_by) REFERENCES users(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_recycle_bin_deleted_at ON recycle_bin(deleted_at);
CREATE INDEX IF NOT EXISTS idx_recycle_bin_restore_until ON recycle_bin(restore_until);
CREATE INDEX IF NOT EXISTS idx_recycle_bin_restored_at ON recycle_bin(restored_at);
CREATE INDEX IF NOT EXISTS idx_recycle_bin_entity ON recycle_bin(entity_type, entity_id);

CREATE TABLE IF NOT EXISTS stock_ledger (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  item_type TEXT NOT NULL CHECK (item_type IN ('JAR','BOTTLE')),
  direction TEXT NOT NULL CHECK (direction IN ('IN','OUT')),
  quantity INTEGER NOT NULL DEFAULT 0,
  entry_date TEXT NOT NULL,
  note TEXT,
  created_by INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (created_by) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_stock_ledger_date ON stock_ledger(entry_date);
CREATE INDEX IF NOT EXISTS idx_stock_ledger_item ON stock_ledger(item_type);

CREATE TABLE IF NOT EXISTS jar_types (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  price REAL NOT NULL DEFAULT 0,
  default_qty INTEGER NOT NULL DEFAULT 0,
  active INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS jar_cap_types (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  default_qty INTEGER NOT NULL DEFAULT 0,
  active INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS jar_sales (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  jar_type_id INTEGER NOT NULL,
  customer_name TEXT,
  vehicle_id INTEGER,
  vehicle_number TEXT,
  sale_date TEXT NOT NULL,
  quantity INTEGER NOT NULL DEFAULT 0,
  unit_price REAL NOT NULL DEFAULT 0,
  total_amount REAL NOT NULL DEFAULT 0,
  paid_amount REAL NOT NULL DEFAULT 0,
  credit_amount REAL NOT NULL DEFAULT 0,
  note TEXT,
  created_by INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (jar_type_id) REFERENCES jar_types(id) ON DELETE CASCADE,
  FOREIGN KEY (vehicle_id) REFERENCES vehicles(id) ON DELETE SET NULL,
  FOREIGN KEY (created_by) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_jar_sales_date ON jar_sales(sale_date);
CREATE INDEX IF NOT EXISTS idx_jar_sales_type ON jar_sales(jar_type_id);

CREATE TABLE IF NOT EXISTS jar_sale_payments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  jar_sale_id INTEGER NOT NULL,
  payment_date TEXT NOT NULL,
  amount REAL NOT NULL DEFAULT 0,
  cash_amount REAL NOT NULL DEFAULT 0,
  bank_amount REAL NOT NULL DEFAULT 0,
  ewallet_amount REAL NOT NULL DEFAULT 0,
  payment_method TEXT NOT NULL DEFAULT 'CASH',
  note TEXT,
  receipt_no TEXT,
  created_by INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (jar_sale_id) REFERENCES jar_sales(id) ON DELETE CASCADE,
  FOREIGN KEY (created_by) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_jar_sale_payments_sale ON jar_sale_payments(jar_sale_id);
CREATE INDEX IF NOT EXISTS idx_jar_sale_payments_date ON jar_sale_payments(payment_date);

CREATE TABLE IF NOT EXISTS jar_container_lendings (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  jar_type_id INTEGER NOT NULL,
  source_type TEXT NOT NULL DEFAULT 'STORAGE',
  vehicle_id INTEGER,
  lend_date TEXT NOT NULL,
  customer_name TEXT NOT NULL,
  phone TEXT,
  location TEXT,
  quantity INTEGER NOT NULL DEFAULT 0,
  deposit_amount REAL NOT NULL DEFAULT 0,
  deposit_paid_amount REAL NOT NULL DEFAULT 0,
  deposit_paid_cash_amount REAL NOT NULL DEFAULT 0,
  deposit_paid_bank_amount REAL NOT NULL DEFAULT 0,
  deposit_paid_ewallet_amount REAL NOT NULL DEFAULT 0,
  deposit_refund_amount REAL NOT NULL DEFAULT 0,
  deposit_refund_cash_amount REAL NOT NULL DEFAULT 0,
  deposit_refund_bank_amount REAL NOT NULL DEFAULT 0,
  deposit_refund_ewallet_amount REAL NOT NULL DEFAULT 0,
  note TEXT,
  created_by INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (jar_type_id) REFERENCES jar_types(id) ON DELETE CASCADE,
  FOREIGN KEY (vehicle_id) REFERENCES vehicles(id) ON DELETE SET NULL,
  FOREIGN KEY (created_by) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_jar_container_lendings_date ON jar_container_lendings(lend_date);
CREATE INDEX IF NOT EXISTS idx_jar_container_lendings_type ON jar_container_lendings(jar_type_id);
CREATE INDEX IF NOT EXISTS idx_jar_container_lendings_customer ON jar_container_lendings(customer_name);

CREATE TABLE IF NOT EXISTS jar_container_lending_payments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  lending_id INTEGER NOT NULL,
  payment_date TEXT NOT NULL,
  amount REAL NOT NULL DEFAULT 0,
  cash_amount REAL NOT NULL DEFAULT 0,
  bank_amount REAL NOT NULL DEFAULT 0,
  ewallet_amount REAL NOT NULL DEFAULT 0,
  payment_method TEXT NOT NULL DEFAULT 'CASH',
  note TEXT,
  receipt_no TEXT,
  created_by INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (lending_id) REFERENCES jar_container_lendings(id) ON DELETE CASCADE,
  FOREIGN KEY (created_by) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_jar_container_lending_payments_lending ON jar_container_lending_payments(lending_id);
CREATE INDEX IF NOT EXISTS idx_jar_container_lending_payments_date ON jar_container_lending_payments(payment_date);

CREATE TABLE IF NOT EXISTS jar_container_lending_returns (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  lending_id INTEGER NOT NULL,
  return_date TEXT NOT NULL,
  quantity INTEGER NOT NULL DEFAULT 0,
  refund_amount REAL NOT NULL DEFAULT 0,
  refund_cash_amount REAL NOT NULL DEFAULT 0,
  refund_bank_amount REAL NOT NULL DEFAULT 0,
  refund_ewallet_amount REAL NOT NULL DEFAULT 0,
  refund_method TEXT NOT NULL DEFAULT 'CASH',
  note TEXT,
  receipt_no TEXT,
  created_by INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (lending_id) REFERENCES jar_container_lendings(id) ON DELETE CASCADE,
  FOREIGN KEY (created_by) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_jar_container_lending_returns_lending ON jar_container_lending_returns(lending_id);
CREATE INDEX IF NOT EXISTS idx_jar_container_lending_returns_date ON jar_container_lending_returns(return_date);

CREATE TABLE IF NOT EXISTS leakage_jar_sales (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  sale_date TEXT NOT NULL,
  quantity INTEGER NOT NULL DEFAULT 0,
  unit_price REAL NOT NULL DEFAULT 0,
  total_amount REAL NOT NULL DEFAULT 0,
  paid_amount REAL NOT NULL DEFAULT 0,
  paid_cash_amount REAL NOT NULL DEFAULT 0,
  paid_bank_amount REAL NOT NULL DEFAULT 0,
  paid_ewallet_amount REAL NOT NULL DEFAULT 0,
  payment_method TEXT NOT NULL DEFAULT 'CASH',
  credit_amount REAL NOT NULL DEFAULT 0,
  note TEXT,
  created_by INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (created_by) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_leakage_jar_sales_date ON leakage_jar_sales(sale_date);

CREATE TABLE IF NOT EXISTS leakage_jar_sale_payments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  leakage_jar_sale_id INTEGER NOT NULL,
  payment_date TEXT NOT NULL,
  amount REAL NOT NULL DEFAULT 0,
  cash_amount REAL NOT NULL DEFAULT 0,
  bank_amount REAL NOT NULL DEFAULT 0,
  ewallet_amount REAL NOT NULL DEFAULT 0,
  payment_method TEXT NOT NULL DEFAULT 'CASH',
  note TEXT,
  receipt_no TEXT,
  created_by INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (leakage_jar_sale_id) REFERENCES leakage_jar_sales(id) ON DELETE CASCADE,
  FOREIGN KEY (created_by) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_leakage_jar_sale_payments_sale ON leakage_jar_sale_payments(leakage_jar_sale_id);
CREATE INDEX IF NOT EXISTS idx_leakage_jar_sale_payments_date ON leakage_jar_sale_payments(payment_date);

CREATE TABLE IF NOT EXISTS daily_cleaning_routines (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  routine_date TEXT NOT NULL,
  shift TEXT NOT NULL DEFAULT 'MORNING',
  area_name TEXT NOT NULL,
  task_name TEXT NOT NULL,
  cleaned_by TEXT,
  status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','DONE','SKIPPED')),
  note TEXT,
  created_by INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (created_by) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_daily_cleaning_routines_date ON daily_cleaning_routines(routine_date);
CREATE INDEX IF NOT EXISTS idx_daily_cleaning_routines_status ON daily_cleaning_routines(status);

CREATE TABLE IF NOT EXISTS import_item_types (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  code TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL,
  unit_label TEXT,
  uses_direction INTEGER NOT NULL DEFAULT 1,
  is_predefined INTEGER NOT NULL DEFAULT 0,
  is_active INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS import_entries (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  item_type TEXT NOT NULL,
  quantity INTEGER NOT NULL DEFAULT 0,
  unit_price REAL NOT NULL DEFAULT 0,
  direction TEXT NOT NULL DEFAULT 'IN',
  jar_type_id INTEGER,
  jar_cap_type_id INTEGER,
  entry_date TEXT NOT NULL,
  seller_name TEXT,
  total_amount REAL NOT NULL DEFAULT 0,
  paid_amount REAL NOT NULL DEFAULT 0,
  is_credit INTEGER NOT NULL DEFAULT 0,
  note TEXT,
  created_by INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (created_by) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_import_entries_date ON import_entries(entry_date);
CREATE INDEX IF NOT EXISTS idx_import_entries_item ON import_entries(item_type);

CREATE TABLE IF NOT EXISTS import_payments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  import_entry_id INTEGER NOT NULL,
  payment_date TEXT NOT NULL,
  amount REAL NOT NULL DEFAULT 0,
  payment_method TEXT NOT NULL DEFAULT 'CASH',
  payment_source TEXT NOT NULL DEFAULT 'DAILY_COLLECTION',
  note TEXT,
  created_by INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (import_entry_id) REFERENCES import_entries(id) ON DELETE CASCADE,
  FOREIGN KEY (created_by) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_import_payments_entry ON import_payments(import_entry_id);
CREATE INDEX IF NOT EXISTS idx_import_payments_date ON import_payments(payment_date);

CREATE TABLE IF NOT EXISTS company_purchases (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  purchase_date TEXT NOT NULL,
  expense_category TEXT NOT NULL DEFAULT 'GENERAL',
  item_name TEXT NOT NULL,
  seller_name TEXT,
  amount REAL NOT NULL DEFAULT 0,
  paid_amount REAL NOT NULL DEFAULT 0,
  is_credit INTEGER NOT NULL DEFAULT 0,
  is_machinery INTEGER NOT NULL DEFAULT 0,
  machinery_name TEXT,
  technician_name TEXT,
  technician_phone TEXT,
  work_details TEXT,
  note TEXT,
  created_by INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (created_by) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_company_purchases_date ON company_purchases(purchase_date);
CREATE INDEX IF NOT EXISTS idx_company_purchases_tech ON company_purchases(technician_phone);
CREATE INDEX IF NOT EXISTS idx_company_purchases_item ON company_purchases(item_name);

CREATE TABLE IF NOT EXISTS company_purchase_payments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  company_purchase_id INTEGER NOT NULL,
  payment_date TEXT NOT NULL,
  amount REAL NOT NULL DEFAULT 0,
  payment_method TEXT NOT NULL DEFAULT 'CASH',
  payment_source TEXT NOT NULL DEFAULT 'DAILY_COLLECTION',
  note TEXT,
  created_by INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (company_purchase_id) REFERENCES company_purchases(id) ON DELETE CASCADE,
  FOREIGN KEY (created_by) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_company_purchase_payments_purchase ON company_purchase_payments(company_purchase_id);
CREATE INDEX IF NOT EXISTS idx_company_purchase_payments_date ON company_purchase_payments(payment_date);

CREATE TABLE IF NOT EXISTS vehicle_expenses (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  vehicle_id INTEGER NOT NULL,
  expense_date TEXT NOT NULL,
  expense_type TEXT NOT NULL CHECK (expense_type IN ('FUEL','REPAIR','SERVICE','OTHER')),
  amount REAL NOT NULL DEFAULT 0,
  paid_amount REAL NOT NULL DEFAULT 0,
  is_credit INTEGER NOT NULL DEFAULT 0,
  note TEXT,
  created_by INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (vehicle_id) REFERENCES vehicles(id) ON DELETE CASCADE,
  FOREIGN KEY (created_by) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_vehicle_expenses_date ON vehicle_expenses(expense_date);
CREATE INDEX IF NOT EXISTS idx_vehicle_expenses_vehicle ON vehicle_expenses(vehicle_id);
CREATE INDEX IF NOT EXISTS idx_vehicle_expenses_type ON vehicle_expenses(expense_type);

CREATE TABLE IF NOT EXISTS vehicle_expense_payments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  vehicle_expense_id INTEGER NOT NULL,
  payment_date TEXT NOT NULL,
  amount REAL NOT NULL DEFAULT 0,
  payment_method TEXT NOT NULL DEFAULT 'CASH',
  payment_source TEXT NOT NULL DEFAULT 'DAILY_COLLECTION',
  note TEXT,
  created_by INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (vehicle_expense_id) REFERENCES vehicle_expenses(id) ON DELETE CASCADE,
  FOREIGN KEY (created_by) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_vehicle_expense_payments_entry ON vehicle_expense_payments(vehicle_expense_id);
CREATE INDEX IF NOT EXISTS idx_vehicle_expense_payments_date ON vehicle_expense_payments(payment_date);

CREATE TABLE IF NOT EXISTS rent_entries (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  rent_date TEXT NOT NULL,
  renter_name TEXT NOT NULL,
  item_name TEXT NOT NULL,
  amount REAL NOT NULL DEFAULT 0,
  payment_method TEXT NOT NULL DEFAULT 'CASH',
  add_to_collection INTEGER NOT NULL DEFAULT 1,
  note TEXT,
  created_by INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (created_by) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_rent_entries_date ON rent_entries(rent_date);
CREATE INDEX IF NOT EXISTS idx_rent_entries_renter ON rent_entries(renter_name);

CREATE TABLE IF NOT EXISTS water_test_reports (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  test_date TEXT NOT NULL,
  ph_value REAL NOT NULL DEFAULT 0,
  tds_value REAL NOT NULL DEFAULT 0,
  coliform TEXT,
  forensic_report_path TEXT,
  forensic_report_name TEXT,
  government_report_path TEXT,
  government_report_name TEXT,
  note TEXT,
  created_by INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (created_by) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_water_test_reports_date ON water_test_reports(test_date);

CREATE TABLE IF NOT EXISTS vehicle_savings (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  vehicle_id INTEGER NOT NULL,
  export_id INTEGER,
  entry_date TEXT NOT NULL,
  amount REAL NOT NULL DEFAULT 0,
  payment_source TEXT NOT NULL DEFAULT 'DAILY_COLLECTION',
  note TEXT,
  created_by INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (vehicle_id) REFERENCES vehicles(id) ON DELETE CASCADE,
  FOREIGN KEY (export_id) REFERENCES exports(id) ON DELETE SET NULL,
  FOREIGN KEY (created_by) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_vehicle_savings_date ON vehicle_savings(entry_date);
CREATE INDEX IF NOT EXISTS idx_vehicle_savings_vehicle ON vehicle_savings(vehicle_id);

CREATE TABLE IF NOT EXISTS staff (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  full_name TEXT NOT NULL,
  staff_role TEXT,
  phone TEXT,
  fingerprint_id TEXT,
  photo_path TEXT,
  start_date TEXT,
  monthly_salary REAL NOT NULL DEFAULT 0,
  is_active INTEGER NOT NULL DEFAULT 1,
  deactivated_at TEXT,
  deactivated_by INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (deactivated_by) REFERENCES users(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS staff_roles (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  code TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL,
  is_active INTEGER NOT NULL DEFAULT 1,
  show_in_exports INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_staff_roles_active ON staff_roles(is_active);

CREATE TABLE IF NOT EXISTS staff_documents (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  staff_id INTEGER NOT NULL,
  doc_type TEXT NOT NULL CHECK (doc_type IN ('CITIZENSHIP','LICENSE','PASSPORT','PAN','NATIONAL_ID','VOTER_CARD','OTHERS')),
  front_path TEXT,
  back_path TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (staff_id) REFERENCES staff(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS staff_attendance (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  staff_id INTEGER NOT NULL,
  attendance_date TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('PRESENT','ABSENT')),
  recorded_by INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  UNIQUE(staff_id, attendance_date),
  FOREIGN KEY (staff_id) REFERENCES staff(id) ON DELETE CASCADE,
  FOREIGN KEY (recorded_by) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_staff_attendance_date ON staff_attendance(attendance_date);
CREATE INDEX IF NOT EXISTS idx_staff_attendance_staff ON staff_attendance(staff_id);

CREATE TABLE IF NOT EXISTS user_attendance (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  attendance_date TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('PRESENT','ABSENT')),
  recorded_by INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  UNIQUE(user_id, attendance_date),
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
  FOREIGN KEY (recorded_by) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_user_attendance_date ON user_attendance(attendance_date);
CREATE INDEX IF NOT EXISTS idx_user_attendance_user ON user_attendance(user_id);

CREATE TABLE IF NOT EXISTS iot_attendance_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source TEXT NOT NULL CHECK (source IN ('MANUAL', 'API')),
  person_type TEXT NOT NULL CHECK (person_type IN ('STAFF', 'WORKER')),
  person_id INTEGER,
  fingerprint_id TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('PRESENT', 'ABSENT')),
  attendance_date TEXT NOT NULL,
  scanned_at TEXT NOT NULL DEFAULT (datetime('now')),
  note TEXT,
  recorded_by INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (recorded_by) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_iot_attendance_logs_date ON iot_attendance_logs(attendance_date);
CREATE INDEX IF NOT EXISTS idx_iot_attendance_logs_person ON iot_attendance_logs(person_type, person_id);
CREATE INDEX IF NOT EXISTS idx_iot_attendance_logs_fp ON iot_attendance_logs(fingerprint_id);

CREATE TABLE IF NOT EXISTS staff_salary_payments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  staff_id INTEGER NOT NULL,
  payment_date TEXT NOT NULL,
  amount REAL NOT NULL DEFAULT 0,
  payment_type TEXT NOT NULL CHECK (payment_type IN ('SALARY','ADVANCE')),
  payment_source TEXT NOT NULL DEFAULT 'DAILY_COLLECTION' CHECK (payment_source IN ('DAILY_COLLECTION','OWNER_PERSONAL','BANK_OTHER')),
  receipt_no TEXT,
  note TEXT,
  created_by INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (staff_id) REFERENCES staff(id) ON DELETE CASCADE,
  FOREIGN KEY (created_by) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_staff_salary_date ON staff_salary_payments(payment_date);
CREATE INDEX IF NOT EXISTS idx_staff_salary_staff ON staff_salary_payments(staff_id);

CREATE TABLE IF NOT EXISTS worker_salary_payments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  worker_id INTEGER NOT NULL,
  payment_date TEXT NOT NULL,
  amount REAL NOT NULL DEFAULT 0,
  payment_type TEXT NOT NULL CHECK (payment_type IN ('SALARY','ADVANCE')),
  payment_source TEXT NOT NULL DEFAULT 'DAILY_COLLECTION' CHECK (payment_source IN ('DAILY_COLLECTION','OWNER_PERSONAL','BANK_OTHER')),
  receipt_no TEXT,
  note TEXT,
  created_by INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (worker_id) REFERENCES users(id) ON DELETE CASCADE,
  FOREIGN KEY (created_by) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_worker_salary_date ON worker_salary_payments(payment_date);
CREATE INDEX IF NOT EXISTS idx_worker_salary_worker ON worker_salary_payments(worker_id);

CREATE TABLE IF NOT EXISTS staff_salary_adjustments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  staff_id INTEGER NOT NULL,
  effective_month TEXT NOT NULL,
  previous_salary REAL NOT NULL DEFAULT 0,
  new_salary REAL NOT NULL DEFAULT 0,
  note TEXT,
  created_by INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (staff_id) REFERENCES staff(id) ON DELETE CASCADE,
  FOREIGN KEY (created_by) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_staff_salary_adjustments_staff ON staff_salary_adjustments(staff_id);
CREATE INDEX IF NOT EXISTS idx_staff_salary_adjustments_month ON staff_salary_adjustments(effective_month);

CREATE TABLE IF NOT EXISTS worker_salary_adjustments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  worker_id INTEGER NOT NULL,
  effective_month TEXT NOT NULL,
  previous_salary REAL NOT NULL DEFAULT 0,
  new_salary REAL NOT NULL DEFAULT 0,
  note TEXT,
  created_by INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (worker_id) REFERENCES users(id) ON DELETE CASCADE,
  FOREIGN KEY (created_by) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_worker_salary_adjustments_worker ON worker_salary_adjustments(worker_id);
CREATE INDEX IF NOT EXISTS idx_worker_salary_adjustments_month ON worker_salary_adjustments(effective_month);

CREATE TABLE IF NOT EXISTS worker_documents (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  worker_id INTEGER NOT NULL UNIQUE,
  doc_type TEXT CHECK (doc_type IN ('CITIZENSHIP','LICENSE','PASSPORT','PAN','NATIONAL_ID','VOTER_CARD','OTHERS')),
  photo_path TEXT,
  front_path TEXT,
  back_path TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (worker_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_worker_documents_worker ON worker_documents(worker_id);

CREATE TABLE IF NOT EXISTS day_closures (
  closure_date TEXT PRIMARY KEY,
  is_closed INTEGER NOT NULL DEFAULT 1,
  note TEXT,
  closed_by INTEGER,
  closed_at TEXT NOT NULL DEFAULT (datetime('now')),
  reopened_by INTEGER,
  reopened_at TEXT,
  FOREIGN KEY (closed_by) REFERENCES users(id),
  FOREIGN KEY (reopened_by) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_day_closures_status ON day_closures(is_closed, closure_date);

CREATE TABLE IF NOT EXISTS day_reconciliations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  business_date TEXT NOT NULL UNIQUE,
  expected_cash REAL NOT NULL DEFAULT 0,
  expected_bank REAL NOT NULL DEFAULT 0,
  expected_ewallet REAL NOT NULL DEFAULT 0,
  expected_total REAL NOT NULL DEFAULT 0,
  deducted_from_collection REAL NOT NULL DEFAULT 0,
  expected_net REAL NOT NULL DEFAULT 0,
  actual_cash REAL NOT NULL DEFAULT 0,
  actual_bank REAL NOT NULL DEFAULT 0,
  actual_ewallet REAL NOT NULL DEFAULT 0,
  actual_total REAL NOT NULL DEFAULT 0,
  difference_total REAL NOT NULL DEFAULT 0,
  note TEXT,
  reconciled_by INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (reconciled_by) REFERENCES users(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_day_reconciliations_date ON day_reconciliations(business_date);

CREATE TABLE IF NOT EXISTS doc_number_sequences (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  doc_type TEXT NOT NULL,
  fiscal_year TEXT NOT NULL,
  next_value INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  UNIQUE(doc_type, fiscal_year)
);

CREATE INDEX IF NOT EXISTS idx_doc_number_sequences_type_year ON doc_number_sequences(doc_type, fiscal_year);

CREATE TABLE IF NOT EXISTS archived_records (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_table TEXT NOT NULL,
  source_id TEXT,
  source_date TEXT,
  payload TEXT NOT NULL,
  archived_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_archived_records_source ON archived_records(source_table);
CREATE INDEX IF NOT EXISTS idx_archived_records_source_date ON archived_records(source_table, source_date);
CREATE INDEX IF NOT EXISTS idx_archived_records_archived_at ON archived_records(archived_at);

CREATE TABLE IF NOT EXISTS archive_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_at TEXT NOT NULL DEFAULT (datetime('now')),
  retention_days INTEGER NOT NULL DEFAULT 365,
  archived_count INTEGER NOT NULL DEFAULT 0,
  details TEXT
);

CREATE INDEX IF NOT EXISTS idx_archive_runs_run_at ON archive_runs(run_at);
`);

ensureHybridSyncSchema();

const documentTypeListSql = "'CITIZENSHIP','LICENSE','PASSPORT','PAN','NATIONAL_ID','VOTER_CARD','OTHERS'";

const staffDocumentsTableDef = db.prepare(
  "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'staff_documents'"
).get();
if (staffDocumentsTableDef && staffDocumentsTableDef.sql && !staffDocumentsTableDef.sql.includes("'PAN'")) {
  db.exec("PRAGMA foreign_keys = OFF;");
  db.exec(`
    BEGIN;
    ALTER TABLE staff_documents RENAME TO staff_documents_old;
    CREATE TABLE staff_documents (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      staff_id INTEGER NOT NULL,
      doc_type TEXT NOT NULL CHECK (doc_type IN (${documentTypeListSql})),
      front_path TEXT,
      back_path TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      FOREIGN KEY (staff_id) REFERENCES staff(id) ON DELETE CASCADE
    );
    INSERT INTO staff_documents (id, staff_id, doc_type, front_path, back_path, created_at)
    SELECT id, staff_id, doc_type, front_path, back_path, created_at
    FROM staff_documents_old;
    DROP TABLE staff_documents_old;
    COMMIT;
  `);
  db.exec("PRAGMA foreign_keys = ON;");
}

const workerDocumentsTableDef = db.prepare(
  "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'worker_documents'"
).get();
if (workerDocumentsTableDef && workerDocumentsTableDef.sql && !workerDocumentsTableDef.sql.includes("'PAN'")) {
  db.exec("PRAGMA foreign_keys = OFF;");
  db.exec(`
    BEGIN;
    ALTER TABLE worker_documents RENAME TO worker_documents_old;
    CREATE TABLE worker_documents (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      worker_id INTEGER NOT NULL UNIQUE,
      doc_type TEXT CHECK (doc_type IN (${documentTypeListSql})),
      photo_path TEXT,
      front_path TEXT,
      back_path TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now')),
      FOREIGN KEY (worker_id) REFERENCES users(id) ON DELETE CASCADE
    );
    INSERT INTO worker_documents (id, worker_id, doc_type, photo_path, front_path, back_path, created_at, updated_at)
    SELECT id, worker_id, doc_type, photo_path, front_path, back_path, created_at, COALESCE(updated_at, created_at, datetime('now'))
    FROM worker_documents_old;
    DROP TABLE worker_documents_old;
    COMMIT;
  `);
  db.exec("PRAGMA foreign_keys = ON;");
}
db.exec("CREATE INDEX IF NOT EXISTS idx_worker_documents_worker ON worker_documents(worker_id);");

const userColumns = new Set(
  db.prepare("PRAGMA table_info(users)").all().map((col) => col.name)
);
if (!userColumns.has("start_date")) {
  db.exec("ALTER TABLE users ADD COLUMN start_date TEXT;");
}
if (!userColumns.has("monthly_salary")) {
  db.exec("ALTER TABLE users ADD COLUMN monthly_salary REAL NOT NULL DEFAULT 0;");
}
if (!userColumns.has("is_active")) {
  db.exec("ALTER TABLE users ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1;");
}
if (!userColumns.has("deactivated_at")) {
  db.exec("ALTER TABLE users ADD COLUMN deactivated_at TEXT;");
}
if (!userColumns.has("deactivated_by")) {
  db.exec("ALTER TABLE users ADD COLUMN deactivated_by INTEGER;");
}
if (!userColumns.has("fingerprint_id")) {
  db.exec("ALTER TABLE users ADD COLUMN fingerprint_id TEXT;");
}
if (!userColumns.has("theme_preset")) {
  db.exec("ALTER TABLE users ADD COLUMN theme_preset TEXT NOT NULL DEFAULT 'classic';");
}
db.exec("UPDATE users SET theme_preset = 'classic' WHERE theme_preset IS NULL OR TRIM(theme_preset) = '';");
db.exec("CREATE INDEX IF NOT EXISTS idx_users_fingerprint ON users(fingerprint_id);");

const creditColumns = new Set(
  db.prepare("PRAGMA table_info(credits)").all().map((col) => col.name)
);
if (!creditColumns.has("paid")) {
  db.exec("ALTER TABLE credits ADD COLUMN paid INTEGER NOT NULL DEFAULT 0;");
}
if (!creditColumns.has("paid_amount")) {
  db.exec("ALTER TABLE credits ADD COLUMN paid_amount REAL NOT NULL DEFAULT 0;");
  db.exec("UPDATE credits SET paid_amount = amount WHERE paid = 1;");
}
if (!creditColumns.has("payment_method")) {
  db.exec("ALTER TABLE credits ADD COLUMN payment_method TEXT NOT NULL DEFAULT 'CASH';");
}
if (!creditColumns.has("receipt_no")) {
  db.exec("ALTER TABLE credits ADD COLUMN receipt_no TEXT;");
}
if (!creditColumns.has("export_id")) {
  db.exec("ALTER TABLE credits ADD COLUMN export_id INTEGER;");
}
if (!creditColumns.has("trip_date")) {
  db.exec("ALTER TABLE credits ADD COLUMN trip_date TEXT;");
}
if (!creditColumns.has("checked_by_staff_id")) {
  db.exec("ALTER TABLE credits ADD COLUMN checked_by_staff_id INTEGER;");
}
if (!creditColumns.has("force_wash_required")) {
  db.exec("ALTER TABLE credits ADD COLUMN force_wash_required INTEGER NOT NULL DEFAULT 0;");
}
if (!creditColumns.has("credit_jar_containers")) {
  db.exec("ALTER TABLE credits ADD COLUMN credit_jar_containers INTEGER NOT NULL DEFAULT 0;");
}
if (!creditColumns.has("jar_price")) {
  db.exec("ALTER TABLE credits ADD COLUMN jar_price REAL NOT NULL DEFAULT 0;");
}
if (!creditColumns.has("bottle_case_price")) {
  db.exec("ALTER TABLE credits ADD COLUMN bottle_case_price REAL NOT NULL DEFAULT 0;");
}
if (!creditColumns.has("credit_dispensers")) {
  db.exec("ALTER TABLE credits ADD COLUMN credit_dispensers INTEGER NOT NULL DEFAULT 0;");
}
if (!creditColumns.has("dispenser_price")) {
  db.exec("ALTER TABLE credits ADD COLUMN dispenser_price REAL NOT NULL DEFAULT 0;");
}
if (!creditColumns.has("jar_container_price")) {
  db.exec("ALTER TABLE credits ADD COLUMN jar_container_price REAL NOT NULL DEFAULT 0;");
}
if (!creditColumns.has("customer_phone")) {
  db.exec("ALTER TABLE credits ADD COLUMN customer_phone TEXT;");
}
if (!creditColumns.has("customer_location")) {
  db.exec("ALTER TABLE credits ADD COLUMN customer_location TEXT;");
}
if (!creditColumns.has("customer_id")) {
  db.exec("ALTER TABLE credits ADD COLUMN customer_id INTEGER;");
}
db.exec("UPDATE credits SET payment_method = 'CASH' WHERE payment_method IS NULL OR TRIM(payment_method) = '';");
db.exec("UPDATE credits SET payment_method = 'CASH' WHERE payment_method NOT IN ('CASH','BANK','E_WALLET');");
db.exec("CREATE INDEX IF NOT EXISTS idx_credits_payment_method ON credits(payment_method);");
db.exec("CREATE INDEX IF NOT EXISTS idx_credits_customer_id ON credits(customer_id);");

const normalizeCustomerText = (value) => String(value || "").trim();
const normalizeCustomerKey = (value) => normalizeCustomerText(value).toLowerCase();
const buildCustomerLookupEntry = (row) => ({
  id: Number(row.id),
  name: normalizeCustomerText(row.name),
  phone: normalizeCustomerText(row.phone),
  location: normalizeCustomerText(row.location),
  nameKey: normalizeCustomerKey(row.name),
  phoneKey: normalizeCustomerKey(row.phone),
  locationKey: normalizeCustomerKey(row.location)
});
const customerById = new Map();
const customersByName = new Map();
const removeCustomerLookup = (entry) => {
  if (!entry || !entry.id) return;
  const existing = customerById.get(entry.id);
  if (!existing) return;
  const bucket = customersByName.get(existing.nameKey);
  if (bucket) {
    const nextBucket = bucket.filter((row) => row.id !== existing.id);
    if (nextBucket.length) {
      customersByName.set(existing.nameKey, nextBucket);
    } else {
      customersByName.delete(existing.nameKey);
    }
  }
  customerById.delete(existing.id);
};
const registerCustomerLookup = (row) => {
  const entry = buildCustomerLookupEntry(row);
  if (!entry.id || !entry.nameKey) return null;
  removeCustomerLookup(entry);
  customerById.set(entry.id, entry);
  if (!customersByName.has(entry.nameKey)) {
    customersByName.set(entry.nameKey, []);
  }
  customersByName.get(entry.nameKey).push(entry);
  return entry;
};
db.prepare("SELECT id, name, phone, location FROM customers ORDER BY id ASC").all().forEach((row) => {
  registerCustomerLookup(row);
});

const insertCustomerMaster = db.prepare(
  `INSERT INTO customers (name, phone, location, created_by)
   VALUES (?, ?, ?, ?)`
);
const updateCustomerMasterContact = db.prepare(
  `UPDATE customers
   SET name = ?, phone = ?, location = ?, updated_at = datetime('now')
   WHERE id = ?`
);
const linkCreditCustomer = db.prepare(
  `UPDATE credits
   SET customer_id = ?,
       customer_name = ?,
       customer_phone = ?,
       customer_location = ?
   WHERE id = ?`
);
const getCustomerByIdStmt = db.prepare(
  "SELECT id, name, phone, location FROM customers WHERE id = ?"
);

const chooseCustomerMatch = ({ name, phone, location }) => {
  const nameKey = normalizeCustomerKey(name);
  if (!nameKey) return null;
  const candidates = customersByName.get(nameKey) || [];
  if (!candidates.length) return null;
  const phoneKey = normalizeCustomerKey(phone);
  const locationKey = normalizeCustomerKey(location);
  const strict = candidates.find((candidate) => {
    if (phoneKey && candidate.phoneKey !== phoneKey) return false;
    if (locationKey && candidate.locationKey !== locationKey) return false;
    return true;
  });
  if (strict) return strict;
  if (phoneKey) {
    const phoneMatch = candidates.find((candidate) => candidate.phoneKey === phoneKey);
    if (phoneMatch) return phoneMatch;
  }
  if (locationKey) {
    const locationMatch = candidates.find((candidate) => candidate.locationKey === locationKey);
    if (locationMatch) return locationMatch;
  }
  if (candidates.length === 1) return candidates[0];
  return candidates[0] || null;
};

const ensureCustomerMasterRow = ({ customerId, name, phone, location, createdBy }) => {
  const safeName = normalizeCustomerText(name);
  if (!safeName) return null;
  const safePhone = normalizeCustomerText(phone);
  const safeLocation = normalizeCustomerText(location);

  let customer = customerId ? customerById.get(Number(customerId)) || null : null;
  if (!customer) {
    customer = chooseCustomerMatch({ name: safeName, phone: safePhone, location: safeLocation });
  }
  if (!customer) {
    const result = insertCustomerMaster.run(
      safeName,
      safePhone || null,
      safeLocation || null,
      createdBy || null
    );
    customer = registerCustomerLookup({
      id: Number(result.lastInsertRowid || 0),
      name: safeName,
      phone: safePhone || null,
      location: safeLocation || null
    });
  } else {
    const nextName = customer.name || safeName;
    const nextPhone = safePhone || customer.phone || "";
    const nextLocation = safeLocation || customer.location || "";
    if (nextName !== customer.name || nextPhone !== customer.phone || nextLocation !== customer.location) {
      updateCustomerMasterContact.run(
        nextName,
        nextPhone || null,
        nextLocation || null,
        customer.id
      );
      customer = registerCustomerLookup(
        getCustomerByIdStmt.get(customer.id)
      ) || customer;
    }
  }
  return customer;
};

db.prepare(
  `SELECT id, customer_id, customer_name, customer_phone, customer_location, created_by
   FROM credits
   WHERE TRIM(COALESCE(customer_name, '')) <> ''
   ORDER BY id ASC`
).all().forEach((row) => {
  const customer = ensureCustomerMasterRow({
    customerId: row.customer_id,
    name: row.customer_name,
    phone: row.customer_phone,
    location: row.customer_location,
    createdBy: row.created_by
  });
  if (!customer) return;
  linkCreditCustomer.run(
    customer.id,
    customer.name,
    customer.phone || null,
    customer.location || null,
    row.id
  );
});

const importColumns = new Set(
  db.prepare("PRAGMA table_info(import_entries)").all().map((col) => col.name)
);

const creditPaymentColumns = new Set(
  db.prepare("PRAGMA table_info(credit_payments)").all().map((col) => col.name)
);
if (creditPaymentColumns.size > 0 && !creditPaymentColumns.has("payment_method")) {
  db.exec("ALTER TABLE credit_payments ADD COLUMN payment_method TEXT NOT NULL DEFAULT 'CASH';");
}
if (creditPaymentColumns.size > 0 && !creditPaymentColumns.has("receipt_no")) {
  db.exec("ALTER TABLE credit_payments ADD COLUMN receipt_no TEXT;");
}
db.exec("UPDATE credit_payments SET payment_method = 'CASH' WHERE payment_method IS NULL OR TRIM(payment_method) = '';");
db.exec("UPDATE credit_payments SET payment_method = 'CASH' WHERE payment_method NOT IN ('CASH','BANK','E_WALLET');");
db.exec("CREATE INDEX IF NOT EXISTS idx_credit_payments_method ON credit_payments(payment_method);");
db.exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_credit_payments_receipt_no ON credit_payments(receipt_no);");

const importPaymentColumns = new Set(
  db.prepare("PRAGMA table_info(import_payments)").all().map((col) => col.name)
);
if (importPaymentColumns.size > 0 && !importPaymentColumns.has("payment_method")) {
  db.exec("ALTER TABLE import_payments ADD COLUMN payment_method TEXT NOT NULL DEFAULT 'CASH';");
}
if (importPaymentColumns.size > 0 && !importPaymentColumns.has("payment_source")) {
  db.exec("ALTER TABLE import_payments ADD COLUMN payment_source TEXT NOT NULL DEFAULT 'DAILY_COLLECTION';");
}
if (importPaymentColumns.size > 0 && !importPaymentColumns.has("receipt_no")) {
  db.exec("ALTER TABLE import_payments ADD COLUMN receipt_no TEXT;");
}
db.exec("UPDATE import_payments SET payment_method = 'CASH' WHERE payment_method IS NULL OR TRIM(payment_method) = '';");
db.exec("UPDATE import_payments SET payment_method = 'CASH' WHERE payment_method NOT IN ('CASH','BANK','E_WALLET');");
db.exec("UPDATE import_payments SET payment_source = 'DAILY_COLLECTION' WHERE payment_source IS NULL OR TRIM(payment_source) = '';");
db.exec("UPDATE import_payments SET payment_source = 'DAILY_COLLECTION' WHERE payment_source NOT IN ('DAILY_COLLECTION','OWNER_PERSONAL','BANK_OTHER');");
db.exec("CREATE INDEX IF NOT EXISTS idx_import_payments_method ON import_payments(payment_method);");
db.exec("CREATE INDEX IF NOT EXISTS idx_import_payments_source ON import_payments(payment_source);");
db.exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_import_payments_receipt_no ON import_payments(receipt_no);");
if (!importColumns.has("direction")) {
  db.exec("ALTER TABLE import_entries ADD COLUMN direction TEXT NOT NULL DEFAULT 'IN';");
  db.exec("UPDATE import_entries SET direction = 'IN' WHERE direction IS NULL;");
}
if (!importColumns.has("jar_type_id")) {
  db.exec("ALTER TABLE import_entries ADD COLUMN jar_type_id INTEGER;");
}
if (!importColumns.has("jar_cap_type_id")) {
  db.exec("ALTER TABLE import_entries ADD COLUMN jar_cap_type_id INTEGER;");
}
if (!importColumns.has("seller_name")) {
  db.exec("ALTER TABLE import_entries ADD COLUMN seller_name TEXT;");
}
if (!importColumns.has("total_amount")) {
  db.exec("ALTER TABLE import_entries ADD COLUMN total_amount REAL NOT NULL DEFAULT 0;");
}
if (!importColumns.has("unit_price")) {
  db.exec("ALTER TABLE import_entries ADD COLUMN unit_price REAL NOT NULL DEFAULT 0;");
  db.exec(
    `UPDATE import_entries
     SET unit_price = CASE
       WHEN COALESCE(quantity, 0) > 0 THEN ROUND(COALESCE(total_amount, 0) / quantity, 6)
       ELSE 0
     END`
  );
}
if (!importColumns.has("paid_amount")) {
  db.exec("ALTER TABLE import_entries ADD COLUMN paid_amount REAL NOT NULL DEFAULT 0;");
}
if (!importColumns.has("is_credit")) {
  db.exec("ALTER TABLE import_entries ADD COLUMN is_credit INTEGER NOT NULL DEFAULT 0;");
}
if (!importColumns.has("due_date")) {
  db.exec("ALTER TABLE import_entries ADD COLUMN due_date TEXT;");
}
if (!importColumns.has("reminder_days")) {
  db.exec("ALTER TABLE import_entries ADD COLUMN reminder_days INTEGER NOT NULL DEFAULT 0;");
}
db.exec("UPDATE import_entries SET reminder_days = 0 WHERE reminder_days IS NULL OR reminder_days < 0;");
db.exec("CREATE INDEX IF NOT EXISTS idx_import_entries_seller ON import_entries(seller_name);");

db.exec(
  `CREATE TABLE IF NOT EXISTS import_payments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    import_entry_id INTEGER NOT NULL,
    payment_date TEXT NOT NULL,
    amount REAL NOT NULL DEFAULT 0,
    payment_method TEXT NOT NULL DEFAULT 'CASH',
    payment_source TEXT NOT NULL DEFAULT 'DAILY_COLLECTION',
    note TEXT,
    created_by INTEGER,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (import_entry_id) REFERENCES import_entries(id) ON DELETE CASCADE,
    FOREIGN KEY (created_by) REFERENCES users(id)
  )`
);
db.exec("CREATE INDEX IF NOT EXISTS idx_import_payments_entry ON import_payments(import_entry_id);");
db.exec("CREATE INDEX IF NOT EXISTS idx_import_payments_date ON import_payments(payment_date);");

const companyPurchaseColumns = new Set(
  db.prepare("PRAGMA table_info(company_purchases)").all().map((col) => col.name)
);
if (!companyPurchaseColumns.has("expense_category")) {
  db.exec("ALTER TABLE company_purchases ADD COLUMN expense_category TEXT NOT NULL DEFAULT 'GENERAL';");
}
if (!companyPurchaseColumns.has("seller_name")) {
  db.exec("ALTER TABLE company_purchases ADD COLUMN seller_name TEXT;");
}
if (!companyPurchaseColumns.has("paid_amount")) {
  db.exec("ALTER TABLE company_purchases ADD COLUMN paid_amount REAL NOT NULL DEFAULT 0;");
  db.exec("UPDATE company_purchases SET paid_amount = amount WHERE paid_amount IS NULL OR paid_amount = 0;");
}
if (!companyPurchaseColumns.has("is_credit")) {
  db.exec("ALTER TABLE company_purchases ADD COLUMN is_credit INTEGER NOT NULL DEFAULT 0;");
}
if (!companyPurchaseColumns.has("due_date")) {
  db.exec("ALTER TABLE company_purchases ADD COLUMN due_date TEXT;");
}
if (!companyPurchaseColumns.has("reminder_days")) {
  db.exec("ALTER TABLE company_purchases ADD COLUMN reminder_days INTEGER NOT NULL DEFAULT 0;");
}
db.exec("UPDATE company_purchases SET expense_category = 'GENERAL' WHERE expense_category IS NULL OR TRIM(expense_category) = '';");
db.exec("UPDATE company_purchases SET expense_category = 'GENERAL' WHERE expense_category NOT IN ('GENERAL','LUNCH','LAND_RENT','UTILITIES','OTHER');");
db.exec("UPDATE company_purchases SET reminder_days = 0 WHERE reminder_days IS NULL OR reminder_days < 0;");
db.exec("CREATE INDEX IF NOT EXISTS idx_company_purchases_seller ON company_purchases(seller_name);");
db.exec("CREATE INDEX IF NOT EXISTS idx_company_purchases_category ON company_purchases(expense_category);");

db.exec(
  `CREATE TABLE IF NOT EXISTS company_purchase_payments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_purchase_id INTEGER NOT NULL,
    payment_date TEXT NOT NULL,
    amount REAL NOT NULL DEFAULT 0,
    payment_method TEXT NOT NULL DEFAULT 'CASH',
    payment_source TEXT NOT NULL DEFAULT 'DAILY_COLLECTION',
    note TEXT,
    created_by INTEGER,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (company_purchase_id) REFERENCES company_purchases(id) ON DELETE CASCADE,
    FOREIGN KEY (created_by) REFERENCES users(id)
  )`
);
db.exec("CREATE INDEX IF NOT EXISTS idx_company_purchase_payments_purchase ON company_purchase_payments(company_purchase_id);");
db.exec("CREATE INDEX IF NOT EXISTS idx_company_purchase_payments_date ON company_purchase_payments(payment_date);");
const companyPurchasePaymentColumns = new Set(
  db.prepare("PRAGMA table_info(company_purchase_payments)").all().map((col) => col.name)
);
if (companyPurchasePaymentColumns.size > 0 && !companyPurchasePaymentColumns.has("payment_method")) {
  db.exec("ALTER TABLE company_purchase_payments ADD COLUMN payment_method TEXT NOT NULL DEFAULT 'CASH';");
}
if (companyPurchasePaymentColumns.size > 0 && !companyPurchasePaymentColumns.has("payment_source")) {
  db.exec("ALTER TABLE company_purchase_payments ADD COLUMN payment_source TEXT NOT NULL DEFAULT 'DAILY_COLLECTION';");
}
if (companyPurchasePaymentColumns.size > 0 && !companyPurchasePaymentColumns.has("receipt_no")) {
  db.exec("ALTER TABLE company_purchase_payments ADD COLUMN receipt_no TEXT;");
}
db.exec("UPDATE company_purchase_payments SET payment_method = 'CASH' WHERE payment_method IS NULL OR TRIM(payment_method) = '';");
db.exec("UPDATE company_purchase_payments SET payment_method = 'CASH' WHERE payment_method NOT IN ('CASH','BANK','E_WALLET');");
db.exec("UPDATE company_purchase_payments SET payment_source = 'DAILY_COLLECTION' WHERE payment_source IS NULL OR TRIM(payment_source) = '';");
db.exec("UPDATE company_purchase_payments SET payment_source = 'DAILY_COLLECTION' WHERE payment_source NOT IN ('DAILY_COLLECTION','OWNER_PERSONAL','BANK_OTHER');");
db.exec("CREATE INDEX IF NOT EXISTS idx_company_purchase_payments_method ON company_purchase_payments(payment_method);");
db.exec("CREATE INDEX IF NOT EXISTS idx_company_purchase_payments_source ON company_purchase_payments(payment_source);");
db.exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_company_purchase_payments_receipt_no ON company_purchase_payments(receipt_no);");

const vehicleExpenseTable = db.prepare(
  "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'vehicle_expenses'"
).get();
if (vehicleExpenseTable && vehicleExpenseTable.sql && !vehicleExpenseTable.sql.includes("'OTHER'")) {
  db.exec("PRAGMA foreign_keys = OFF;");
  db.exec(`
    BEGIN;
    ALTER TABLE vehicle_expenses RENAME TO vehicle_expenses_old;
    CREATE TABLE vehicle_expenses (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      vehicle_id INTEGER NOT NULL,
      expense_date TEXT NOT NULL,
      expense_type TEXT NOT NULL CHECK (expense_type IN ('FUEL','REPAIR','SERVICE','OTHER')),
      amount REAL NOT NULL DEFAULT 0,
      paid_amount REAL NOT NULL DEFAULT 0,
      is_credit INTEGER NOT NULL DEFAULT 0,
      note TEXT,
      created_by INTEGER,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now')),
      FOREIGN KEY (vehicle_id) REFERENCES vehicles(id) ON DELETE CASCADE,
      FOREIGN KEY (created_by) REFERENCES users(id)
    );
    INSERT INTO vehicle_expenses (id, vehicle_id, expense_date, expense_type, amount, paid_amount, is_credit, note, created_by, created_at, updated_at)
    SELECT id, vehicle_id, expense_date, expense_type, amount, amount, 0, note, created_by, created_at, updated_at
    FROM vehicle_expenses_old;
    DROP TABLE vehicle_expenses_old;
    COMMIT;
  `);
  db.exec("PRAGMA foreign_keys = ON;");
  db.exec("CREATE INDEX IF NOT EXISTS idx_vehicle_expenses_date ON vehicle_expenses(expense_date);");
  db.exec("CREATE INDEX IF NOT EXISTS idx_vehicle_expenses_vehicle ON vehicle_expenses(vehicle_id);");
  db.exec("CREATE INDEX IF NOT EXISTS idx_vehicle_expenses_type ON vehicle_expenses(expense_type);");
}

const vehicleExpenseColumns = new Set(
  db.prepare("PRAGMA table_info(vehicle_expenses)").all().map((col) => col.name)
);
if (!vehicleExpenseColumns.has("export_id")) {
  db.exec("ALTER TABLE vehicle_expenses ADD COLUMN export_id INTEGER;");
}
if (!vehicleExpenseColumns.has("paid_amount")) {
  db.exec("ALTER TABLE vehicle_expenses ADD COLUMN paid_amount REAL NOT NULL DEFAULT 0;");
  db.exec("UPDATE vehicle_expenses SET paid_amount = amount WHERE paid_amount IS NULL OR paid_amount = 0;");
}
if (!vehicleExpenseColumns.has("is_credit")) {
  db.exec("ALTER TABLE vehicle_expenses ADD COLUMN is_credit INTEGER NOT NULL DEFAULT 0;");
}
db.exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_vehicle_expenses_export ON vehicle_expenses(export_id);");

db.exec(
  `CREATE TABLE IF NOT EXISTS vehicle_expense_payments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    vehicle_expense_id INTEGER NOT NULL,
    payment_date TEXT NOT NULL,
    amount REAL NOT NULL DEFAULT 0,
    payment_method TEXT NOT NULL DEFAULT 'CASH',
    payment_source TEXT NOT NULL DEFAULT 'DAILY_COLLECTION',
    note TEXT,
    created_by INTEGER,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (vehicle_expense_id) REFERENCES vehicle_expenses(id) ON DELETE CASCADE,
    FOREIGN KEY (created_by) REFERENCES users(id)
  )`
);
db.exec("CREATE INDEX IF NOT EXISTS idx_vehicle_expense_payments_entry ON vehicle_expense_payments(vehicle_expense_id);");
db.exec("CREATE INDEX IF NOT EXISTS idx_vehicle_expense_payments_date ON vehicle_expense_payments(payment_date);");
const vehicleExpensePaymentColumns = new Set(
  db.prepare("PRAGMA table_info(vehicle_expense_payments)").all().map((col) => col.name)
);
if (vehicleExpensePaymentColumns.size > 0 && !vehicleExpensePaymentColumns.has("payment_method")) {
  db.exec("ALTER TABLE vehicle_expense_payments ADD COLUMN payment_method TEXT NOT NULL DEFAULT 'CASH';");
}
if (vehicleExpensePaymentColumns.size > 0 && !vehicleExpensePaymentColumns.has("payment_source")) {
  db.exec("ALTER TABLE vehicle_expense_payments ADD COLUMN payment_source TEXT NOT NULL DEFAULT 'DAILY_COLLECTION';");
}
if (vehicleExpensePaymentColumns.size > 0 && !vehicleExpensePaymentColumns.has("receipt_no")) {
  db.exec("ALTER TABLE vehicle_expense_payments ADD COLUMN receipt_no TEXT;");
}
db.exec("UPDATE vehicle_expense_payments SET payment_method = 'CASH' WHERE payment_method IS NULL OR TRIM(payment_method) = '';");
db.exec("UPDATE vehicle_expense_payments SET payment_method = 'CASH' WHERE payment_method NOT IN ('CASH','BANK','E_WALLET');");
db.exec("UPDATE vehicle_expense_payments SET payment_source = 'DAILY_COLLECTION' WHERE payment_source IS NULL OR TRIM(payment_source) = '';");
db.exec("UPDATE vehicle_expense_payments SET payment_source = 'DAILY_COLLECTION' WHERE payment_source NOT IN ('DAILY_COLLECTION','OWNER_PERSONAL','BANK_OTHER');");
db.exec("CREATE INDEX IF NOT EXISTS idx_vehicle_expense_payments_method ON vehicle_expense_payments(payment_method);");
db.exec("CREATE INDEX IF NOT EXISTS idx_vehicle_expense_payments_source ON vehicle_expense_payments(payment_source);");
db.exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_vehicle_expense_payments_receipt_no ON vehicle_expense_payments(receipt_no);");

const jarTypeColumns = new Set(
  db.prepare("PRAGMA table_info(jar_types)").all().map((col) => col.name)
);
if (!jarTypeColumns.has("default_qty")) {
  db.exec("ALTER TABLE jar_types ADD COLUMN default_qty INTEGER NOT NULL DEFAULT 0;");
}

const jarCapColumns = new Set(
  db.prepare("PRAGMA table_info(jar_cap_types)").all().map((col) => col.name)
);
if (jarCapColumns.size > 0 && !jarCapColumns.has("default_qty")) {
  db.exec("ALTER TABLE jar_cap_types ADD COLUMN default_qty INTEGER NOT NULL DEFAULT 0;");
}

const staffColumns = new Set(
  db.prepare("PRAGMA table_info(staff)").all().map((col) => col.name)
);
if (staffColumns.size > 0 && !staffColumns.has("staff_role")) {
  db.exec("ALTER TABLE staff ADD COLUMN staff_role TEXT;");
}
if (staffColumns.size > 0 && !staffColumns.has("fingerprint_id")) {
  db.exec("ALTER TABLE staff ADD COLUMN fingerprint_id TEXT;");
}
if (staffColumns.size > 0 && !staffColumns.has("is_active")) {
  db.exec("ALTER TABLE staff ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1;");
}
if (staffColumns.size > 0 && !staffColumns.has("deactivated_at")) {
  db.exec("ALTER TABLE staff ADD COLUMN deactivated_at TEXT;");
}
if (staffColumns.size > 0 && !staffColumns.has("deactivated_by")) {
  db.exec("ALTER TABLE staff ADD COLUMN deactivated_by INTEGER;");
}
db.exec("CREATE INDEX IF NOT EXISTS idx_staff_fingerprint ON staff(fingerprint_id);");
db.exec("CREATE INDEX IF NOT EXISTS idx_staff_active ON staff(is_active);");

const staffRoleColumns = new Set(
  db.prepare("PRAGMA table_info(staff_roles)").all().map((col) => col.name)
);
if (staffRoleColumns.size > 0 && !staffRoleColumns.has("show_in_exports")) {
  db.exec("ALTER TABLE staff_roles ADD COLUMN show_in_exports INTEGER NOT NULL DEFAULT 1;");
}

const exportColumns = new Set(
  db.prepare("PRAGMA table_info(exports)").all().map((col) => col.name)
);
if (!exportColumns.has("total_amount")) {
  db.exec("ALTER TABLE exports ADD COLUMN total_amount REAL NOT NULL DEFAULT 0;");
}
if (!exportColumns.has("paid_amount")) {
  db.exec("ALTER TABLE exports ADD COLUMN paid_amount REAL NOT NULL DEFAULT 0;");
}
if (!exportColumns.has("payment_method")) {
  db.exec("ALTER TABLE exports ADD COLUMN payment_method TEXT NOT NULL DEFAULT 'CASH';");
}
if (!exportColumns.has("paid_cash_amount")) {
  db.exec("ALTER TABLE exports ADD COLUMN paid_cash_amount REAL NOT NULL DEFAULT 0;");
}
if (!exportColumns.has("paid_bank_amount")) {
  db.exec("ALTER TABLE exports ADD COLUMN paid_bank_amount REAL NOT NULL DEFAULT 0;");
}
if (!exportColumns.has("paid_ewallet_amount")) {
  db.exec("ALTER TABLE exports ADD COLUMN paid_ewallet_amount REAL NOT NULL DEFAULT 0;");
}
if (!exportColumns.has("credit_amount")) {
  db.exec("ALTER TABLE exports ADD COLUMN credit_amount REAL NOT NULL DEFAULT 0;");
}
if (!exportColumns.has("return_jar_count")) {
  db.exec("ALTER TABLE exports ADD COLUMN return_jar_count INTEGER NOT NULL DEFAULT 0;");
}
if (!exportColumns.has("jar_container_given_count")) {
  db.exec("ALTER TABLE exports ADD COLUMN jar_container_given_count INTEGER NOT NULL DEFAULT 0;");
}
if (!exportColumns.has("return_bottle_case_count")) {
  db.exec("ALTER TABLE exports ADD COLUMN return_bottle_case_count INTEGER NOT NULL DEFAULT 0;");
}
if (!exportColumns.has("damaged_bottle_case_count")) {
  db.exec("ALTER TABLE exports ADD COLUMN damaged_bottle_case_count INTEGER NOT NULL DEFAULT 0;");
}
if (!exportColumns.has("leakage_jar_count")) {
  db.exec("ALTER TABLE exports ADD COLUMN leakage_jar_count INTEGER NOT NULL DEFAULT 0;");
}
if (!exportColumns.has("sold_jar_count")) {
  db.exec("ALTER TABLE exports ADD COLUMN sold_jar_count INTEGER NOT NULL DEFAULT 0;");
}
if (!exportColumns.has("sold_jar_price")) {
  db.exec("ALTER TABLE exports ADD COLUMN sold_jar_price REAL NOT NULL DEFAULT 0;");
}
if (!exportColumns.has("sold_jar_amount")) {
  db.exec("ALTER TABLE exports ADD COLUMN sold_jar_amount REAL NOT NULL DEFAULT 0;");
}
if (!exportColumns.has("collection_amount")) {
  db.exec("ALTER TABLE exports ADD COLUMN collection_amount REAL NOT NULL DEFAULT 0;");
}
if (!exportColumns.has("expense_amount")) {
  db.exec("ALTER TABLE exports ADD COLUMN expense_amount REAL NOT NULL DEFAULT 0;");
}
if (!exportColumns.has("expense_note")) {
  db.exec("ALTER TABLE exports ADD COLUMN expense_note TEXT;");
}
if (!exportColumns.has("route")) {
  db.exec("ALTER TABLE exports ADD COLUMN route TEXT;");
}
if (!exportColumns.has("receipt_no")) {
  db.exec("ALTER TABLE exports ADD COLUMN receipt_no TEXT;");
}
if (!exportColumns.has("jar_unit_price")) {
  db.exec("ALTER TABLE exports ADD COLUMN jar_unit_price REAL NOT NULL DEFAULT 0;");
}
if (!exportColumns.has("bottle_case_unit_price")) {
  db.exec("ALTER TABLE exports ADD COLUMN bottle_case_unit_price REAL NOT NULL DEFAULT 0;");
}
if (!exportColumns.has("dispenser_count")) {
  db.exec("ALTER TABLE exports ADD COLUMN dispenser_count INTEGER NOT NULL DEFAULT 0;");
}
if (!exportColumns.has("dispenser_unit_price")) {
  db.exec("ALTER TABLE exports ADD COLUMN dispenser_unit_price REAL NOT NULL DEFAULT 0;");
}
if (!exportColumns.has("checked_by_staff_id")) {
  db.exec("ALTER TABLE exports ADD COLUMN checked_by_staff_id INTEGER;");
}
if (!exportColumns.has("checked_by_staff_name")) {
  db.exec("ALTER TABLE exports ADD COLUMN checked_by_staff_name TEXT;");
}
if (!exportColumns.has("force_wash_required")) {
  db.exec("ALTER TABLE exports ADD COLUMN force_wash_required INTEGER NOT NULL DEFAULT 0;");
}
if (!exportColumns.has("force_wash_staff_name")) {
  db.exec("ALTER TABLE exports ADD COLUMN force_wash_staff_name TEXT;");
}
db.exec("CREATE INDEX IF NOT EXISTS idx_credits_export_id ON credits(export_id);");
db.exec("CREATE INDEX IF NOT EXISTS idx_credits_checked_staff ON credits(checked_by_staff_id);");
db.exec("CREATE INDEX IF NOT EXISTS idx_exports_checked_staff ON exports(checked_by_staff_id);");
db.exec("CREATE INDEX IF NOT EXISTS idx_staff_roles_show_in_exports ON staff_roles(show_in_exports);");
db.exec("UPDATE exports SET payment_method = 'CASH' WHERE payment_method IS NULL OR TRIM(payment_method) = '';");
db.exec("UPDATE exports SET payment_method = 'CASH' WHERE payment_method NOT IN ('CASH','BANK','E_WALLET','MIXED');");
db.exec("CREATE INDEX IF NOT EXISTS idx_exports_payment_method ON exports(payment_method);");
db.exec("UPDATE exports SET paid_cash_amount = 0 WHERE paid_cash_amount IS NULL OR paid_cash_amount < 0;");
db.exec("UPDATE exports SET paid_bank_amount = 0 WHERE paid_bank_amount IS NULL OR paid_bank_amount < 0;");
db.exec("UPDATE exports SET paid_ewallet_amount = 0 WHERE paid_ewallet_amount IS NULL OR paid_ewallet_amount < 0;");
db.exec(
  `UPDATE exports
   SET paid_cash_amount = CASE WHEN payment_method = 'CASH' THEN paid_amount ELSE 0 END,
       paid_bank_amount = CASE WHEN payment_method = 'BANK' THEN paid_amount ELSE 0 END,
       paid_ewallet_amount = CASE WHEN payment_method = 'E_WALLET' THEN paid_amount ELSE 0 END
   WHERE paid_amount > 0
     AND COALESCE(paid_cash_amount, 0) = 0
     AND COALESCE(paid_bank_amount, 0) = 0
     AND COALESCE(paid_ewallet_amount, 0) = 0`
);
db.exec(
  `UPDATE exports
   SET paid_amount = ROUND(COALESCE(paid_cash_amount, 0) + COALESCE(paid_bank_amount, 0) + COALESCE(paid_ewallet_amount, 0), 2)`
);
db.exec(
  `UPDATE exports
   SET credit_amount = CASE
     WHEN total_amount - paid_amount > 0 THEN total_amount - paid_amount
     ELSE 0
   END`
);

const staffSalaryColumns = new Set(
  db.prepare("PRAGMA table_info(staff_salary_payments)").all().map((col) => col.name)
);
if (staffSalaryColumns.size > 0 && !staffSalaryColumns.has("receipt_no")) {
  db.exec("ALTER TABLE staff_salary_payments ADD COLUMN receipt_no TEXT;");
}
if (staffSalaryColumns.size > 0 && !staffSalaryColumns.has("payment_source")) {
  db.exec("ALTER TABLE staff_salary_payments ADD COLUMN payment_source TEXT NOT NULL DEFAULT 'DAILY_COLLECTION';");
}
db.exec(
  "UPDATE staff_salary_payments SET payment_source = 'DAILY_COLLECTION' WHERE payment_source IS NULL OR TRIM(payment_source) = '';"
);
db.exec(
  "UPDATE staff_salary_payments SET payment_source = 'DAILY_COLLECTION' WHERE payment_source NOT IN ('DAILY_COLLECTION','OWNER_PERSONAL','BANK_OTHER');"
);
db.exec("CREATE INDEX IF NOT EXISTS idx_staff_salary_source ON staff_salary_payments(payment_source);");

const workerSalaryColumns = new Set(
  db.prepare("PRAGMA table_info(worker_salary_payments)").all().map((col) => col.name)
);
if (workerSalaryColumns.size > 0 && !workerSalaryColumns.has("receipt_no")) {
  db.exec("ALTER TABLE worker_salary_payments ADD COLUMN receipt_no TEXT;");
}
if (workerSalaryColumns.size > 0 && !workerSalaryColumns.has("payment_source")) {
  db.exec("ALTER TABLE worker_salary_payments ADD COLUMN payment_source TEXT NOT NULL DEFAULT 'DAILY_COLLECTION';");
}
db.exec(
  "UPDATE worker_salary_payments SET payment_source = 'DAILY_COLLECTION' WHERE payment_source IS NULL OR TRIM(payment_source) = '';"
);
db.exec(
  "UPDATE worker_salary_payments SET payment_source = 'DAILY_COLLECTION' WHERE payment_source NOT IN ('DAILY_COLLECTION','OWNER_PERSONAL','BANK_OTHER');"
);
db.exec("CREATE INDEX IF NOT EXISTS idx_worker_salary_source ON worker_salary_payments(payment_source);");

const vehicleSavingsColumns = new Set(
  db.prepare("PRAGMA table_info(vehicle_savings)").all().map((col) => col.name)
);
if (vehicleSavingsColumns.size > 0 && !vehicleSavingsColumns.has("export_id")) {
  db.exec("ALTER TABLE vehicle_savings ADD COLUMN export_id INTEGER;");
}
if (vehicleSavingsColumns.size > 0 && !vehicleSavingsColumns.has("payment_source")) {
  db.exec("ALTER TABLE vehicle_savings ADD COLUMN payment_source TEXT NOT NULL DEFAULT 'DAILY_COLLECTION';");
}
db.exec(
  "UPDATE vehicle_savings SET payment_source = 'DAILY_COLLECTION' WHERE payment_source IS NULL OR TRIM(payment_source) = '';"
);
db.exec(
  "UPDATE vehicle_savings SET payment_source = 'DAILY_COLLECTION' WHERE payment_source NOT IN ('DAILY_COLLECTION','OWNER_PERSONAL','BANK_OTHER');"
);
db.exec(
  "UPDATE vehicle_savings SET payment_source = 'DAILY_COLLECTION' WHERE amount >= 0;"
);
db.exec("CREATE INDEX IF NOT EXISTS idx_vehicle_savings_source ON vehicle_savings(payment_source);");
db.exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_vehicle_savings_export ON vehicle_savings(export_id);");
db.exec("UPDATE rent_entries SET payment_method = 'CASH' WHERE payment_method IS NULL OR TRIM(payment_method) = '';");
db.exec("UPDATE rent_entries SET payment_method = 'CASH' WHERE payment_method NOT IN ('CASH','BANK','E_WALLET');");
db.exec("UPDATE rent_entries SET add_to_collection = CASE WHEN add_to_collection = 0 THEN 0 ELSE 1 END;");

const vehicleColumns = new Set(
  db.prepare("PRAGMA table_info(vehicles)").all().map((col) => col.name)
);
if (vehicleColumns.size > 0 && !vehicleColumns.has("is_company")) {
  db.exec("ALTER TABLE vehicles ADD COLUMN is_company INTEGER NOT NULL DEFAULT 0;");
}
if (vehicleColumns.size > 0 && !vehicleColumns.has("is_active")) {
  db.exec("ALTER TABLE vehicles ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1;");
}
if (vehicleColumns.size > 0 && !vehicleColumns.has("deactivated_at")) {
  db.exec("ALTER TABLE vehicles ADD COLUMN deactivated_at TEXT;");
}
if (vehicleColumns.size > 0 && !vehicleColumns.has("deactivated_by")) {
  db.exec("ALTER TABLE vehicles ADD COLUMN deactivated_by INTEGER;");
}
if (vehicleColumns.size > 0 && !vehicleColumns.has("is_system")) {
  db.exec("ALTER TABLE vehicles ADD COLUMN is_system INTEGER NOT NULL DEFAULT 0;");
}
db.exec("CREATE INDEX IF NOT EXISTS idx_vehicles_active ON vehicles(is_active);");

const customerColumns = new Set(
  db.prepare("PRAGMA table_info(customers)").all().map((col) => col.name)
);
if (customerColumns.size > 0 && !customerColumns.has("phone")) {
  db.exec("ALTER TABLE customers ADD COLUMN phone TEXT;");
}
if (customerColumns.size > 0 && !customerColumns.has("location")) {
  db.exec("ALTER TABLE customers ADD COLUMN location TEXT;");
}
if (customerColumns.size > 0 && !customerColumns.has("note")) {
  db.exec("ALTER TABLE customers ADD COLUMN note TEXT;");
}
if (customerColumns.size > 0 && !customerColumns.has("is_active")) {
  db.exec("ALTER TABLE customers ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1;");
}
if (customerColumns.size > 0 && !customerColumns.has("merged_into_customer_id")) {
  db.exec("ALTER TABLE customers ADD COLUMN merged_into_customer_id INTEGER;");
}
if (customerColumns.size > 0 && !customerColumns.has("deactivated_at")) {
  db.exec("ALTER TABLE customers ADD COLUMN deactivated_at TEXT;");
}
if (customerColumns.size > 0 && !customerColumns.has("deactivated_by")) {
  db.exec("ALTER TABLE customers ADD COLUMN deactivated_by INTEGER;");
}
if (customerColumns.size > 0 && !customerColumns.has("created_by")) {
  db.exec("ALTER TABLE customers ADD COLUMN created_by INTEGER;");
}
if (customerColumns.size > 0 && !customerColumns.has("updated_at")) {
  db.exec("ALTER TABLE customers ADD COLUMN updated_at TEXT NOT NULL DEFAULT (datetime('now'));");
}
db.exec("UPDATE customers SET is_active = 1 WHERE is_active IS NULL;");
db.exec("UPDATE customers SET updated_at = COALESCE(NULLIF(updated_at, ''), created_at, datetime('now')) WHERE updated_at IS NULL OR TRIM(updated_at) = '';");
db.exec("CREATE INDEX IF NOT EXISTS idx_customers_name ON customers(name);");
db.exec("CREATE INDEX IF NOT EXISTS idx_customers_active ON customers(is_active);");
db.exec("CREATE INDEX IF NOT EXISTS idx_customers_merged_into ON customers(merged_into_customer_id);");

const vehicleComplianceColumns = new Set(
  db.prepare("PRAGMA table_info(vehicle_compliance)").all().map((col) => col.name)
);
if (vehicleComplianceColumns.size > 0 && !vehicleComplianceColumns.has("insurance_details")) {
  db.exec("ALTER TABLE vehicle_compliance ADD COLUMN insurance_details TEXT;");
}
if (vehicleComplianceColumns.size > 0 && !vehicleComplianceColumns.has("permit_details")) {
  db.exec("ALTER TABLE vehicle_compliance ADD COLUMN permit_details TEXT;");
}

const jarSalesColumns = new Set(
  db.prepare("PRAGMA table_info(jar_sales)").all().map((col) => col.name)
);
if (jarSalesColumns.size > 0 && !jarSalesColumns.has("customer_name")) {
  db.exec("ALTER TABLE jar_sales ADD COLUMN customer_name TEXT;");
}
if (jarSalesColumns.size > 0 && !jarSalesColumns.has("vehicle_id")) {
  db.exec("ALTER TABLE jar_sales ADD COLUMN vehicle_id INTEGER;");
}
if (jarSalesColumns.size > 0 && !jarSalesColumns.has("vehicle_number")) {
  db.exec("ALTER TABLE jar_sales ADD COLUMN vehicle_number TEXT;");
}

const jarSalePaymentColumns = new Set(
  db.prepare("PRAGMA table_info(jar_sale_payments)").all().map((col) => col.name)
);
if (jarSalePaymentColumns.size > 0 && !jarSalePaymentColumns.has("cash_amount")) {
  db.exec("ALTER TABLE jar_sale_payments ADD COLUMN cash_amount REAL NOT NULL DEFAULT 0;");
}
if (jarSalePaymentColumns.size > 0 && !jarSalePaymentColumns.has("bank_amount")) {
  db.exec("ALTER TABLE jar_sale_payments ADD COLUMN bank_amount REAL NOT NULL DEFAULT 0;");
}
if (jarSalePaymentColumns.size > 0 && !jarSalePaymentColumns.has("ewallet_amount")) {
  db.exec("ALTER TABLE jar_sale_payments ADD COLUMN ewallet_amount REAL NOT NULL DEFAULT 0;");
}
if (jarSalePaymentColumns.size > 0 && !jarSalePaymentColumns.has("payment_method")) {
  db.exec("ALTER TABLE jar_sale_payments ADD COLUMN payment_method TEXT NOT NULL DEFAULT 'CASH';");
}
if (jarSalePaymentColumns.size > 0 && !jarSalePaymentColumns.has("receipt_no")) {
  db.exec("ALTER TABLE jar_sale_payments ADD COLUMN receipt_no TEXT;");
}
if (jarSalePaymentColumns.size > 0 && !jarSalePaymentColumns.has("updated_at")) {
  db.exec("ALTER TABLE jar_sale_payments ADD COLUMN updated_at TEXT NOT NULL DEFAULT (datetime('now'));");
}
db.exec("UPDATE jar_sale_payments SET payment_method = 'CASH' WHERE payment_method IS NULL OR TRIM(payment_method) = '';");
db.exec("UPDATE jar_sale_payments SET payment_method = 'CASH' WHERE payment_method NOT IN ('CASH','BANK','E_WALLET','MIXED');");
db.exec("UPDATE jar_sale_payments SET cash_amount = 0 WHERE cash_amount IS NULL OR cash_amount < 0;");
db.exec("UPDATE jar_sale_payments SET bank_amount = 0 WHERE bank_amount IS NULL OR bank_amount < 0;");
db.exec("UPDATE jar_sale_payments SET ewallet_amount = 0 WHERE ewallet_amount IS NULL OR ewallet_amount < 0;");
db.exec(
  `UPDATE jar_sale_payments
   SET cash_amount = CASE WHEN payment_method = 'CASH' THEN amount ELSE 0 END,
       bank_amount = CASE WHEN payment_method = 'BANK' THEN amount ELSE 0 END,
       ewallet_amount = CASE WHEN payment_method = 'E_WALLET' THEN amount ELSE 0 END
   WHERE COALESCE(amount, 0) > 0
     AND COALESCE(cash_amount, 0) = 0
     AND COALESCE(bank_amount, 0) = 0
     AND COALESCE(ewallet_amount, 0) = 0`
);
db.exec(
  `UPDATE jar_sale_payments
   SET amount = ROUND(COALESCE(cash_amount, 0) + COALESCE(bank_amount, 0) + COALESCE(ewallet_amount, 0), 2)`
);
db.exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_jar_sale_payments_receipt_no ON jar_sale_payments(receipt_no);");

const jarContainerLendingColumns = new Set(
  db.prepare("PRAGMA table_info(jar_container_lendings)").all().map((col) => col.name)
);
if (jarContainerLendingColumns.size > 0 && !jarContainerLendingColumns.has("source_type")) {
  db.exec("ALTER TABLE jar_container_lendings ADD COLUMN source_type TEXT NOT NULL DEFAULT 'STORAGE';");
}
if (jarContainerLendingColumns.size > 0 && !jarContainerLendingColumns.has("vehicle_id")) {
  db.exec("ALTER TABLE jar_container_lendings ADD COLUMN vehicle_id INTEGER;");
}
db.exec("UPDATE jar_container_lendings SET source_type = 'STORAGE' WHERE source_type IS NULL OR TRIM(source_type) = '';");
db.exec("UPDATE jar_container_lendings SET source_type = 'STORAGE' WHERE source_type NOT IN ('STORAGE','VEHICLE');");
db.exec("CREATE INDEX IF NOT EXISTS idx_jar_container_lendings_source ON jar_container_lendings(source_type);");
db.exec("CREATE INDEX IF NOT EXISTS idx_jar_container_lendings_vehicle ON jar_container_lendings(vehicle_id);");

const leakageJarSalePaymentColumns = new Set(
  db.prepare("PRAGMA table_info(leakage_jar_sale_payments)").all().map((col) => col.name)
);
if (leakageJarSalePaymentColumns.size > 0 && !leakageJarSalePaymentColumns.has("cash_amount")) {
  db.exec("ALTER TABLE leakage_jar_sale_payments ADD COLUMN cash_amount REAL NOT NULL DEFAULT 0;");
}
if (leakageJarSalePaymentColumns.size > 0 && !leakageJarSalePaymentColumns.has("bank_amount")) {
  db.exec("ALTER TABLE leakage_jar_sale_payments ADD COLUMN bank_amount REAL NOT NULL DEFAULT 0;");
}
if (leakageJarSalePaymentColumns.size > 0 && !leakageJarSalePaymentColumns.has("ewallet_amount")) {
  db.exec("ALTER TABLE leakage_jar_sale_payments ADD COLUMN ewallet_amount REAL NOT NULL DEFAULT 0;");
}
if (leakageJarSalePaymentColumns.size > 0 && !leakageJarSalePaymentColumns.has("payment_method")) {
  db.exec("ALTER TABLE leakage_jar_sale_payments ADD COLUMN payment_method TEXT NOT NULL DEFAULT 'CASH';");
}
if (leakageJarSalePaymentColumns.size > 0 && !leakageJarSalePaymentColumns.has("receipt_no")) {
  db.exec("ALTER TABLE leakage_jar_sale_payments ADD COLUMN receipt_no TEXT;");
}
if (leakageJarSalePaymentColumns.size > 0 && !leakageJarSalePaymentColumns.has("updated_at")) {
  db.exec("ALTER TABLE leakage_jar_sale_payments ADD COLUMN updated_at TEXT NOT NULL DEFAULT (datetime('now'));");
}
db.exec("UPDATE leakage_jar_sale_payments SET payment_method = 'CASH' WHERE payment_method IS NULL OR TRIM(payment_method) = '';");
db.exec("UPDATE leakage_jar_sale_payments SET payment_method = 'CASH' WHERE payment_method NOT IN ('CASH','BANK','E_WALLET','MIXED');");
db.exec("UPDATE leakage_jar_sale_payments SET cash_amount = 0 WHERE cash_amount IS NULL OR cash_amount < 0;");
db.exec("UPDATE leakage_jar_sale_payments SET bank_amount = 0 WHERE bank_amount IS NULL OR bank_amount < 0;");
db.exec("UPDATE leakage_jar_sale_payments SET ewallet_amount = 0 WHERE ewallet_amount IS NULL OR ewallet_amount < 0;");
db.exec(
  `UPDATE leakage_jar_sale_payments
   SET cash_amount = CASE WHEN payment_method = 'CASH' THEN amount ELSE 0 END,
       bank_amount = CASE WHEN payment_method = 'BANK' THEN amount ELSE 0 END,
       ewallet_amount = CASE WHEN payment_method = 'E_WALLET' THEN amount ELSE 0 END
   WHERE COALESCE(amount, 0) > 0
     AND COALESCE(cash_amount, 0) = 0
     AND COALESCE(bank_amount, 0) = 0
     AND COALESCE(ewallet_amount, 0) = 0`
);
db.exec(
  `UPDATE leakage_jar_sale_payments
   SET amount = ROUND(COALESCE(cash_amount, 0) + COALESCE(bank_amount, 0) + COALESCE(ewallet_amount, 0), 2)`
);
db.exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_leakage_jar_sale_payments_receipt_no ON leakage_jar_sale_payments(receipt_no);");

if (jarTypeColumns.size > 0 && !jarTypeColumns.has("updated_at")) {
  db.exec("ALTER TABLE jar_types ADD COLUMN updated_at TEXT NOT NULL DEFAULT (datetime('now'));");
}

const importTypeColumns = new Set(
  db.prepare("PRAGMA table_info(import_item_types)").all().map((col) => col.name)
);
if (importTypeColumns.size > 0 && !importTypeColumns.has("is_active")) {
  db.exec("ALTER TABLE import_item_types ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1;");
}
if (importTypeColumns.size > 0 && !importTypeColumns.has("is_predefined")) {
  db.exec("ALTER TABLE import_item_types ADD COLUMN is_predefined INTEGER NOT NULL DEFAULT 0;");
}
if (importTypeColumns.size > 0 && !importTypeColumns.has("uses_direction")) {
  db.exec("ALTER TABLE import_item_types ADD COLUMN uses_direction INTEGER NOT NULL DEFAULT 1;");
}
if (importTypeColumns.size > 0 && !importTypeColumns.has("unit_label")) {
  db.exec("ALTER TABLE import_item_types ADD COLUMN unit_label TEXT;");
}
if (importTypeColumns.size > 0 && !importTypeColumns.has("updated_at")) {
  db.exec("ALTER TABLE import_item_types ADD COLUMN updated_at TEXT NOT NULL DEFAULT (datetime('now'));");
}

const recycleColumns = new Set(
  db.prepare("PRAGMA table_info(recycle_bin)").all().map((col) => col.name)
);
if (recycleColumns.size > 0 && !recycleColumns.has("note")) {
  db.exec("ALTER TABLE recycle_bin ADD COLUMN note TEXT;");
}
if (recycleColumns.size > 0 && !recycleColumns.has("restore_until")) {
  db.exec("ALTER TABLE recycle_bin ADD COLUMN restore_until TEXT;");
  db.exec("UPDATE recycle_bin SET restore_until = datetime(deleted_at, '+30 day') WHERE restore_until IS NULL;");
}
if (recycleColumns.size > 0 && !recycleColumns.has("restored_at")) {
  db.exec("ALTER TABLE recycle_bin ADD COLUMN restored_at TEXT;");
}
if (recycleColumns.size > 0 && !recycleColumns.has("restored_by")) {
  db.exec("ALTER TABLE recycle_bin ADD COLUMN restored_by INTEGER;");
}

const defaultImportItemTypes = [
  { code: "JAR_CONTAINER", name: "Jar Container", unit_label: "", uses_direction: 0 },
  { code: "BOTTLE_CASE", name: "Bottle Case", unit_label: "Case", uses_direction: 0 },
  { code: "DISPENSER", name: "Dispenser", unit_label: "Piece", uses_direction: 0 },
  { code: "JAR_CAP", name: "Jar Cap", unit_label: "Bora", uses_direction: 1 },
  { code: "CHEMICAL_LABEL", name: "Wash Chemical", unit_label: "Gallon", uses_direction: 1 },
  { code: "LABEL_STICKER", name: "Label Sticker", unit_label: "Bundle", uses_direction: 1 },
  { code: "DATE_LABEL", name: "Date Sticker", unit_label: "Roll", uses_direction: 1 }
];
const upsertImportItemType = db.prepare(
  `INSERT INTO import_item_types (code, name, unit_label, uses_direction, is_predefined, is_active)
   VALUES (?, ?, ?, ?, 1, 1)
   ON CONFLICT(code) DO UPDATE SET
     name = excluded.name,
     unit_label = COALESCE(import_item_types.unit_label, excluded.unit_label),
     uses_direction = excluded.uses_direction,
     is_predefined = 1`
);
defaultImportItemTypes.forEach((row) => {
  upsertImportItemType.run(row.code, row.name, row.unit_label, row.uses_direction);
});

const defaultStaffRoles = [
  { code: "CLEANER", name: "Cleaner" },
  { code: "MACHINE_MANAGER", name: "Machine Manager" },
  { code: "VEHICLE_CONDUCTOR", name: "Vehicle Conductor" },
  { code: "KITCHEN_COOK", name: "Kitchen Cook" }
];
const insertStaffRoleIfMissing = db.prepare(
  `INSERT INTO staff_roles (code, name, is_active)
   VALUES (?, ?, 1)
   ON CONFLICT(code) DO NOTHING`
);
defaultStaffRoles.forEach((role) => {
  insertStaffRoleIfMissing.run(role.code, role.name);
});

ensureRolePermissionRows(db);
db.exec("CREATE INDEX IF NOT EXISTS idx_role_permissions_role ON role_permissions(role);");
db.exec("CREATE INDEX IF NOT EXISTS idx_role_permissions_module ON role_permissions(module_key);");

const humanizeRoleCode = (code) => String(code || "")
  .trim()
  .replace(/_/g, " ")
  .toLowerCase()
  .replace(/\b\w/g, (c) => c.toUpperCase());
const existingRoleCodes = db.prepare("SELECT DISTINCT TRIM(staff_role) as code FROM staff WHERE staff_role IS NOT NULL AND TRIM(staff_role) <> ''").all();
existingRoleCodes.forEach((row) => {
  const roleCode = String(row.code || "").trim().toUpperCase();
  if (!roleCode) return;
  insertStaffRoleIfMissing.run(roleCode, humanizeRoleCode(roleCode));
});

db.exec(
  `UPDATE exports
   SET receipt_no = 'EXP-' || COALESCE(NULLIF(REPLACE(export_date, '-', ''), ''), strftime('%Y%m%d', 'now')) || '-' || printf('%06d', id)
   WHERE receipt_no IS NULL OR TRIM(receipt_no) = ''`
);
db.exec(
  `UPDATE credits
   SET receipt_no = 'CRD-' || COALESCE(NULLIF(REPLACE(credit_date, '-', ''), ''), strftime('%Y%m%d', 'now')) || '-' || printf('%06d', id)
   WHERE receipt_no IS NULL OR TRIM(receipt_no) = ''`
);
db.exec(
  `UPDATE staff_salary_payments
   SET receipt_no = 'STF-' || COALESCE(NULLIF(REPLACE(payment_date, '-', ''), ''), strftime('%Y%m%d', 'now')) || '-' || printf('%06d', id)
   WHERE receipt_no IS NULL OR TRIM(receipt_no) = ''`
);
db.exec(
  `UPDATE worker_salary_payments
   SET receipt_no = 'WRK-' || COALESCE(NULLIF(REPLACE(payment_date, '-', ''), ''), strftime('%Y%m%d', 'now')) || '-' || printf('%06d', id)
   WHERE receipt_no IS NULL OR TRIM(receipt_no) = ''`
);
db.exec(
  `UPDATE credit_payments
   SET receipt_no = 'CPY-' || COALESCE(NULLIF(REPLACE(date(paid_at), '-', ''), ''), strftime('%Y%m%d', 'now')) || '-' || printf('%06d', id)
   WHERE receipt_no IS NULL OR TRIM(receipt_no) = ''`
);
db.exec(
  `UPDATE export_credit_payments
   SET receipt_no = 'ECP-' || COALESCE(NULLIF(REPLACE(payment_date, '-', ''), ''), strftime('%Y%m%d', 'now')) || '-' || printf('%06d', id)
   WHERE receipt_no IS NULL OR TRIM(receipt_no) = ''`
);
db.exec(
  `UPDATE import_payments
   SET receipt_no = 'IMP-' || COALESCE(NULLIF(REPLACE(payment_date, '-', ''), ''), strftime('%Y%m%d', 'now')) || '-' || printf('%06d', id)
   WHERE receipt_no IS NULL OR TRIM(receipt_no) = ''`
);
db.exec(
  `UPDATE company_purchase_payments
   SET receipt_no = 'CPP-' || COALESCE(NULLIF(REPLACE(payment_date, '-', ''), ''), strftime('%Y%m%d', 'now')) || '-' || printf('%06d', id)
   WHERE receipt_no IS NULL OR TRIM(receipt_no) = ''`
);
db.exec(
  `UPDATE vehicle_expense_payments
   SET receipt_no = 'VEP-' || COALESCE(NULLIF(REPLACE(payment_date, '-', ''), ''), strftime('%Y%m%d', 'now')) || '-' || printf('%06d', id)
   WHERE receipt_no IS NULL OR TRIM(receipt_no) = ''`
);
db.exec(
  `UPDATE jar_sale_payments
   SET receipt_no = 'JRP-' || COALESCE(NULLIF(REPLACE(payment_date, '-', ''), ''), strftime('%Y%m%d', 'now')) || '-' || printf('%06d', id)
   WHERE receipt_no IS NULL OR TRIM(receipt_no) = ''`
);
db.exec(
  `UPDATE leakage_jar_sale_payments
   SET receipt_no = 'LJP-' || COALESCE(NULLIF(REPLACE(payment_date, '-', ''), ''), strftime('%Y%m%d', 'now')) || '-' || printf('%06d', id)
   WHERE receipt_no IS NULL OR TRIM(receipt_no) = ''`
);
db.exec(
  "CREATE UNIQUE INDEX IF NOT EXISTS idx_exports_receipt_no ON exports(receipt_no);"
);
db.exec(
  "CREATE UNIQUE INDEX IF NOT EXISTS idx_credits_receipt_no ON credits(receipt_no);"
);
db.exec(
  "CREATE UNIQUE INDEX IF NOT EXISTS idx_staff_salary_receipt_no ON staff_salary_payments(receipt_no);"
);
db.exec(
  "CREATE UNIQUE INDEX IF NOT EXISTS idx_worker_salary_receipt_no ON worker_salary_payments(receipt_no);"
);
db.exec(
  "CREATE UNIQUE INDEX IF NOT EXISTS idx_credit_payments_receipt_no ON credit_payments(receipt_no);"
);
db.exec(
  "CREATE UNIQUE INDEX IF NOT EXISTS idx_export_credit_payments_receipt_no ON export_credit_payments(receipt_no);"
);
db.exec(
  "CREATE UNIQUE INDEX IF NOT EXISTS idx_import_payments_receipt_no ON import_payments(receipt_no);"
);
db.exec(
  "CREATE UNIQUE INDEX IF NOT EXISTS idx_company_purchase_payments_receipt_no ON company_purchase_payments(receipt_no);"
);
db.exec(
  "CREATE UNIQUE INDEX IF NOT EXISTS idx_vehicle_expense_payments_receipt_no ON vehicle_expense_payments(receipt_no);"
);
db.exec(
  "CREATE UNIQUE INDEX IF NOT EXISTS idx_jar_sale_payments_receipt_no ON jar_sale_payments(receipt_no);"
);
db.exec(
  "CREATE UNIQUE INDEX IF NOT EXISTS idx_leakage_jar_sale_payments_receipt_no ON leakage_jar_sale_payments(receipt_no);"
);
db.exec(
  `UPDATE recycle_bin
   SET restore_until = datetime(deleted_at, '+30 day')
   WHERE restore_until IS NULL OR TRIM(restore_until) = ''`
);

const paymentCount = db.prepare("SELECT COUNT(*) as count FROM credit_payments").get().count;
if (paymentCount === 0) {
  const existingCredits = db.prepare(
    "SELECT id, paid_amount, credit_date, created_by FROM credits WHERE paid_amount > 0"
  ).all();
  const insertPayment = db.prepare(
    "INSERT INTO credit_payments (credit_id, amount, note, created_by, paid_at) VALUES (?, ?, ?, ?, ?)"
  );
  existingCredits.forEach((credit) => {
    insertPayment.run(
      credit.id,
      Number(credit.paid_amount || 0),
      "Opening balance",
      credit.created_by || null,
      credit.credit_date
    );
  });
}
{
  const creditPaymentDiffRows = db.prepare(
    `SELECT credits.id,
            credits.paid_amount,
            credits.credit_date,
            credits.created_by,
            COALESCE(NULLIF(credits.payment_method, ''), 'CASH') AS payment_method,
            COALESCE(payment_totals.total_paid, 0) AS total_paid
     FROM credits
     LEFT JOIN (
       SELECT credit_id, COALESCE(SUM(amount), 0) AS total_paid
       FROM credit_payments
       GROUP BY credit_id
     ) AS payment_totals ON payment_totals.credit_id = credits.id
     WHERE credits.paid_amount > COALESCE(payment_totals.total_paid, 0) + 0.009`
  ).all();
  const insertMissingCreditPayment = db.prepare(
    "INSERT INTO credit_payments (credit_id, amount, payment_method, note, created_by, paid_at) VALUES (?, ?, ?, ?, ?, ?)"
  );
  creditPaymentDiffRows.forEach((row) => {
    const missingAmount = positiveMoney(Number(row.paid_amount || 0) - Number(row.total_paid || 0));
    if (missingAmount <= 0) return;
    insertMissingCreditPayment.run(
      row.id,
      missingAmount,
      row.payment_method,
      "Opening payment",
      row.created_by || null,
      row.credit_date || new Date().toISOString().slice(0, 10)
    );
  });
}
db.exec(
  `UPDATE credits
   SET paid_amount = COALESCE((
     SELECT ROUND(SUM(credit_payments.amount), 2)
     FROM credit_payments
     WHERE credit_payments.credit_id = credits.id
   ), 0),
       paid = CASE
         WHEN amount <= COALESCE((
           SELECT ROUND(SUM(credit_payments.amount), 2)
           FROM credit_payments
           WHERE credit_payments.credit_id = credits.id
         ), 0) + 0.009 THEN 1
         ELSE 0
       END`
);

{
  const activityRows = db.prepare(
    `SELECT id, entity_type, entity_id, details, user_id, created_at
     FROM activity_logs
     WHERE action = 'payment'
       AND entity_type IN ('export', 'export_day_credit', 'export_vehicle_cumulative_settlement')
       AND id NOT IN (
         SELECT legacy_activity_id
         FROM export_credit_payments
         WHERE legacy_activity_id IS NOT NULL
       )
     ORDER BY id ASC`
  ).all();
  const getNumberFromDetails = (details, key) => {
    const match = String(details || "").match(new RegExp(`${key}=(-?\\d+(?:\\.\\d+)?)`, "i"));
    return match ? Number(match[1]) : 0;
  };
  const getTextFromDetails = (details, key) => {
    const match = String(details || "").match(new RegExp(`${key}=([^;,\n]+)`, "i"));
    return match ? String(match[1]).trim() : "";
  };
  const exportVehicleLookup = db.prepare("SELECT vehicle_id FROM exports WHERE id = ?");
  const insertExportCreditEvent = db.prepare(
    `INSERT INTO export_credit_payments (
        export_id, vehicle_id, payment_date, amount, cash_amount, bank_amount, ewallet_amount,
        payment_method, note, receipt_no, legacy_activity_id, created_by, created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
  );
  activityRows.forEach((row) => {
    const amount = Number(getNumberFromDetails(row.details, "payment") || 0);
    if (!(amount > 0)) return;
    const cashAmount = Number(getNumberFromDetails(row.details, "cash") || 0);
    const bankAmount = Number(getNumberFromDetails(row.details, "bank") || 0);
    const ewalletAmount = Number(getNumberFromDetails(row.details, "ewallet") || 0);
    const paymentDate = getTextFromDetails(row.details, "payment_date")
      || String(row.created_at || "").slice(0, 10)
      || new Date().toISOString().slice(0, 10);
    let exportId = null;
    let vehicleId = null;
    if (String(row.entity_type) === "export") {
      const parsedId = Number(row.entity_id || 0);
      if (parsedId > 0) {
        exportId = parsedId;
        const exportRow = exportVehicleLookup.get(parsedId);
        vehicleId = exportRow ? Number(exportRow.vehicle_id || 0) || null : null;
      }
    } else if (String(row.entity_type) === "export_day_credit") {
      const parsedVehicleId = Number(String(row.entity_id || "").split(":")[0] || 0);
      vehicleId = parsedVehicleId > 0 ? parsedVehicleId : null;
    } else if (String(row.entity_type) === "export_vehicle_cumulative_settlement") {
      const parsedVehicleId = Number(row.entity_id || 0);
      vehicleId = parsedVehicleId > 0 ? parsedVehicleId : null;
    }
    const method = getTextFromDetails(row.details, "method").toUpperCase().replace(/[^A-Z_]/g, "") || "CASH";
    const receiptNo = getTextFromDetails(row.details, "receipt") || null;
    insertExportCreditEvent.run(
      exportId,
      vehicleId,
      paymentDate,
      amount,
      cashAmount,
      bankAmount,
      ewalletAmount,
      method,
      "Backfilled export credit payment",
      receiptNo,
      row.id,
      row.user_id || null,
      row.created_at || null
    );
  });
}

db.exec(
  `UPDATE import_entries
   SET paid_amount = CASE
     WHEN paid_amount < 0 THEN 0
     WHEN paid_amount > total_amount THEN total_amount
     ELSE paid_amount
   END`
);
db.exec(
  `UPDATE import_entries
   SET is_credit = CASE
     WHEN total_amount - paid_amount > 0 THEN 1
     ELSE 0
   END`
);

const importPaymentCount = db.prepare("SELECT COUNT(*) as count FROM import_payments").get().count;
if (importPaymentCount === 0) {
  const existingImportPayments = db.prepare(
    `SELECT id, paid_amount, entry_date, created_by
     FROM import_entries
     WHERE paid_amount > 0`
  ).all();
  const insertImportPayment = db.prepare(
    "INSERT INTO import_payments (import_entry_id, payment_date, amount, note, created_by) VALUES (?, ?, ?, ?, ?)"
  );
  existingImportPayments.forEach((row) => {
    insertImportPayment.run(
      row.id,
      row.entry_date || new Date().toISOString().slice(0, 10),
      Number(row.paid_amount || 0),
      "Opening payment",
      row.created_by || null
    );
  });
}
db.exec(
  `INSERT INTO import_payments (import_entry_id, payment_date, amount, note, created_by)
   SELECT import_entries.id,
          COALESCE(NULLIF(import_entries.entry_date, ''), date('now')),
          import_entries.paid_amount,
          'Opening payment',
          import_entries.created_by
   FROM import_entries
   WHERE import_entries.paid_amount > 0
     AND NOT EXISTS (
       SELECT 1
       FROM import_payments
       WHERE import_payments.import_entry_id = import_entries.id
     )`
);
db.exec(
  `UPDATE import_entries
   SET paid_amount = COALESCE((
     SELECT SUM(import_payments.amount)
     FROM import_payments
     WHERE import_payments.import_entry_id = import_entries.id
   ), 0)`
);
db.exec(
  `UPDATE import_entries
   SET is_credit = CASE
     WHEN total_amount - paid_amount > 0 THEN 1
     ELSE 0
   END`
);

db.exec(
  `UPDATE company_purchases
   SET paid_amount = CASE
     WHEN paid_amount < 0 THEN 0
     WHEN paid_amount > amount THEN amount
     ELSE paid_amount
   END`
);
db.exec(
  `UPDATE company_purchases
   SET is_credit = CASE
     WHEN amount - paid_amount > 0 THEN 1
     ELSE 0
   END`
);

const companyPurchasePaymentCount = db.prepare("SELECT COUNT(*) as count FROM company_purchase_payments").get().count;
if (companyPurchasePaymentCount === 0) {
  const existingCompanyPurchasePayments = db.prepare(
    `SELECT id, paid_amount, purchase_date, created_by
     FROM company_purchases
     WHERE paid_amount > 0`
  ).all();
  const insertCompanyPurchasePayment = db.prepare(
    `INSERT INTO company_purchase_payments (company_purchase_id, payment_date, amount, note, created_by)
     VALUES (?, ?, ?, ?, ?)`
  );
  existingCompanyPurchasePayments.forEach((row) => {
    insertCompanyPurchasePayment.run(
      row.id,
      row.purchase_date || new Date().toISOString().slice(0, 10),
      Number(row.paid_amount || 0),
      "Opening payment",
      row.created_by || null
    );
  });
}
db.exec(
  `INSERT INTO company_purchase_payments (company_purchase_id, payment_date, amount, note, created_by)
   SELECT company_purchases.id,
          COALESCE(NULLIF(company_purchases.purchase_date, ''), date('now')),
          company_purchases.paid_amount,
          'Opening payment',
          company_purchases.created_by
   FROM company_purchases
   WHERE company_purchases.paid_amount > 0
     AND NOT EXISTS (
       SELECT 1
       FROM company_purchase_payments
       WHERE company_purchase_payments.company_purchase_id = company_purchases.id
     )`
);
db.exec(
  `UPDATE company_purchases
   SET paid_amount = COALESCE((
     SELECT SUM(company_purchase_payments.amount)
     FROM company_purchase_payments
     WHERE company_purchase_payments.company_purchase_id = company_purchases.id
   ), 0)`
);
db.exec(
  `UPDATE company_purchases
   SET is_credit = CASE
     WHEN amount - paid_amount > 0 THEN 1
     ELSE 0
   END`
);

db.exec(
  `UPDATE vehicle_expenses
   SET paid_amount = CASE
     WHEN paid_amount < 0 THEN 0
     WHEN paid_amount > amount THEN amount
     ELSE paid_amount
   END`
);
db.exec(
  `UPDATE vehicle_expenses
   SET is_credit = CASE
     WHEN amount - paid_amount > 0 THEN 1
     ELSE 0
   END`
);

const vehicleExpensePaymentCount = db.prepare("SELECT COUNT(*) as count FROM vehicle_expense_payments").get().count;
if (vehicleExpensePaymentCount === 0) {
  const existingVehicleExpensePayments = db.prepare(
    `SELECT id, paid_amount, expense_date, created_by
     FROM vehicle_expenses
     WHERE paid_amount > 0`
  ).all();
  const insertVehicleExpensePayment = db.prepare(
    `INSERT INTO vehicle_expense_payments (vehicle_expense_id, payment_date, amount, note, created_by)
     VALUES (?, ?, ?, ?, ?)`
  );
  existingVehicleExpensePayments.forEach((row) => {
    insertVehicleExpensePayment.run(
      row.id,
      row.expense_date || new Date().toISOString().slice(0, 10),
      Number(row.paid_amount || 0),
      "Opening payment",
      row.created_by || null
    );
  });
}
db.exec(
  `INSERT INTO vehicle_expense_payments (vehicle_expense_id, payment_date, amount, note, created_by)
   SELECT vehicle_expenses.id,
          COALESCE(NULLIF(vehicle_expenses.expense_date, ''), date('now')),
          vehicle_expenses.paid_amount,
          'Opening payment',
          vehicle_expenses.created_by
   FROM vehicle_expenses
   WHERE vehicle_expenses.paid_amount > 0
     AND NOT EXISTS (
       SELECT 1
       FROM vehicle_expense_payments
       WHERE vehicle_expense_payments.vehicle_expense_id = vehicle_expenses.id
     )`
);
db.exec(
  `UPDATE vehicle_expenses
   SET paid_amount = COALESCE((
     SELECT SUM(vehicle_expense_payments.amount)
     FROM vehicle_expense_payments
     WHERE vehicle_expense_payments.vehicle_expense_id = vehicle_expenses.id
   ), 0)`
);
db.exec(
  `UPDATE vehicle_expenses
   SET is_credit = CASE
     WHEN amount - paid_amount > 0 THEN 1
     ELSE 0
   END`
);

db.exec(
  `UPDATE jar_sales
   SET paid_amount = CASE
     WHEN paid_amount < 0 THEN 0
     WHEN paid_amount > total_amount THEN total_amount
     ELSE paid_amount
   END`
);
db.exec(
  `UPDATE jar_sales
   SET credit_amount = CASE
     WHEN total_amount - paid_amount > 0 THEN total_amount - paid_amount
     ELSE 0
   END`
);

const leakageJarSalePaymentCount = db.prepare("SELECT COUNT(*) as count FROM leakage_jar_sale_payments").get().count;
if (leakageJarSalePaymentCount === 0) {
  const existingLeakageSalePayments = db.prepare(
    `SELECT id, sale_date, paid_amount, paid_cash_amount, paid_bank_amount, paid_ewallet_amount, payment_method, created_by
     FROM leakage_jar_sales
     WHERE paid_amount > 0`
  ).all();
  const insertLeakageSalePayment = db.prepare(
    `INSERT INTO leakage_jar_sale_payments (
      leakage_jar_sale_id, payment_date, amount, cash_amount, bank_amount, ewallet_amount, payment_method, note, created_by
     ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
  );
  existingLeakageSalePayments.forEach((row) => {
    insertLeakageSalePayment.run(
      row.id,
      row.sale_date || new Date().toISOString().slice(0, 10),
      Number(row.paid_amount || 0),
      Number(row.paid_cash_amount || 0),
      Number(row.paid_bank_amount || 0),
      Number(row.paid_ewallet_amount || 0),
      row.payment_method || "CASH",
      "Opening payment",
      row.created_by || null
    );
  });
}
db.exec(
  `INSERT INTO leakage_jar_sale_payments (
      leakage_jar_sale_id, payment_date, amount, cash_amount, bank_amount, ewallet_amount, payment_method, note, created_by
   )
   SELECT leakage_jar_sales.id,
          COALESCE(NULLIF(leakage_jar_sales.sale_date, ''), date('now')),
          leakage_jar_sales.paid_amount,
          COALESCE(leakage_jar_sales.paid_cash_amount, 0),
          COALESCE(leakage_jar_sales.paid_bank_amount, 0),
          COALESCE(leakage_jar_sales.paid_ewallet_amount, 0),
          COALESCE(NULLIF(leakage_jar_sales.payment_method, ''), 'CASH'),
          'Opening payment',
          leakage_jar_sales.created_by
   FROM leakage_jar_sales
   WHERE leakage_jar_sales.paid_amount > 0
     AND NOT EXISTS (
       SELECT 1
       FROM leakage_jar_sale_payments
       WHERE leakage_jar_sale_payments.leakage_jar_sale_id = leakage_jar_sales.id
     )`
);
{
  const leakageDiffRows = db.prepare(
    `SELECT leakage_jar_sales.id,
            leakage_jar_sales.sale_date,
            leakage_jar_sales.created_by,
            leakage_jar_sales.payment_method,
            leakage_jar_sales.paid_amount,
            leakage_jar_sales.paid_cash_amount,
            leakage_jar_sales.paid_bank_amount,
            leakage_jar_sales.paid_ewallet_amount,
            COALESCE(payment_totals.total_paid, 0) AS total_paid,
            COALESCE(payment_totals.cash_paid, 0) AS cash_paid,
            COALESCE(payment_totals.bank_paid, 0) AS bank_paid,
            COALESCE(payment_totals.ewallet_paid, 0) AS ewallet_paid
     FROM leakage_jar_sales
     LEFT JOIN (
       SELECT leakage_jar_sale_id,
              COALESCE(SUM(amount), 0) AS total_paid,
              COALESCE(SUM(cash_amount), 0) AS cash_paid,
              COALESCE(SUM(bank_amount), 0) AS bank_paid,
              COALESCE(SUM(ewallet_amount), 0) AS ewallet_paid
       FROM leakage_jar_sale_payments
       GROUP BY leakage_jar_sale_id
     ) AS payment_totals ON payment_totals.leakage_jar_sale_id = leakage_jar_sales.id
     WHERE leakage_jar_sales.paid_amount > COALESCE(payment_totals.total_paid, 0) + 0.009`
  ).all();
  const insertLeakageDiffPayment = db.prepare(
    `INSERT INTO leakage_jar_sale_payments (
      leakage_jar_sale_id, payment_date, amount, cash_amount, bank_amount, ewallet_amount, payment_method, note, created_by
     ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
  );
  leakageDiffRows.forEach((row) => {
    const missingAmount = positiveMoney(Number(row.paid_amount || 0) - Number(row.total_paid || 0));
    if (missingAmount <= 0) return;
    let cashAmount = positiveMoney(Number(row.paid_cash_amount || 0) - Number(row.cash_paid || 0));
    let bankAmount = positiveMoney(Number(row.paid_bank_amount || 0) - Number(row.bank_paid || 0));
    let ewalletAmount = positiveMoney(Number(row.paid_ewallet_amount || 0) - Number(row.ewallet_paid || 0));
    const breakdownTotal = toMoney(cashAmount + bankAmount + ewalletAmount);
    if (breakdownTotal <= 0) {
      cashAmount = missingAmount;
    } else if (Math.abs(breakdownTotal - missingAmount) > 0.01) {
      const scale = missingAmount / breakdownTotal;
      cashAmount = toMoney(cashAmount * scale);
      bankAmount = toMoney(bankAmount * scale);
      ewalletAmount = toMoney(missingAmount - cashAmount - bankAmount);
    }
    insertLeakageDiffPayment.run(
      row.id,
      row.sale_date || new Date().toISOString().slice(0, 10),
      missingAmount,
      cashAmount,
      bankAmount,
      ewalletAmount,
      row.payment_method || "CASH",
      "Opening payment",
      row.created_by || null
    );
  });
}
db.exec(
  `UPDATE leakage_jar_sales
   SET paid_amount = COALESCE((
     SELECT ROUND(SUM(leakage_jar_sale_payments.amount), 2)
     FROM leakage_jar_sale_payments
     WHERE leakage_jar_sale_payments.leakage_jar_sale_id = leakage_jar_sales.id
   ), 0),
       paid_cash_amount = COALESCE((
         SELECT ROUND(SUM(leakage_jar_sale_payments.cash_amount), 2)
         FROM leakage_jar_sale_payments
         WHERE leakage_jar_sale_payments.leakage_jar_sale_id = leakage_jar_sales.id
       ), 0),
       paid_bank_amount = COALESCE((
         SELECT ROUND(SUM(leakage_jar_sale_payments.bank_amount), 2)
         FROM leakage_jar_sale_payments
         WHERE leakage_jar_sale_payments.leakage_jar_sale_id = leakage_jar_sales.id
       ), 0),
       paid_ewallet_amount = COALESCE((
         SELECT ROUND(SUM(leakage_jar_sale_payments.ewallet_amount), 2)
         FROM leakage_jar_sale_payments
         WHERE leakage_jar_sale_payments.leakage_jar_sale_id = leakage_jar_sales.id
       ), 0)`
);
db.exec(
  `UPDATE leakage_jar_sales
   SET credit_amount = CASE
     WHEN total_amount - paid_amount > 0 THEN total_amount - paid_amount
     ELSE 0
   END`
);

const jarSalePaymentCount = db.prepare("SELECT COUNT(*) as count FROM jar_sale_payments").get().count;
if (jarSalePaymentCount === 0) {
  const existingJarSalePayments = db.prepare(
    `SELECT id, paid_amount, sale_date, created_by
     FROM jar_sales
     WHERE paid_amount > 0`
  ).all();
  const insertJarSalePayment = db.prepare(
    `INSERT INTO jar_sale_payments (jar_sale_id, payment_date, amount, cash_amount, bank_amount, ewallet_amount, payment_method, note, created_by)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
  );
  existingJarSalePayments.forEach((row) => {
    insertJarSalePayment.run(
      row.id,
      row.sale_date || new Date().toISOString().slice(0, 10),
      Number(row.paid_amount || 0),
      Number(row.paid_amount || 0),
      0,
      0,
      "CASH",
      "Opening payment",
      row.created_by || null
    );
  });
}
db.exec(
  `INSERT INTO jar_sale_payments (jar_sale_id, payment_date, amount, cash_amount, bank_amount, ewallet_amount, payment_method, note, created_by)
   SELECT jar_sales.id,
          COALESCE(NULLIF(jar_sales.sale_date, ''), date('now')),
          jar_sales.paid_amount,
          jar_sales.paid_amount,
          0,
          0,
          'CASH',
          'Opening payment',
          jar_sales.created_by
   FROM jar_sales
   WHERE jar_sales.paid_amount > 0
     AND NOT EXISTS (
       SELECT 1
       FROM jar_sale_payments
       WHERE jar_sale_payments.jar_sale_id = jar_sales.id
     )`
);
{
  const jarSaleDiffRows = db.prepare(
    `SELECT jar_sales.id,
            jar_sales.sale_date,
            jar_sales.created_by,
            jar_sales.paid_amount,
            COALESCE(payment_totals.total_paid, 0) AS total_paid
     FROM jar_sales
     LEFT JOIN (
       SELECT jar_sale_id,
              COALESCE(SUM(amount), 0) AS total_paid
       FROM jar_sale_payments
       GROUP BY jar_sale_id
     ) AS payment_totals ON payment_totals.jar_sale_id = jar_sales.id
     WHERE jar_sales.paid_amount > COALESCE(payment_totals.total_paid, 0) + 0.009`
  ).all();
  const insertJarSaleDiffPayment = db.prepare(
    `INSERT INTO jar_sale_payments (
      jar_sale_id, payment_date, amount, cash_amount, bank_amount, ewallet_amount, payment_method, note, created_by
     ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
  );
  jarSaleDiffRows.forEach((row) => {
    const missingAmount = positiveMoney(Number(row.paid_amount || 0) - Number(row.total_paid || 0));
    if (missingAmount <= 0) return;
    const cashAmount = missingAmount;
    const bankAmount = 0;
    const ewalletAmount = 0;
    insertJarSaleDiffPayment.run(
      row.id,
      row.sale_date || new Date().toISOString().slice(0, 10),
      missingAmount,
      cashAmount,
      bankAmount,
      ewalletAmount,
      "CASH",
      "Opening payment",
      row.created_by || null
    );
  });
}
db.exec(
  `UPDATE jar_sales
   SET paid_amount = COALESCE((
     SELECT SUM(jar_sale_payments.amount)
     FROM jar_sale_payments
     WHERE jar_sale_payments.jar_sale_id = jar_sales.id
   ), 0)`
);
db.exec(
  `UPDATE jar_sales
   SET credit_amount = CASE
     WHEN total_amount - paid_amount > 0 THEN total_amount - paid_amount
     ELSE 0
   END`
);

module.exports = {
  db,
  dbPath
};
