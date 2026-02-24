const fs = require("fs");
const path = require("path");
const { DatabaseSync } = require("node:sqlite");

const dbPath = path.join(__dirname, "..", "data", "aqua.db");
fs.mkdirSync(path.dirname(dbPath), { recursive: true });

const db = new DatabaseSync(dbPath);

db.exec("PRAGMA journal_mode = WAL;");

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
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

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
  bottle_case_count INTEGER NOT NULL DEFAULT 0,
  dispenser_count INTEGER NOT NULL DEFAULT 0,
  jar_unit_price REAL NOT NULL DEFAULT 0,
  bottle_case_unit_price REAL NOT NULL DEFAULT 0,
  dispenser_unit_price REAL NOT NULL DEFAULT 0,
  return_jar_count INTEGER NOT NULL DEFAULT 0,
  return_bottle_case_count INTEGER NOT NULL DEFAULT 0,
  leakage_jar_count INTEGER NOT NULL DEFAULT 0,
  sold_jar_count INTEGER NOT NULL DEFAULT 0,
  sold_jar_price REAL NOT NULL DEFAULT 0,
  sold_jar_amount REAL NOT NULL DEFAULT 0,
  collection_amount REAL NOT NULL DEFAULT 0,
  expense_amount REAL NOT NULL DEFAULT 0,
  expense_note TEXT,
  total_amount REAL NOT NULL DEFAULT 0,
  paid_amount REAL NOT NULL DEFAULT 0,
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

CREATE TABLE IF NOT EXISTS credits (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  vehicle_id INTEGER NOT NULL,
  export_id INTEGER,
  customer_name TEXT NOT NULL,
  amount REAL NOT NULL DEFAULT 0,
  paid_amount REAL NOT NULL DEFAULT 0,
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
  FOREIGN KEY (checked_by_staff_id) REFERENCES staff(id) ON DELETE SET NULL,
  FOREIGN KEY (created_by) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_sales_date ON daily_sales(sale_date);
CREATE INDEX IF NOT EXISTS idx_exports_date ON exports(export_date);
CREATE INDEX IF NOT EXISTS idx_credits_date ON credits(credit_date);

CREATE TABLE IF NOT EXISTS credit_payments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  credit_id INTEGER NOT NULL,
  amount REAL NOT NULL DEFAULT 0,
  note TEXT,
  created_by INTEGER,
  paid_at TEXT NOT NULL DEFAULT (datetime('now')),
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (credit_id) REFERENCES credits(id) ON DELETE CASCADE,
  FOREIGN KEY (created_by) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_credit_payments_credit ON credit_payments(credit_id);
CREATE INDEX IF NOT EXISTS idx_credit_payments_paid_at ON credit_payments(paid_at);

CREATE TABLE IF NOT EXISTS settings (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
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
  note TEXT,
  created_by INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (jar_sale_id) REFERENCES jar_sales(id) ON DELETE CASCADE,
  FOREIGN KEY (created_by) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_jar_sale_payments_sale ON jar_sale_payments(jar_sale_id);
CREATE INDEX IF NOT EXISTS idx_jar_sale_payments_date ON jar_sale_payments(payment_date);

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
  note TEXT,
  created_by INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (vehicle_expense_id) REFERENCES vehicle_expenses(id) ON DELETE CASCADE,
  FOREIGN KEY (created_by) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_vehicle_expense_payments_entry ON vehicle_expense_payments(vehicle_expense_id);
CREATE INDEX IF NOT EXISTS idx_vehicle_expense_payments_date ON vehicle_expense_payments(payment_date);

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
  entry_date TEXT NOT NULL,
  amount REAL NOT NULL DEFAULT 0,
  note TEXT,
  created_by INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (vehicle_id) REFERENCES vehicles(id) ON DELETE CASCADE,
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
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
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
  doc_type TEXT NOT NULL CHECK (doc_type IN ('CITIZENSHIP','LICENSE','PASSPORT')),
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
  receipt_no TEXT,
  note TEXT,
  created_by INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (worker_id) REFERENCES users(id) ON DELETE CASCADE,
  FOREIGN KEY (created_by) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_worker_salary_date ON worker_salary_payments(payment_date);
CREATE INDEX IF NOT EXISTS idx_worker_salary_worker ON worker_salary_payments(worker_id);

CREATE TABLE IF NOT EXISTS worker_documents (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  worker_id INTEGER NOT NULL UNIQUE,
  doc_type TEXT CHECK (doc_type IN ('CITIZENSHIP','LICENSE','PASSPORT')),
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
`);

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

const importColumns = new Set(
  db.prepare("PRAGMA table_info(import_entries)").all().map((col) => col.name)
);
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
if (!importColumns.has("paid_amount")) {
  db.exec("ALTER TABLE import_entries ADD COLUMN paid_amount REAL NOT NULL DEFAULT 0;");
}
if (!importColumns.has("is_credit")) {
  db.exec("ALTER TABLE import_entries ADD COLUMN is_credit INTEGER NOT NULL DEFAULT 0;");
}
db.exec("CREATE INDEX IF NOT EXISTS idx_import_entries_seller ON import_entries(seller_name);");

db.exec(
  `CREATE TABLE IF NOT EXISTS import_payments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    import_entry_id INTEGER NOT NULL,
    payment_date TEXT NOT NULL,
    amount REAL NOT NULL DEFAULT 0,
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
db.exec("CREATE INDEX IF NOT EXISTS idx_company_purchases_seller ON company_purchases(seller_name);");

db.exec(
  `CREATE TABLE IF NOT EXISTS company_purchase_payments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_purchase_id INTEGER NOT NULL,
    payment_date TEXT NOT NULL,
    amount REAL NOT NULL DEFAULT 0,
    note TEXT,
    created_by INTEGER,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (company_purchase_id) REFERENCES company_purchases(id) ON DELETE CASCADE,
    FOREIGN KEY (created_by) REFERENCES users(id)
  )`
);
db.exec("CREATE INDEX IF NOT EXISTS idx_company_purchase_payments_purchase ON company_purchase_payments(company_purchase_id);");
db.exec("CREATE INDEX IF NOT EXISTS idx_company_purchase_payments_date ON company_purchase_payments(payment_date);");

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
if (!vehicleExpenseColumns.has("paid_amount")) {
  db.exec("ALTER TABLE vehicle_expenses ADD COLUMN paid_amount REAL NOT NULL DEFAULT 0;");
  db.exec("UPDATE vehicle_expenses SET paid_amount = amount WHERE paid_amount IS NULL OR paid_amount = 0;");
}
if (!vehicleExpenseColumns.has("is_credit")) {
  db.exec("ALTER TABLE vehicle_expenses ADD COLUMN is_credit INTEGER NOT NULL DEFAULT 0;");
}

db.exec(
  `CREATE TABLE IF NOT EXISTS vehicle_expense_payments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    vehicle_expense_id INTEGER NOT NULL,
    payment_date TEXT NOT NULL,
    amount REAL NOT NULL DEFAULT 0,
    note TEXT,
    created_by INTEGER,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (vehicle_expense_id) REFERENCES vehicle_expenses(id) ON DELETE CASCADE,
    FOREIGN KEY (created_by) REFERENCES users(id)
  )`
);
db.exec("CREATE INDEX IF NOT EXISTS idx_vehicle_expense_payments_entry ON vehicle_expense_payments(vehicle_expense_id);");
db.exec("CREATE INDEX IF NOT EXISTS idx_vehicle_expense_payments_date ON vehicle_expense_payments(payment_date);");

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
db.exec("CREATE INDEX IF NOT EXISTS idx_staff_fingerprint ON staff(fingerprint_id);");

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
if (!exportColumns.has("credit_amount")) {
  db.exec("ALTER TABLE exports ADD COLUMN credit_amount REAL NOT NULL DEFAULT 0;");
}
if (!exportColumns.has("return_jar_count")) {
  db.exec("ALTER TABLE exports ADD COLUMN return_jar_count INTEGER NOT NULL DEFAULT 0;");
}
if (!exportColumns.has("return_bottle_case_count")) {
  db.exec("ALTER TABLE exports ADD COLUMN return_bottle_case_count INTEGER NOT NULL DEFAULT 0;");
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

const staffSalaryColumns = new Set(
  db.prepare("PRAGMA table_info(staff_salary_payments)").all().map((col) => col.name)
);
if (staffSalaryColumns.size > 0 && !staffSalaryColumns.has("receipt_no")) {
  db.exec("ALTER TABLE staff_salary_payments ADD COLUMN receipt_no TEXT;");
}

const workerSalaryColumns = new Set(
  db.prepare("PRAGMA table_info(worker_salary_payments)").all().map((col) => col.name)
);
if (workerSalaryColumns.size > 0 && !workerSalaryColumns.has("receipt_no")) {
  db.exec("ALTER TABLE worker_salary_payments ADD COLUMN receipt_no TEXT;");
}

const vehicleColumns = new Set(
  db.prepare("PRAGMA table_info(vehicles)").all().map((col) => col.name)
);
if (vehicleColumns.size > 0 && !vehicleColumns.has("is_company")) {
  db.exec("ALTER TABLE vehicles ADD COLUMN is_company INTEGER NOT NULL DEFAULT 0;");
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

const jarSalePaymentCount = db.prepare("SELECT COUNT(*) as count FROM jar_sale_payments").get().count;
if (jarSalePaymentCount === 0) {
  const existingJarSalePayments = db.prepare(
    `SELECT id, paid_amount, sale_date, created_by
     FROM jar_sales
     WHERE paid_amount > 0`
  ).all();
  const insertJarSalePayment = db.prepare(
    `INSERT INTO jar_sale_payments (jar_sale_id, payment_date, amount, note, created_by)
     VALUES (?, ?, ?, ?, ?)`
  );
  existingJarSalePayments.forEach((row) => {
    insertJarSalePayment.run(
      row.id,
      row.sale_date || new Date().toISOString().slice(0, 10),
      Number(row.paid_amount || 0),
      "Opening payment",
      row.created_by || null
    );
  });
}
db.exec(
  `INSERT INTO jar_sale_payments (jar_sale_id, payment_date, amount, note, created_by)
   SELECT jar_sales.id,
          COALESCE(NULLIF(jar_sales.sale_date, ''), date('now')),
          jar_sales.paid_amount,
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
