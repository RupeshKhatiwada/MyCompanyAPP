const test = require("node:test");
const assert = require("node:assert/strict");
const { DatabaseSync } = require("node:sqlite");

const { getDailyCollectionBalance } = require("../src/utils/salary");

const createDb = () => {
  const db = new DatabaseSync(":memory:");
  db.exec(`
    CREATE TABLE vehicles (
      id INTEGER PRIMARY KEY,
      is_company INTEGER NOT NULL DEFAULT 0
    );
    CREATE TABLE exports (
      id INTEGER PRIMARY KEY,
      vehicle_id INTEGER NOT NULL,
      export_date TEXT NOT NULL,
      paid_cash_amount REAL NOT NULL DEFAULT 0,
      paid_bank_amount REAL NOT NULL DEFAULT 0,
      paid_ewallet_amount REAL NOT NULL DEFAULT 0,
      collection_amount REAL NOT NULL DEFAULT 0,
      sold_jar_amount REAL NOT NULL DEFAULT 0
    );
    CREATE TABLE export_credit_payments (
      id INTEGER PRIMARY KEY,
      export_id INTEGER,
      payment_date TEXT NOT NULL,
      amount REAL NOT NULL DEFAULT 0,
      cash_amount REAL NOT NULL DEFAULT 0,
      bank_amount REAL NOT NULL DEFAULT 0,
      ewallet_amount REAL NOT NULL DEFAULT 0
    );
    CREATE TABLE credit_payments (
      paid_at TEXT NOT NULL,
      amount REAL NOT NULL DEFAULT 0
    );
    CREATE TABLE rent_entries (
      rent_date TEXT NOT NULL,
      amount REAL NOT NULL DEFAULT 0,
      add_to_collection INTEGER NOT NULL DEFAULT 1
    );
    CREATE TABLE jar_sale_payments (
      payment_date TEXT NOT NULL,
      amount REAL NOT NULL DEFAULT 0
    );
    CREATE TABLE jar_container_lending_payments (
      payment_date TEXT NOT NULL,
      amount REAL NOT NULL DEFAULT 0
    );
    CREATE TABLE leakage_jar_sale_payments (
      payment_date TEXT NOT NULL,
      amount REAL NOT NULL DEFAULT 0
    );
    CREATE TABLE vehicle_savings (
      id INTEGER PRIMARY KEY,
      entry_date TEXT NOT NULL,
      amount REAL NOT NULL DEFAULT 0,
      payment_source TEXT NOT NULL DEFAULT 'DAILY_COLLECTION'
    );
    CREATE TABLE import_payments (
      payment_date TEXT NOT NULL,
      amount REAL NOT NULL DEFAULT 0,
      payment_source TEXT NOT NULL DEFAULT 'DAILY_COLLECTION'
    );
    CREATE TABLE company_purchase_payments (
      payment_date TEXT NOT NULL,
      amount REAL NOT NULL DEFAULT 0,
      payment_source TEXT NOT NULL DEFAULT 'DAILY_COLLECTION'
    );
    CREATE TABLE vehicle_expense_payments (
      payment_date TEXT NOT NULL,
      amount REAL NOT NULL DEFAULT 0,
      payment_source TEXT NOT NULL DEFAULT 'DAILY_COLLECTION'
    );
    CREATE TABLE staff_salary_payments (
      id INTEGER PRIMARY KEY,
      payment_date TEXT NOT NULL,
      amount REAL NOT NULL DEFAULT 0,
      payment_source TEXT NOT NULL DEFAULT 'DAILY_COLLECTION'
    );
    CREATE TABLE worker_salary_payments (
      id INTEGER PRIMARY KEY,
      payment_date TEXT NOT NULL,
      amount REAL NOT NULL DEFAULT 0,
      payment_source TEXT NOT NULL DEFAULT 'DAILY_COLLECTION'
    );
    CREATE TABLE jar_container_lending_returns (
      return_date TEXT NOT NULL,
      refund_amount REAL NOT NULL DEFAULT 0
    );
  `);
  return db;
};

test("daily collection balance uses cash + bank + e-wallet minus daily collection deductions", () => {
  const db = createDb();
  db.prepare("INSERT INTO vehicles (id, is_company) VALUES (?, ?)").run(1, 1);
  db.prepare("INSERT INTO vehicles (id, is_company) VALUES (?, ?)").run(2, 0);

  db.prepare(
    `INSERT INTO exports
     (id, vehicle_id, export_date, paid_cash_amount, paid_bank_amount, paid_ewallet_amount, collection_amount, sold_jar_amount)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
  ).run(1, 1, "2026-05-10", 0, 0, 0, 2000, 500);

  db.prepare(
    `INSERT INTO exports
     (id, vehicle_id, export_date, paid_cash_amount, paid_bank_amount, paid_ewallet_amount, collection_amount, sold_jar_amount)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
  ).run(2, 2, "2026-05-10", 1200, 0, 0, 0, 0);
  db.prepare(
    "INSERT INTO export_credit_payments (export_id, payment_date, amount, cash_amount, bank_amount, ewallet_amount) VALUES (?, ?, ?, ?, ?, ?)"
  ).run(2, "2026-05-10", 200, 200, 0, 0);

  db.prepare("INSERT INTO staff_salary_payments (id, payment_date, amount, payment_source) VALUES (?, ?, ?, ?)")
    .run(1, "2026-05-10", 700, "DAILY_COLLECTION");
  db.prepare("INSERT INTO vehicle_expense_payments (payment_date, amount, payment_source) VALUES (?, ?, ?)")
    .run("2026-05-10", 300, "DAILY_COLLECTION");
  db.prepare("INSERT INTO import_payments (payment_date, amount, payment_source) VALUES (?, ?, ?)")
    .run("2026-05-10", 500, "OWNER_PERSONAL");

  assert.equal(getDailyCollectionBalance(db, "2026-05-10"), 2700);
});
