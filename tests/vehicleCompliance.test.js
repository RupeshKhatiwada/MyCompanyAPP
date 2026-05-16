const test = require("node:test");
const assert = require("node:assert/strict");
const { DatabaseSync } = require("node:sqlite");

const {
  getVehicleCompliance,
  getVehicleComplianceMap,
  saveVehicleCompliance,
  listVehicleComplianceAlerts
} = require("../src/utils/vehicleCompliance");

const createDb = () => {
  const db = new DatabaseSync(":memory:");
  db.exec(`
    CREATE TABLE settings (
      key TEXT PRIMARY KEY,
      value TEXT
    );
    CREATE TABLE vehicles (
      id INTEGER PRIMARY KEY,
      vehicle_number TEXT NOT NULL,
      owner_name TEXT NOT NULL,
      is_company INTEGER NOT NULL DEFAULT 0,
      is_active INTEGER NOT NULL DEFAULT 1
    );
    CREATE TABLE vehicle_compliance (
      vehicle_id INTEGER PRIMARY KEY,
      insurance_details TEXT,
      insurance_expiry TEXT,
      tax_expiry TEXT,
      permit_details TEXT,
      permit_expiry TEXT,
      note TEXT,
      updated_by INTEGER,
      updated_at TEXT
    );
  `);
  return db;
};

test("saveVehicleCompliance stores and reads company compliance including tax expiry", () => {
  const db = createDb();
  db.prepare("INSERT INTO vehicles (id, vehicle_number, owner_name, is_company, is_active) VALUES (?, ?, ?, ?, ?)")
    .run(1, "BA-1-PA-1234", "Nabin", 1, 1);

  saveVehicleCompliance(db, {
    vehicleId: 1,
    isCompany: true,
    insuranceDetails: "POL-1",
    insuranceExpiry: "2026-05-10",
    taxExpiry: "2026-05-20",
    permitDetails: "PERMIT-1",
    permitExpiry: "2026-05-15",
    note: "Check before renewal",
    userId: 9
  });

  const record = getVehicleCompliance(db, 1);
  assert.equal(record.insurance_details, "POL-1");
  assert.equal(record.tax_expiry, "2026-05-20");

  const map = getVehicleComplianceMap(db, [1]);
  assert.equal(map[1].permit_expiry, "2026-05-15");
});

test("saveVehicleCompliance removes records when company compliance is cleared", () => {
  const db = createDb();
  db.prepare("INSERT INTO vehicles (id, vehicle_number, owner_name, is_company, is_active) VALUES (?, ?, ?, ?, ?)")
    .run(1, "BA-2-PA-5678", "Sita", 1, 1);

  saveVehicleCompliance(db, {
    vehicleId: 1,
    isCompany: true,
    insuranceExpiry: "2026-05-10",
    taxExpiry: "",
    permitExpiry: "",
    note: "",
    userId: 4
  });
  assert.equal(Boolean(getVehicleCompliance(db, 1).insurance_expiry), true);

  saveVehicleCompliance(db, {
    vehicleId: 1,
    isCompany: false,
    insuranceExpiry: "2026-05-10"
  });
  assert.deepEqual(getVehicleCompliance(db, 1), {});
});

test("listVehicleComplianceAlerts returns expired and due soon document reminders", () => {
  const db = createDb();
  db.prepare("INSERT INTO vehicles (id, vehicle_number, owner_name, is_company, is_active) VALUES (?, ?, ?, ?, ?)")
    .run(1, "BA-1-PA-1234", "Nabin", 1, 1);
  db.prepare("INSERT INTO vehicles (id, vehicle_number, owner_name, is_company, is_active) VALUES (?, ?, ?, ?, ?)")
    .run(2, "BA-2-PA-5678", "Sita", 1, 1);
  db.prepare("INSERT INTO vehicles (id, vehicle_number, owner_name, is_company, is_active) VALUES (?, ?, ?, ?, ?)")
    .run(3, "BA-3-PA-9999", "Ram", 0, 1);

  saveVehicleCompliance(db, {
    vehicleId: 1,
    isCompany: true,
    insuranceExpiry: "2026-04-10",
    taxExpiry: "2026-05-01",
    permitExpiry: "2026-04-25"
  });
  saveVehicleCompliance(db, {
    vehicleId: 2,
    isCompany: true,
    insuranceExpiry: "2026-04-22"
  });
  saveVehicleCompliance(db, {
    vehicleId: 3,
    isCompany: false,
    insuranceExpiry: "2026-04-21"
  });

  const alerts = listVehicleComplianceAlerts(db, {
    today: "2026-04-20",
    dueSoonDays: 10
  });

  assert.equal(alerts.summary.totalAlerts, 3);
  assert.equal(alerts.summary.expiredCount, 1);
  assert.equal(alerts.summary.dueSoonCount, 2);
  assert.equal(alerts.summary.vehiclesAffected, 2);
  assert.equal(alerts.rows[0].status, "EXPIRED");
  assert.equal(alerts.rows[0].label_key, "insuranceExpiry");
  assert.equal(alerts.rows[1].vehicle_number, "BA-2-PA-5678");
});
