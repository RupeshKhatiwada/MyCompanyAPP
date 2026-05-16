const dayjs = require("dayjs");

const VEHICLE_COMPLIANCE_SELECT = `SELECT
  vehicle_id,
  insurance_details,
  insurance_expiry,
  tax_expiry,
  permit_details,
  permit_expiry,
  note
FROM vehicle_compliance`;

const COMPLIANCE_FIELDS = [
  { key: "insurance_expiry", labelKey: "insuranceExpiry" },
  { key: "permit_expiry", labelKey: "permitExpiry" },
  { key: "tax_expiry", labelKey: "taxExpiry" }
];

const normalizeIsoDate = (value) => {
  const safe = String(value || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(safe)) return null;
  const parsed = dayjs(safe);
  if (!parsed.isValid()) return null;
  return parsed.format("YYYY-MM-DD") === safe ? safe : null;
};

const getVehicleCompliance = (db, vehicleId) => db.prepare(
  `${VEHICLE_COMPLIANCE_SELECT}
   WHERE vehicle_id = ?`
).get(vehicleId) || {};

const getVehicleComplianceMap = (db, vehicleIds) => {
  const ids = Array.isArray(vehicleIds)
    ? vehicleIds.map((id) => Number(id)).filter((id) => Number.isInteger(id) && id > 0)
    : [];
  if (!ids.length) return {};
  const placeholders = ids.map(() => "?").join(", ");
  const rows = db.prepare(
    `${VEHICLE_COMPLIANCE_SELECT}
     WHERE vehicle_id IN (${placeholders})`
  ).all(...ids);
  return rows.reduce((acc, row) => {
    acc[row.vehicle_id] = row;
    return acc;
  }, {});
};

const saveVehicleCompliance = (db, {
  vehicleId,
  isCompany,
  insuranceDetails,
  insuranceExpiry,
  taxExpiry,
  permitDetails,
  permitExpiry,
  note,
  userId
}) => {
  if (!vehicleId) return;
  if (!isCompany) {
    db.prepare("DELETE FROM vehicle_compliance WHERE vehicle_id = ?").run(vehicleId);
    return;
  }
  const payload = {
    insuranceDetails: String(insuranceDetails || "").trim() || null,
    insuranceExpiry: String(insuranceExpiry || "").trim() || null,
    taxExpiry: String(taxExpiry || "").trim() || null,
    permitDetails: String(permitDetails || "").trim() || null,
    permitExpiry: String(permitExpiry || "").trim() || null,
    note: String(note || "").trim() || null
  };
  const hasAnyValue = Object.values(payload).some(Boolean);
  if (!hasAnyValue) {
    db.prepare("DELETE FROM vehicle_compliance WHERE vehicle_id = ?").run(vehicleId);
    return;
  }
  db.prepare(
    `INSERT INTO vehicle_compliance (
        vehicle_id,
        insurance_details,
        insurance_expiry,
        tax_expiry,
        permit_details,
        permit_expiry,
        note,
        updated_by,
        updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
      ON CONFLICT(vehicle_id) DO UPDATE SET
        insurance_details = excluded.insurance_details,
        insurance_expiry = excluded.insurance_expiry,
        tax_expiry = excluded.tax_expiry,
        permit_details = excluded.permit_details,
        permit_expiry = excluded.permit_expiry,
        note = excluded.note,
        updated_by = excluded.updated_by,
        updated_at = datetime('now')`
  ).run(
    vehicleId,
    payload.insuranceDetails,
    payload.insuranceExpiry,
    payload.taxExpiry,
    payload.permitDetails,
    payload.permitExpiry,
    payload.note,
    userId || null
  );
};

const getVehicleComplianceAlertThresholdDays = (db, fallback = 30) => {
  const row = db.prepare("SELECT value FROM settings WHERE key = ?").get("alert_vehicle_compliance_days");
  const value = Number(row ? row.value : fallback);
  if (Number.isNaN(value)) return fallback;
  return Math.max(1, Math.floor(value));
};

const listVehicleComplianceAlerts = (db, options = {}) => {
  const todayText = normalizeIsoDate(options.today) || dayjs().format("YYYY-MM-DD");
  const today = dayjs(todayText);
  const dueSoonDays = Number.isFinite(Number(options.dueSoonDays))
    ? Math.max(1, Math.floor(Number(options.dueSoonDays)))
    : getVehicleComplianceAlertThresholdDays(db);
  const rows = db.prepare(
    `SELECT
        vehicles.id as vehicle_id,
        vehicles.vehicle_number,
        vehicles.owner_name,
        vehicle_compliance.insurance_expiry,
        vehicle_compliance.tax_expiry,
        vehicle_compliance.permit_expiry
     FROM vehicles
     LEFT JOIN vehicle_compliance ON vehicle_compliance.vehicle_id = vehicles.id
     WHERE COALESCE(vehicles.is_company, 0) = 1
       AND COALESCE(vehicles.is_active, 1) = 1
     ORDER BY vehicles.vehicle_number ASC, vehicles.owner_name ASC`
  ).all();

  const alerts = [];
  rows.forEach((row) => {
    COMPLIANCE_FIELDS.forEach((field) => {
      const expiryDate = normalizeIsoDate(row[field.key]);
      if (!expiryDate) return;
      const daysRemaining = dayjs(expiryDate).diff(today, "day");
      if (daysRemaining > dueSoonDays) return;
      alerts.push({
        vehicle_id: row.vehicle_id,
        vehicle_number: row.vehicle_number,
        owner_name: row.owner_name,
        compliance_key: field.key,
        label_key: field.labelKey,
        expiry_date: expiryDate,
        days_remaining: daysRemaining,
        status: daysRemaining < 0 ? "EXPIRED" : "DUE_SOON"
      });
    });
  });

  alerts.sort((left, right) => {
    const rankLeft = left.status === "EXPIRED" ? 0 : 1;
    const rankRight = right.status === "EXPIRED" ? 0 : 1;
    if (rankLeft !== rankRight) return rankLeft - rankRight;
    if (left.expiry_date !== right.expiry_date) return left.expiry_date.localeCompare(right.expiry_date);
    if (left.vehicle_number !== right.vehicle_number) return String(left.vehicle_number || "").localeCompare(String(right.vehicle_number || ""));
    return String(left.label_key || "").localeCompare(String(right.label_key || ""));
  });

  const vehicleIds = new Set(alerts.map((row) => row.vehicle_id));
  const expiredCount = alerts.filter((row) => row.status === "EXPIRED").length;
  return {
    dueSoonDays,
    rows: alerts,
    summary: {
      totalAlerts: alerts.length,
      expiredCount,
      dueSoonCount: alerts.length - expiredCount,
      vehiclesAffected: vehicleIds.size
    }
  };
};

module.exports = {
  getVehicleCompliance,
  getVehicleComplianceMap,
  saveVehicleCompliance,
  getVehicleComplianceAlertThresholdDays,
  listVehicleComplianceAlerts
};
