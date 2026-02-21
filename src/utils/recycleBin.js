const dayjs = require("dayjs");
const { db } = require("../db");

const tableColumnsCache = new Map();
const allowedTables = new Set([
  "exports",
  "credits",
  "credit_payments",
  "import_entries",
  "jar_sales",
  "vehicle_savings",
  "company_purchases",
  "staff",
  "staff_documents",
  "staff_salary_payments",
  "staff_attendance",
  "worker_salary_payments"
]);

const ensureAllowedTable = (table) => {
  if (!allowedTables.has(table)) {
    throw new Error(`Unsupported table: ${table}`);
  }
};

const getTableColumns = (table) => {
  ensureAllowedTable(table);
  if (tableColumnsCache.has(table)) return tableColumnsCache.get(table);
  const cols = db.prepare(`PRAGMA table_info(${table})`).all().map((row) => row.name);
  tableColumnsCache.set(table, cols);
  return cols;
};

const rowExists = (table, id) => {
  if (id === undefined || id === null) return false;
  ensureAllowedTable(table);
  const row = db.prepare(`SELECT id FROM ${table} WHERE id = ?`).get(id);
  return Boolean(row);
};

const insertRow = (table, row, options = {}) => {
  ensureAllowedTable(table);
  const keepId = options.keepId !== false;
  const columns = getTableColumns(table);
  const data = {};
  columns.forEach((col) => {
    if (row[col] !== undefined) data[col] = row[col];
  });

  if (!keepId || rowExists(table, data.id)) {
    delete data.id;
  }

  const keys = Object.keys(data);
  if (keys.length === 0) {
    throw new Error(`No insertable fields for ${table}`);
  }
  const placeholders = keys.map(() => "?").join(", ");
  const values = keys.map((key) => data[key]);
  const result = db.prepare(
    `INSERT INTO ${table} (${keys.join(", ")}) VALUES (${placeholders})`
  ).run(...values);

  if (data.id !== undefined) return Number(data.id);
  return Number(result.lastInsertRowid);
};

const createRecycleEntry = ({ entityType, entityId, payload, deletedBy, note }) => {
  const safePayload = JSON.stringify(payload || {});
  const restoreUntil = dayjs().add(30, "day").format("YYYY-MM-DD HH:mm:ss");
  const result = db.prepare(
    `INSERT INTO recycle_bin (entity_type, entity_id, payload, note, deleted_by, restore_until)
     VALUES (?, ?, ?, ?, ?, ?)`
  ).run(
    String(entityType || ""),
    entityId !== undefined && entityId !== null ? String(entityId) : null,
    safePayload,
    note || null,
    deletedBy || null,
    restoreUntil
  );
  return Number(result.lastInsertRowid);
};

const listRecycleEntries = ({ q = "", entityType = "all", status = "active" } = {}) => {
  const conditions = [];
  const params = [];
  if (status === "active") {
    conditions.push("recycle_bin.restored_at IS NULL");
  } else if (status === "restored") {
    conditions.push("recycle_bin.restored_at IS NOT NULL");
  }
  if (entityType && entityType !== "all") {
    conditions.push("recycle_bin.entity_type = ?");
    params.push(entityType);
  }
  if (q && String(q).trim()) {
    conditions.push("(recycle_bin.entity_type LIKE ? OR recycle_bin.entity_id LIKE ? OR recycle_bin.note LIKE ?)");
    const like = `%${String(q).trim()}%`;
    params.push(like, like, like);
  }
  const whereSql = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";
  const rows = db.prepare(
    `SELECT recycle_bin.*,
            deleter.full_name as deleted_by_name,
            restorer.full_name as restored_by_name
     FROM recycle_bin
     LEFT JOIN users as deleter ON recycle_bin.deleted_by = deleter.id
     LEFT JOIN users as restorer ON recycle_bin.restored_by = restorer.id
     ${whereSql}
     ORDER BY recycle_bin.deleted_at DESC, recycle_bin.id DESC`
  ).all(...params);
  const now = dayjs();
  return rows.map((row) => ({
    ...row,
    is_expired: row.restore_until ? dayjs(row.restore_until).isBefore(now) : true
  }));
};

const getRecycleEntryById = (id) => db.prepare("SELECT * FROM recycle_bin WHERE id = ?").get(id);

const removeRecycleEntry = (id) => {
  db.prepare("DELETE FROM recycle_bin WHERE id = ?").run(id);
};

const markRestored = (id, userId) => {
  db.prepare(
    "UPDATE recycle_bin SET restored_at = datetime('now'), restored_by = ? WHERE id = ?"
  ).run(userId || null, id);
};

const parsePayload = (payloadText) => {
  try {
    return JSON.parse(payloadText || "{}");
  } catch (err) {
    return null;
  }
};

const restoreEntry = (entryId, userId) => {
  const entry = getRecycleEntryById(entryId);
  if (!entry) {
    throw new Error("not_found");
  }
  if (entry.restored_at) {
    throw new Error("already_restored");
  }
  if (!entry.restore_until || dayjs(entry.restore_until).isBefore(dayjs())) {
    throw new Error("expired");
  }
  const payload = parsePayload(entry.payload);
  if (!payload) {
    throw new Error("invalid_payload");
  }

  let restoredEntityId = null;
  db.exec("BEGIN IMMEDIATE");
  try {
    switch (entry.entity_type) {
      case "export": {
        restoredEntityId = insertRow("exports", payload.export || {}, { keepId: true });
        break;
      }
      case "credit": {
        const creditId = insertRow("credits", payload.credit || {}, { keepId: true });
        (payload.payments || []).forEach((paymentRow) => {
          const row = { ...paymentRow, credit_id: creditId };
          try {
            insertRow("credit_payments", row, { keepId: false });
          } catch (err) {
            // Skip problematic historical child rows and continue restore.
          }
        });
        restoredEntityId = creditId;
        break;
      }
      case "import_entry": {
        restoredEntityId = insertRow("import_entries", payload.import_entry || {}, { keepId: true });
        break;
      }
      case "jar_sale": {
        restoredEntityId = insertRow("jar_sales", payload.jar_sale || {}, { keepId: true });
        break;
      }
      case "vehicle_savings": {
        restoredEntityId = insertRow("vehicle_savings", payload.vehicle_savings || {}, { keepId: true });
        break;
      }
      case "company_purchase": {
        restoredEntityId = insertRow("company_purchases", payload.company_purchase || {}, { keepId: true });
        break;
      }
      case "staff_salary_payment": {
        restoredEntityId = insertRow("staff_salary_payments", payload.staff_salary_payment || {}, { keepId: true });
        break;
      }
      case "worker_salary_payment": {
        restoredEntityId = insertRow("worker_salary_payments", payload.worker_salary_payment || {}, { keepId: true });
        break;
      }
      case "staff": {
        const newStaffId = insertRow("staff", payload.staff || {}, { keepId: true });
        (payload.documents || []).forEach((docRow) => {
          try {
            insertRow("staff_documents", { ...docRow, staff_id: newStaffId }, { keepId: false });
          } catch (err) {
            // Continue restoring remaining rows.
          }
        });
        (payload.payments || []).forEach((paymentRow) => {
          try {
            insertRow("staff_salary_payments", { ...paymentRow, staff_id: newStaffId }, { keepId: false });
          } catch (err) {
            // Continue restoring remaining rows.
          }
        });
        (payload.attendance || []).forEach((attRow) => {
          try {
            insertRow("staff_attendance", { ...attRow, staff_id: newStaffId }, { keepId: false });
          } catch (err) {
            // Continue restoring remaining rows.
          }
        });
        restoredEntityId = newStaffId;
        break;
      }
      default:
        throw new Error("unsupported_entity_type");
    }

    markRestored(entryId, userId);
    db.exec("COMMIT");
    return {
      restoredEntityId,
      entityType: entry.entity_type
    };
  } catch (err) {
    db.exec("ROLLBACK");
    throw err;
  }
};

module.exports = {
  createRecycleEntry,
  listRecycleEntries,
  getRecycleEntryById,
  removeRecycleEntry,
  restoreEntry
};
