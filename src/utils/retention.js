const dayjs = require("dayjs");

const archiveSpecs = [
  {
    table: "activity_logs",
    idColumn: "id",
    dateColumn: "created_at"
  },
  {
    table: "iot_attendance_logs",
    idColumn: "id",
    dateColumn: "created_at"
  },
  {
    table: "recycle_bin",
    idColumn: "id",
    dateColumn: "deleted_at",
    extraWhere: "(restored_at IS NOT NULL OR datetime(restore_until) < datetime('now'))"
  }
];

const readSetting = (db, key, fallback = "") => {
  const row = db.prepare("SELECT value FROM settings WHERE key = ?").get(key);
  return row ? String(row.value) : String(fallback);
};

const writeSetting = (db, key, value) => {
  const safeValue = String(value ?? "");
  const exists = db.prepare("SELECT key FROM settings WHERE key = ?").get(key);
  if (exists) {
    db.prepare("UPDATE settings SET value = ? WHERE key = ?").run(safeValue, key);
  } else {
    db.prepare("INSERT INTO settings (key, value) VALUES (?, ?)").run(key, safeValue);
  }
};

const parseSettingInt = (value, fallback, min, max) => {
  const raw = Number(value);
  const numeric = Number.isNaN(raw) ? fallback : raw;
  const withMin = typeof min === "number" ? Math.max(min, numeric) : numeric;
  const withMax = typeof max === "number" ? Math.min(max, withMin) : withMin;
  return Math.floor(withMax);
};

const getRetentionConfig = (db) => {
  const enabled = readSetting(db, "retention_enabled", "1") !== "0";
  const days = parseSettingInt(readSetting(db, "retention_days", "365"), 365, 30, 3650);
  const batchSize = parseSettingInt(readSetting(db, "retention_batch_size", "500"), 500, 50, 5000);
  return {
    enabled,
    days,
    batchSize
  };
};

const listArchiveRuns = (db, limit = 20) => db.prepare(
  `SELECT id, run_at, retention_days, archived_count, details
   FROM archive_runs
   ORDER BY run_at DESC, id DESC
   LIMIT ?`
).all(limit);

const countPendingRows = (db, cutoffDateText) => {
  return archiveSpecs.reduce((acc, spec) => {
    const where = [`datetime(${spec.dateColumn}) <= datetime(?)`];
    if (spec.extraWhere) where.push(spec.extraWhere);
    const sql = `SELECT COUNT(*) as count FROM ${spec.table} WHERE ${where.join(" AND ")}`;
    const row = db.prepare(sql).get(cutoffDateText);
    const count = Number(row.count || 0);
    acc.byTable[spec.table] = count;
    acc.total += count;
    return acc;
  }, { total: 0, byTable: {} });
};

const getRetentionStatus = (db) => {
  const config = getRetentionConfig(db);
  const lastRunAt = readSetting(db, "retention_last_run_at", "");
  const lastRunCount = parseSettingInt(readSetting(db, "retention_last_run_count", "0"), 0, 0, Number.MAX_SAFE_INTEGER);
  const cutoffDateText = dayjs().subtract(config.days, "day").endOf("day").format("YYYY-MM-DD HH:mm:ss");
  const pending = countPendingRows(db, cutoffDateText);
  const archivedTotalRow = db.prepare("SELECT COUNT(*) as count FROM archived_records").get();

  return {
    config,
    lastRunAt,
    lastRunCount,
    cutoffDateText,
    pendingTotal: pending.total,
    pendingByTable: pending.byTable,
    archivedTotal: Number(archivedTotalRow.count || 0)
  };
};

const archiveRowsForSpec = (db, spec, cutoffDateText, batchSize) => {
  const where = [`datetime(${spec.dateColumn}) <= datetime(?)`];
  if (spec.extraWhere) where.push(spec.extraWhere);
  const selectSql = `SELECT * FROM ${spec.table} WHERE ${where.join(" AND ")} ORDER BY ${spec.dateColumn} ASC LIMIT ?`;
  const rows = db.prepare(selectSql).all(cutoffDateText, batchSize);
  if (!rows.length) return 0;

  const insertArchive = db.prepare(
    `INSERT INTO archived_records (source_table, source_id, source_date, payload, archived_at)
     VALUES (?, ?, ?, ?, datetime('now'))`
  );
  const deleteSource = db.prepare(`DELETE FROM ${spec.table} WHERE ${spec.idColumn} = ?`);

  db.exec("BEGIN IMMEDIATE;");
  try {
    rows.forEach((row) => {
      insertArchive.run(
        spec.table,
        row[spec.idColumn] === null || typeof row[spec.idColumn] === "undefined"
          ? null
          : String(row[spec.idColumn]),
        row[spec.dateColumn] || null,
        JSON.stringify(row)
      );
      deleteSource.run(row[spec.idColumn]);
    });
    db.exec("COMMIT;");
    return rows.length;
  } catch (err) {
    db.exec("ROLLBACK;");
    throw err;
  }
};

const runRetentionArchive = (db, options = {}) => {
  const force = Boolean(options.force);
  const config = getRetentionConfig(db);
  if (!force && !config.enabled) {
    return {
      skipped: true,
      reason: "disabled",
      archivedCount: 0,
      details: {}
    };
  }

  const today = dayjs().format("YYYY-MM-DD");
  const lastRunDate = readSetting(db, "retention_last_run_date", "");
  if (!force && lastRunDate === today) {
    return {
      skipped: true,
      reason: "already_ran_today",
      archivedCount: 0,
      details: {}
    };
  }

  const cutoffDateText = dayjs().subtract(config.days, "day").endOf("day").format("YYYY-MM-DD HH:mm:ss");
  const details = {};
  let archivedCount = 0;

  archiveSpecs.forEach((spec) => {
    const moved = archiveRowsForSpec(db, spec, cutoffDateText, config.batchSize);
    details[spec.table] = moved;
    archivedCount += moved;
  });

  const nowIso = dayjs().toISOString();
  writeSetting(db, "retention_last_run_at", nowIso);
  writeSetting(db, "retention_last_run_date", today);
  writeSetting(db, "retention_last_run_count", archivedCount);

  db.prepare(
    "INSERT INTO archive_runs (run_at, retention_days, archived_count, details) VALUES (datetime('now'), ?, ?, ?)"
  ).run(config.days, archivedCount, JSON.stringify(details));

  return {
    skipped: false,
    archivedCount,
    details,
    cutoffDateText,
    config
  };
};

module.exports = {
  getRetentionConfig,
  getRetentionStatus,
  listArchiveRuns,
  runRetentionArchive
};
