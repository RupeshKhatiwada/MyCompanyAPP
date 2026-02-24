const dayjs = require("dayjs");

let PgClient = null;
try {
  ({ Client: PgClient } = require("pg"));
} catch (err) {
  PgClient = null;
}

const HYBRID_SYNC_TABLES = [
  { name: "users", idColumn: "id" },
  { name: "vehicles", idColumn: "id" },
  { name: "exports", idColumn: "id" },
  { name: "credits", idColumn: "id" },
  { name: "credit_payments", idColumn: "id" },
  { name: "jar_types", idColumn: "id" },
  { name: "jar_cap_types", idColumn: "id" },
  { name: "jar_sales", idColumn: "id" },
  { name: "import_item_types", idColumn: "id" },
  { name: "import_entries", idColumn: "id" },
  { name: "import_payments", idColumn: "id" },
  { name: "company_purchases", idColumn: "id" },
  { name: "company_purchase_payments", idColumn: "id" },
  { name: "vehicle_expenses", idColumn: "id" },
  { name: "vehicle_expense_payments", idColumn: "id" },
  { name: "vehicle_savings", idColumn: "id" },
  { name: "staff_roles", idColumn: "id" },
  { name: "staff", idColumn: "id" },
  { name: "staff_documents", idColumn: "id" },
  { name: "worker_documents", idColumn: "id" },
  { name: "staff_attendance", idColumn: "id" },
  { name: "user_attendance", idColumn: "id" },
  { name: "staff_salary_payments", idColumn: "id" },
  { name: "worker_salary_payments", idColumn: "id" },
  { name: "water_test_reports", idColumn: "id" },
  { name: "jar_sale_payments", idColumn: "id" },
  { name: "day_closures", idColumn: "closure_date" },
  { name: "settings", idColumn: "key" }
];

const quoteIdentifier = (value) => `"${String(value || "").replace(/"/g, "\"\"")}"`;

const getSettingValue = (db, key, fallback = "") => {
  const row = db.prepare("SELECT value FROM settings WHERE key = ?").get(key);
  return row ? String(row.value || "") : String(fallback || "");
};

const setSettingValue = (db, key, value) => {
  const existing = db.prepare("SELECT key FROM settings WHERE key = ?").get(key);
  if (existing) {
    db.prepare("UPDATE settings SET value = ? WHERE key = ?").run(String(value), key);
  } else {
    db.prepare("INSERT INTO settings (key, value) VALUES (?, ?)").run(key, String(value));
  }
};

const normalizeSiteId = (value) => {
  const safe = String(value || "")
    .trim()
    .replace(/[^a-zA-Z0-9_-]/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-+|-+$/g, "");
  return safe || "";
};

const getHybridSyncStatus = (db) => {
  const enabled = getSettingValue(db, "hybrid_sync_enabled", "0") === "1";
  const postgresUrl = getSettingValue(db, "hybrid_sync_pg_url", "");
  const siteId = getSettingValue(db, "hybrid_sync_site_id", "");
  const intervalMinRaw = Number(getSettingValue(db, "hybrid_sync_interval_min", "15"));
  const intervalMin = Number.isNaN(intervalMinRaw) ? 15 : Math.max(5, Math.min(720, Math.floor(intervalMinRaw)));
  const sslEnabled = getSettingValue(db, "hybrid_sync_ssl_enabled", "1") !== "0";
  const lastRun = getSettingValue(db, "hybrid_sync_last_run", "");
  const lastAttempt = getSettingValue(db, "hybrid_sync_last_attempt", "");
  const lastStatus = getSettingValue(db, "hybrid_sync_last_status", "");
  const lastRowsRaw = Number(getSettingValue(db, "hybrid_sync_last_rows", "0"));
  const lastRows = Number.isNaN(lastRowsRaw) ? 0 : Math.max(0, Math.floor(lastRowsRaw));
  const lastDurationRaw = Number(getSettingValue(db, "hybrid_sync_last_duration_ms", "0"));
  const lastDurationMs = Number.isNaN(lastDurationRaw) ? 0 : Math.max(0, Math.floor(lastDurationRaw));
  const lastError = getSettingValue(db, "hybrid_sync_last_error", "");
  const lastReason = getSettingValue(db, "hybrid_sync_last_reason", "");
  return {
    enabled,
    postgresUrl,
    siteId,
    intervalMin,
    sslEnabled,
    lastRun,
    lastAttempt,
    lastStatus,
    lastRows,
    lastDurationMs,
    lastError,
    lastReason
  };
};

const ensureRemoteSchema = async (client) => {
  await client.query(`
    CREATE TABLE IF NOT EXISTS hybrid_records (
      site_id TEXT NOT NULL,
      table_name TEXT NOT NULL,
      record_id TEXT NOT NULL,
      payload JSONB NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (site_id, table_name, record_id)
    );
  `);
  await client.query(`
    CREATE TABLE IF NOT EXISTS hybrid_sync_runs (
      id BIGSERIAL PRIMARY KEY,
      site_id TEXT NOT NULL,
      run_reason TEXT,
      status TEXT NOT NULL,
      synced_rows INTEGER NOT NULL DEFAULT 0,
      started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      finished_at TIMESTAMPTZ,
      details TEXT
    );
  `);
  await client.query("CREATE INDEX IF NOT EXISTS idx_hybrid_records_site_table ON hybrid_records(site_id, table_name);");
  await client.query("CREATE INDEX IF NOT EXISTS idx_hybrid_sync_runs_site ON hybrid_sync_runs(site_id, started_at DESC);");
};

const buildClient = ({ postgresUrl, sslEnabled }) => {
  if (!PgClient) {
    const error = new Error("pg module is not installed");
    error.code = "PG_NOT_INSTALLED";
    throw error;
  }
  return new PgClient({
    connectionString: postgresUrl,
    ssl: sslEnabled ? { rejectUnauthorized: false } : undefined
  });
};

const syncLocalToPostgres = async ({ db, reason = "manual" }) => {
  const status = getHybridSyncStatus(db);
  if (!status.enabled) {
    const error = new Error("hybrid sync is disabled");
    error.code = "HYBRID_DISABLED";
    throw error;
  }
  if (!status.postgresUrl) {
    const error = new Error("hybrid sync PostgreSQL URL is missing");
    error.code = "HYBRID_MISSING_URL";
    throw error;
  }
  if (!status.siteId) {
    const error = new Error("hybrid sync site id is missing");
    error.code = "HYBRID_MISSING_SITE_ID";
    throw error;
  }

  const startedAt = Date.now();
  setSettingValue(db, "hybrid_sync_last_attempt", new Date().toISOString());
  const client = buildClient({
    postgresUrl: status.postgresUrl,
    sslEnabled: status.sslEnabled
  });

  let syncedRows = 0;
  const tableStats = [];
  const siteId = normalizeSiteId(status.siteId);
  try {
    await client.connect();
    await ensureRemoteSchema(client);
    await client.query("BEGIN");

    for (const table of HYBRID_SYNC_TABLES) {
      const tableNameSql = quoteIdentifier(table.name);
      const rows = db.prepare(`SELECT * FROM ${tableNameSql}`).all();
      await client.query("DELETE FROM hybrid_records WHERE site_id = $1 AND table_name = $2", [siteId, table.name]);
      for (const row of rows) {
        const recordIdRaw = row[table.idColumn];
        if (recordIdRaw === null || recordIdRaw === undefined || recordIdRaw === "") continue;
        const recordId = String(recordIdRaw);
        await client.query(
          `INSERT INTO hybrid_records (site_id, table_name, record_id, payload, updated_at)
           VALUES ($1, $2, $3, $4::jsonb, NOW())`,
          [siteId, table.name, recordId, JSON.stringify(row)]
        );
        syncedRows += 1;
      }
      tableStats.push(`${table.name}:${rows.length}`);
    }

    await client.query(
      `INSERT INTO hybrid_sync_runs (site_id, run_reason, status, synced_rows, started_at, finished_at, details)
       VALUES ($1, $2, 'SUCCESS', $3, NOW(), NOW(), $4)`,
      [siteId, reason, syncedRows, tableStats.join(", ")]
    );
    await client.query("COMMIT");

    const durationMs = Date.now() - startedAt;
    setSettingValue(db, "hybrid_sync_last_run", new Date().toISOString());
    setSettingValue(db, "hybrid_sync_last_status", "success");
    setSettingValue(db, "hybrid_sync_last_rows", syncedRows);
    setSettingValue(db, "hybrid_sync_last_duration_ms", durationMs);
    setSettingValue(db, "hybrid_sync_last_error", "");
    setSettingValue(db, "hybrid_sync_last_reason", reason);

    return {
      ok: true,
      siteId,
      syncedRows,
      durationMs,
      tableStats
    };
  } catch (err) {
    try {
      await client.query("ROLLBACK");
    } catch (rollbackError) {
      // ignore rollback failures
    }
    const durationMs = Date.now() - startedAt;
    setSettingValue(db, "hybrid_sync_last_status", "failed");
    setSettingValue(db, "hybrid_sync_last_duration_ms", durationMs);
    setSettingValue(db, "hybrid_sync_last_error", err.message || "sync_failed");
    setSettingValue(db, "hybrid_sync_last_reason", reason);
    throw err;
  } finally {
    try {
      await client.end();
    } catch (closeErr) {
      // ignore close failures
    }
  }
};

const shouldAutoSync = (db) => {
  const status = getHybridSyncStatus(db);
  if (!status.enabled || !status.postgresUrl || !status.siteId) return false;
  const intervalMin = Math.max(5, status.intervalMin || 15);
  const reference = status.lastAttempt || status.lastRun;
  if (!reference) return true;
  const parsed = dayjs(reference);
  if (!parsed.isValid()) return true;
  return dayjs().diff(parsed, "minute") >= intervalMin;
};

module.exports = {
  getHybridSyncStatus,
  normalizeSiteId,
  syncLocalToPostgres,
  shouldAutoSync
};
