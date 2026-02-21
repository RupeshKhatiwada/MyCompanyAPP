const fs = require("fs");
const path = require("path");
const { DatabaseSync } = require("node:sqlite");

const backupDir = path.join(__dirname, "..", "..", "data", "backups");

const ensureBackupDir = () => {
  fs.mkdirSync(backupDir, { recursive: true });
};

const escapeSqliteLiteral = (value) => String(value).replace(/'/g, "''");

const formatStamp = () => {
  const now = new Date();
  const parts = [
    now.getFullYear(),
    String(now.getMonth() + 1).padStart(2, "0"),
    String(now.getDate()).padStart(2, "0")
  ];
  const time = [
    String(now.getHours()).padStart(2, "0"),
    String(now.getMinutes()).padStart(2, "0"),
    String(now.getSeconds()).padStart(2, "0")
  ];
  return `${parts.join("-")}_${time.join("-")}`;
};

const createBackupFile = ({ db, prefix = "aqua_msk_backup" }) => {
  ensureBackupDir();
  const stamp = formatStamp();
  const filename = `${prefix}_${stamp}.db`;
  const filePath = path.join(backupDir, filename);
  const escapedPath = escapeSqliteLiteral(filePath);
  db.exec(`VACUUM INTO '${escapedPath}'`);
  return { filePath, filename };
};

const listBackupFiles = () => {
  ensureBackupDir();
  const entries = fs.readdirSync(backupDir)
    .filter((name) => name.endsWith(".db"))
    .map((name) => {
      const filePath = path.join(backupDir, name);
      const stat = fs.statSync(filePath);
      return {
        name,
        filePath,
        size: stat.size,
        mtimeMs: stat.mtimeMs
      };
    })
    .sort((a, b) => b.mtimeMs - a.mtimeMs);
  return entries;
};

const getLatestBackup = () => listBackupFiles()[0] || null;

const pruneOldBackups = (keepCount = 30) => {
  const list = listBackupFiles();
  const toDelete = list.slice(Math.max(0, keepCount));
  toDelete.forEach((row) => {
    try {
      fs.rmSync(row.filePath, { force: true });
    } catch (err) {
      // ignore cleanup errors
    }
  });
};

const testBackupFile = (filePath) => {
  if (!filePath || !fs.existsSync(filePath)) {
    return { ok: false, error: "Backup file not found." };
  }
  let tempDb = null;
  try {
    tempDb = new DatabaseSync(filePath);
    const tableCount = Number(
      tempDb.prepare("SELECT COUNT(*) as count FROM sqlite_master WHERE type = 'table'").get().count || 0
    );
    const usersCount = Number(tempDb.prepare("SELECT COUNT(*) as count FROM users").get().count || 0);
    return {
      ok: tableCount > 0,
      tableCount,
      usersCount
    };
  } catch (err) {
    return { ok: false, error: err.message || "Backup test failed." };
  } finally {
    if (tempDb && typeof tempDb.close === "function") {
      tempDb.close();
    }
  }
};

module.exports = {
  backupDir,
  ensureBackupDir,
  createBackupFile,
  listBackupFiles,
  getLatestBackup,
  pruneOldBackups,
  testBackupFile
};
