#!/usr/bin/env node
const path = require("path");
const { db } = require(path.join(__dirname, "..", "src", "db"));
const { createBackupFile, testBackupFile, getLatestBackup, pruneOldBackups } = require(path.join(
  __dirname,
  "..",
  "src",
  "utils",
  "backup"
));

try {
  const created = createBackupFile({ db, prefix: "aqua_msk_manual_check" });
  const tested = testBackupFile(created.filePath);
  if (!tested.ok) {
    console.error("Backup verification FAILED");
    console.error(tested.error || "Backup test returned not ok.");
    process.exit(1);
  }

  pruneOldBackups(50);
  const latest = getLatestBackup();
  console.log("Backup verification PASSED");
  console.log(`Created: ${created.filePath}`);
  if (latest) {
    console.log(`Latest: ${latest.filePath}`);
    console.log(`Size: ${latest.size} bytes`);
  }
  console.log(`Tables: ${tested.tableCount}`);
  console.log(`Users: ${tested.usersCount}`);
} catch (err) {
  console.error("Backup verification FAILED");
  console.error(err.message || err);
  process.exit(1);
} finally {
  try {
    if (db && typeof db.close === "function") db.close();
  } catch (err) {
    // ignore close failures
  }
}
