#!/usr/bin/env node
const fs = require("fs");
const path = require("path");
const { execFileSync } = require("child_process");
const ejs = require("ejs");

const projectRoot = path.join(__dirname, "..");
const srcDir = path.join(projectRoot, "src");
const viewsDir = path.join(projectRoot, "views");
const scriptsDir = path.join(projectRoot, "scripts");

const failures = [];
const notes = [];

const walkFiles = (dir, matcher) => {
  const out = [];
  const stack = [dir];
  while (stack.length > 0) {
    const current = stack.pop();
    if (!fs.existsSync(current)) continue;
    const entries = fs.readdirSync(current, { withFileTypes: true });
    for (const entry of entries) {
      const full = path.join(current, entry.name);
      if (entry.isDirectory()) {
        stack.push(full);
      } else if (matcher(full)) {
        out.push(full);
      }
    }
  }
  return out.sort();
};

const rel = (filePath) => path.relative(projectRoot, filePath);

const checkJsSyntax = (filePath) => {
  try {
    execFileSync(process.execPath, ["--check", filePath], { stdio: "pipe" });
    return true;
  } catch (err) {
    failures.push(`JS syntax failed: ${rel(filePath)}\n${String(err.stderr || err.message || err)}`);
    return false;
  }
};

const checkEjsSyntax = (filePath) => {
  try {
    const source = fs.readFileSync(filePath, "utf8");
    ejs.compile(source, { filename: filePath });
    return true;
  } catch (err) {
    failures.push(`EJS compile failed: ${rel(filePath)}\n${err.message || err}`);
    return false;
  }
};

const ensureDir = (dirPath) => {
  try {
    fs.mkdirSync(dirPath, { recursive: true });
    notes.push(`OK dir: ${rel(dirPath)}`);
  } catch (err) {
    failures.push(`Failed creating dir ${rel(dirPath)}: ${err.message || err}`);
  }
};

const jsFiles = [
  ...walkFiles(srcDir, (file) => file.endsWith(".js")),
  ...walkFiles(scriptsDir, (file) => file.endsWith(".js"))
];
jsFiles.forEach(checkJsSyntax);

const ejsFiles = walkFiles(viewsDir, (file) => file.endsWith(".ejs"));
ejsFiles.forEach(checkEjsSyntax);

ensureDir(path.join(projectRoot, "data"));
ensureDir(path.join(projectRoot, "data", "sessions"));
ensureDir(path.join(projectRoot, "data", "backups"));

let db = null;
try {
  ({ db } = require(path.join(projectRoot, "src", "db")));
  const requiredTables = [
    "users",
    "vehicles",
    "exports",
    "credits",
    "credit_payments",
    "import_entries",
    "company_purchases",
    "vehicle_expenses",
    "day_reconciliations"
  ];
  const existing = new Set(
    db.prepare("SELECT name FROM sqlite_master WHERE type = 'table'").all().map((row) => row.name)
  );
  requiredTables.forEach((name) => {
    if (!existing.has(name)) {
      failures.push(`Missing table: ${name}`);
    }
  });
  const activeUsers = Number(
    db.prepare("SELECT COUNT(*) as count FROM users WHERE is_active = 1").get().count || 0
  );
  notes.push(`Active users: ${activeUsers}`);
} catch (err) {
  failures.push(`DB check failed: ${err.message || err}`);
} finally {
  try {
    if (db && typeof db.close === "function") db.close();
  } catch (err) {
    // ignore close failures
  }
}

if (failures.length > 0) {
  console.error("Health check FAILED");
  failures.forEach((item, idx) => {
    console.error(`\n${idx + 1}. ${item}`);
  });
  process.exit(1);
}

console.log("Health check PASSED");
console.log(`Checked JS files: ${jsFiles.length}`);
console.log(`Checked EJS files: ${ejsFiles.length}`);
notes.forEach((note) => console.log(note));
