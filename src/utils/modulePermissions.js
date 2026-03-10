const MODULE_PERMISSION_ITEMS = [
  { key: "exports", labelKey: "moduleExports" },
  { key: "credits", labelKey: "moduleCredits" },
  { key: "imports", labelKey: "moduleImports" },
  { key: "invoices", labelKey: "moduleInvoices" },
  { key: "jar_sales", labelKey: "moduleJarSales" },
  { key: "rentals", labelKey: "moduleRentals" },
  { key: "company_expenses", labelKey: "moduleCompanyExpenses" },
  { key: "vehicle_expenses", labelKey: "moduleVehicleExpenses" },
  { key: "payment_ledger", labelKey: "modulePaymentLedger" },
  { key: "vendor_aging", labelKey: "moduleVendorAging" },
  { key: "savings", labelKey: "moduleSavings" },
  { key: "reconciliation", labelKey: "moduleReconciliation" },
  { key: "water_tests", labelKey: "moduleWaterTests" },
  { key: "vehicles", labelKey: "moduleVehicles" },
  { key: "staffs", labelKey: "moduleStaffs" },
  { key: "attendance", labelKey: "moduleAttendance" },
  { key: "history", labelKey: "moduleHistory" }
];

const ROLE_PERMISSION_ROLES = ["ADMIN", "WORKER"];
const MODULE_KEYS = new Set(MODULE_PERMISSION_ITEMS.map((row) => row.key));

const normalizeRole = (role) => String(role || "").trim().toUpperCase();

const getDefaultRoleMap = (role) => {
  const safeRole = normalizeRole(role);
  const fullAccess = safeRole === "SUPER_ADMIN" || safeRole === "ADMIN" || safeRole === "WORKER";
  return MODULE_PERMISSION_ITEMS.reduce((acc, item) => {
    acc[item.key] = { view: fullAccess, edit: fullAccess };
    return acc;
  }, {});
};

const ensureRolePermissionRows = (db) => {
  const insert = db.prepare(
    `INSERT INTO role_permissions (role, module_key, can_view, can_edit, updated_at)
     VALUES (?, ?, ?, ?, datetime('now'))
     ON CONFLICT(role, module_key) DO NOTHING`
  );
  ROLE_PERMISSION_ROLES.forEach((role) => {
    MODULE_PERMISSION_ITEMS.forEach((item) => {
      const defaults = getDefaultRoleMap(role)[item.key];
      insert.run(role, item.key, defaults.view ? 1 : 0, defaults.edit ? 1 : 0);
    });
  });
};

const getRolePermissionMap = (db, role) => {
  const safeRole = normalizeRole(role);
  const defaults = getDefaultRoleMap(safeRole);
  if (safeRole === "SUPER_ADMIN") return defaults;
  if (!ROLE_PERMISSION_ROLES.includes(safeRole)) return defaults;
  ensureRolePermissionRows(db);
  const rows = db.prepare(
    "SELECT module_key, can_view, can_edit FROM role_permissions WHERE role = ?"
  ).all(safeRole);
  rows.forEach((row) => {
    const moduleKey = String(row.module_key || "").trim();
    if (!MODULE_KEYS.has(moduleKey)) return;
    defaults[moduleKey] = {
      view: Number(row.can_view) === 1,
      edit: Number(row.can_edit) === 1
    };
  });
  return defaults;
};

const canRoleAccessModule = (db, role, moduleKey, action = "view", roleMap = null) => {
  const safeRole = normalizeRole(role);
  if (safeRole === "SUPER_ADMIN") return true;
  const safeModule = String(moduleKey || "").trim();
  if (!safeModule || !MODULE_KEYS.has(safeModule)) return true;
  const map = roleMap || getRolePermissionMap(db, safeRole);
  const permissions = map[safeModule] || { view: true, edit: true };
  if (action === "edit") return Boolean(permissions.view && permissions.edit);
  return Boolean(permissions.view);
};

module.exports = {
  MODULE_PERMISSION_ITEMS,
  ROLE_PERMISSION_ROLES,
  ensureRolePermissionRows,
  getRolePermissionMap,
  canRoleAccessModule
};
