const ACTION_LABEL_KEYS = {
  create: "activityCreate",
  update: "activityUpdate",
  delete: "activityDelete",
  payment: "activityPayment",
  backup: "activityBackup",
  restore: "activityRestore"
};

const ENTITY_LABEL_KEYS = {
  export: "exports",
  credit: "credits",
  jar_sale: "jarSalesTitle",
  vehicle: "vehicles",
  worker: "workers",
  admin: "admins",
  jar_type: "jarTypesTitle",
  jar_cap_type: "jarCapTypesTitle",
  import_entry: "importsTitle",
  vehicle_savings: "savingsTitle",
  staff: "staffsTitle",
  staff_role: "staffRolesTitle",
  staff_salary: "salaryHistory",
  settings: "settingsTitle",
  branding: "brandImageTitle",
  recovery: "recoveryTitle",
  stock_ledger: "stockLedgerTitle",
  system: "system",
  staff_attendance: "staffAttendanceTitle",
  user_attendance: "workerAttendanceTitle",
  worker_attendance: "workerAttendanceTitle"
};

const DETAIL_LABEL_KEYS = {
  qty: "quantity",
  quantity: "quantity",
  amount: "amount",
  payment: "paymentAmount",
  paid: "paidAmount",
  credit: "creditAmount",
  total: "total",
  price: "price",
  date: "date",
  status: "status",
  type: "itemType",
  user: "user",
  note: "note"
};

const SPECIAL_DETAILS = {
  wordmark_uploaded: "activityDetailWordmarkUploaded",
  recovery_questions_saved: "activityDetailRecoveryQuestionsSaved"
};

const toTitleCase = (value) =>
  String(value || "")
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\b\w/g, (m) => m.toUpperCase());

const parseDetailPairs = (details) => {
  if (!details || typeof details !== "string" || !details.includes("=")) return null;
  const pairs = {};
  details.split(",").forEach((segment) => {
    const [rawKey, ...rest] = segment.split("=");
    if (!rawKey || rest.length === 0) return;
    const key = rawKey.trim();
    const value = rest.join("=").trim();
    if (!key || !value) return;
    pairs[key] = value;
  });
  return Object.keys(pairs).length ? pairs : null;
};

const getActionLabel = (action, t) => {
  const key = ACTION_LABEL_KEYS[action];
  return key ? t(key) : toTitleCase(action);
};

const getEntityLabel = (entityType, t) => {
  const key = ENTITY_LABEL_KEYS[entityType];
  return key ? t(key) : toTitleCase(entityType);
};

const getDetailLabel = (key, t) => {
  const translationKey = DETAIL_LABEL_KEYS[key];
  return translationKey ? t(translationKey) : toTitleCase(key);
};

const getDetailText = (row, t) => {
  const details = (row && row.details ? String(row.details).trim() : "");

  if (!details) {
    if (row.action === "backup") return t("activityDetailBackupCreated");
    if (row.action === "restore") return t("activityDetailBackupRestored");
    return t("activityNoDetails");
  }

  if (SPECIAL_DETAILS[details]) {
    return t(SPECIAL_DETAILS[details]);
  }

  const parsed = parseDetailPairs(details);
  if (parsed) {
    return Object.entries(parsed)
      .map(([key, value]) => `${getDetailLabel(key, t)}: ${value}`)
      .join(" | ");
  }

  return details.replace(/[_-]+/g, " ");
};

const formatActivityRow = (row, t) => ({
  ...row,
  actionLabel: getActionLabel(row.action, t),
  entityLabel: getEntityLabel(row.entity_type, t),
  detailLabel: getDetailText(row, t)
});

const formatActivityRows = (rows, t) => (rows || []).map((row) => formatActivityRow(row, t));

module.exports = {
  formatActivityRows
};
