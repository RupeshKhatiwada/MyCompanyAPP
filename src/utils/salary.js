const dayjs = require("dayjs");

const salaryConfigByPersonType = {
  STAFF: {
    table: "staff_salary_adjustments",
    foreignKey: "staff_id"
  },
  WORKER: {
    table: "worker_salary_adjustments",
    foreignKey: "worker_id"
  }
};

const normalizePersonType = (value) => {
  const safe = String(value || "").trim().toUpperCase();
  return safe === "WORKER" ? "WORKER" : "STAFF";
};

const toMoneyCents = (value, { allowNegative = false } = {}) => {
  const num = Number(value || 0);
  if (Number.isNaN(num)) return 0;
  const safe = allowNegative ? num : Math.max(0, num);
  return Math.round((safe + Number.EPSILON) * 100);
};

const fromMoneyCents = (value) => Number((Number(value || 0) / 100).toFixed(2));

const roundMoney = (value, options = {}) => fromMoneyCents(toMoneyCents(value, options));

const parseWholeMoney = (value, { allowNegative = false } = {}) => {
  const num = Number(value || 0);
  if (Number.isNaN(num)) return 0;
  const rounded = Math.round(num);
  return allowNegative ? rounded : Math.max(0, rounded);
};

const normalizeMonthStart = (value) => {
  const parsed = dayjs(value).startOf("month");
  return parsed.isValid() ? parsed : null;
};

const getSalaryAdjustments = (db, personType, personId) => {
  if (!personId) return [];
  const safeType = normalizePersonType(personType);
  const config = salaryConfigByPersonType[safeType];
  return db.prepare(
    `SELECT id, effective_month, previous_salary, new_salary, note, created_by, created_at
     FROM ${config.table}
     WHERE ${config.foreignKey} = ?
     ORDER BY effective_month ASC, id ASC`
  ).all(personId).map((row) => ({
    ...row,
    previous_salary: roundMoney(row.previous_salary || 0),
    new_salary: roundMoney(row.new_salary || 0)
  }));
};

const getBaselineSalary = (adjustments, fallbackSalary) => (
  adjustments.length ? roundMoney(adjustments[0].previous_salary || 0) : roundMoney(fallbackSalary || 0)
);

const getMonthlySalaryForMonth = (db, options = {}) => {
  const personRow = options.personRow || null;
  const monthStart = normalizeMonthStart(options.month);
  if (!personRow || !monthStart) return roundMoney(personRow?.monthly_salary || 0);
  const adjustments = Array.isArray(options.adjustments)
    ? options.adjustments
    : getSalaryAdjustments(db, options.personType, personRow.id);

  let salaryCents = toMoneyCents(getBaselineSalary(adjustments, personRow.monthly_salary || 0));
  adjustments.forEach((row) => {
    const effectiveMonth = normalizeMonthStart(row.effective_month);
    if (!effectiveMonth || effectiveMonth.isAfter(monthStart, "day")) return;
    salaryCents = toMoneyCents(row.new_salary || 0);
  });
  return fromMoneyCents(salaryCents);
};

const computeAccruedSalaryUntilMonth = (db, options = {}) => {
  const personRow = options.personRow || null;
  if (!personRow || !personRow.start_date) return 0;
  const startMonth = normalizeMonthStart(personRow.start_date);
  const targetMonth = normalizeMonthStart(options.month);
  if (!startMonth || !targetMonth || targetMonth.isBefore(startMonth, "month")) return 0;

  const adjustments = Array.isArray(options.adjustments)
    ? options.adjustments
    : getSalaryAdjustments(db, options.personType, personRow.id);

  let totalCents = 0;
  let cursor = startMonth;
  while (!cursor.isAfter(targetMonth, "month")) {
    totalCents += toMoneyCents(getMonthlySalaryForMonth(db, {
      personType: options.personType,
      personRow,
      month: cursor,
      adjustments
    }));
    cursor = cursor.add(1, "month");
  }
  return fromMoneyCents(totalCents);
};

const computeSalaryDue = (db, options = {}) => {
  const personRow = options.personRow || null;
  if (!personRow || !personRow.start_date) return 0;
  const lastCompletedMonth = dayjs(options.asOf).startOf("month").subtract(1, "month");
  const startMonth = normalizeMonthStart(personRow.start_date);
  if (!startMonth || !lastCompletedMonth.isValid() || lastCompletedMonth.isBefore(startMonth, "month")) return 0;

  const accruedCents = toMoneyCents(computeAccruedSalaryUntilMonth(db, {
    personType: options.personType,
    personRow,
    month: lastCompletedMonth,
    adjustments: options.adjustments
  }));
  const paidCents = toMoneyCents(options.paidTotal || 0);
  return fromMoneyCents(Math.max(0, accruedCents - paidCents));
};

const recordSalaryAdjustment = (db, options = {}) => {
  const personId = options.personId;
  if (!personId) return null;
  const safeType = normalizePersonType(options.personType);
  const config = salaryConfigByPersonType[safeType];
  const previousSalary = parseWholeMoney(options.previousSalary || 0);
  const newSalary = parseWholeMoney(options.newSalary || 0);
  const effectiveMonth = normalizeMonthStart(options.effectiveMonth || dayjs());
  if (!effectiveMonth || previousSalary === newSalary) return null;

  const result = db.prepare(
    `INSERT INTO ${config.table} (${config.foreignKey}, effective_month, previous_salary, new_salary, note, created_by)
     VALUES (?, ?, ?, ?, ?, ?)`
  ).run(
    personId,
    effectiveMonth.format("YYYY-MM-DD"),
    previousSalary,
    newSalary,
    options.note ? String(options.note).trim() || null : null,
    options.createdBy || null
  );
  return Number(result.lastInsertRowid);
};

const getDailyCollectionBalance = (db, businessDate, options = {}) => {
  const safeDate = String(businessDate || "").trim();
  if (!safeDate) return 0;

  const sumScalar = (sql, ...params) => {
    const row = db.prepare(sql).get(...params);
    return toMoneyCents(row?.amount || 0, { allowNegative: true });
  };

  const inflowCents = [
    sumScalar(
      `SELECT
          COALESCE(SUM(paid_cash_amount), 0) +
          COALESCE(SUM(paid_bank_amount), 0) +
          COALESCE(SUM(paid_ewallet_amount), 0) AS amount
       FROM exports
       WHERE export_date = ?`,
      safeDate
    ),
    sumScalar(
      `SELECT COALESCE(SUM(amount), 0) AS amount
       FROM credit_payments
       WHERE date(paid_at) = ?`,
      safeDate
    ),
    sumScalar(
      `SELECT COALESCE(SUM(CASE WHEN add_to_collection = 1 THEN amount ELSE 0 END), 0) AS amount
       FROM rent_entries
       WHERE rent_date = ?`,
      safeDate
    ),
    sumScalar(
      `SELECT COALESCE(SUM(amount), 0) AS amount
       FROM jar_sale_payments
       WHERE payment_date = ?`,
      safeDate
    ),
    sumScalar(
      `SELECT COALESCE(SUM(amount), 0) AS amount
       FROM leakage_jar_sale_payments
       WHERE payment_date = ?`,
      safeDate
    ),
    sumScalar(
      `SELECT COALESCE(SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END), 0) AS amount
       FROM vehicle_savings
       WHERE entry_date = ?`,
      safeDate
    )
  ].reduce((sum, amount) => sum + amount, 0);

  const staffSalarySql = options.excludeStaffPaymentId
    ? "SELECT COALESCE(SUM(amount), 0) AS amount FROM staff_salary_payments WHERE payment_date = ? AND payment_source = 'DAILY_COLLECTION' AND id != ?"
    : "SELECT COALESCE(SUM(amount), 0) AS amount FROM staff_salary_payments WHERE payment_date = ? AND payment_source = 'DAILY_COLLECTION'";
  const workerSalarySql = options.excludeWorkerPaymentId
    ? "SELECT COALESCE(SUM(amount), 0) AS amount FROM worker_salary_payments WHERE payment_date = ? AND payment_source = 'DAILY_COLLECTION' AND id != ?"
    : "SELECT COALESCE(SUM(amount), 0) AS amount FROM worker_salary_payments WHERE payment_date = ? AND payment_source = 'DAILY_COLLECTION'";

  const deductionCents = [
    sumScalar(
      "SELECT COALESCE(SUM(amount), 0) AS amount FROM import_payments WHERE payment_date = ? AND payment_source = 'DAILY_COLLECTION'",
      safeDate
    ),
    sumScalar(
      "SELECT COALESCE(SUM(amount), 0) AS amount FROM company_purchase_payments WHERE payment_date = ? AND payment_source = 'DAILY_COLLECTION'",
      safeDate
    ),
    sumScalar(
      "SELECT COALESCE(SUM(amount), 0) AS amount FROM vehicle_expense_payments WHERE payment_date = ? AND payment_source = 'DAILY_COLLECTION'",
      safeDate
    ),
    options.excludeStaffPaymentId
      ? sumScalar(staffSalarySql, safeDate, options.excludeStaffPaymentId)
      : sumScalar(staffSalarySql, safeDate),
    options.excludeWorkerPaymentId
      ? sumScalar(workerSalarySql, safeDate, options.excludeWorkerPaymentId)
      : sumScalar(workerSalarySql, safeDate),
    sumScalar(
      `SELECT COALESCE(SUM(CASE WHEN amount < 0 AND payment_source = 'DAILY_COLLECTION' THEN ABS(amount) ELSE 0 END), 0) AS amount
       FROM vehicle_savings
       WHERE entry_date = ?`,
      safeDate
    )
  ].reduce((sum, amount) => sum + amount, 0);

  return fromMoneyCents(inflowCents - deductionCents);
};

module.exports = {
  computeAccruedSalaryUntilMonth,
  computeSalaryDue,
  getDailyCollectionBalance,
  getMonthlySalaryForMonth,
  getSalaryAdjustments,
  parseWholeMoney,
  recordSalaryAdjustment,
  roundMoney
};
