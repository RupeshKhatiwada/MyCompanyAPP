const moneyTolerance = 0.01;

const toMoney = (value) => {
  const num = Number(value || 0);
  if (!Number.isFinite(num)) return 0;
  return Math.round((num + Number.EPSILON) * 100) / 100;
};

const formatMoney = (value) => toMoney(value).toFixed(2);

const buildExample = (row, amountKey = "delta") => {
  const source = String(row.source || "").trim();
  const ref = String(row.reference_no || "").trim() || `#${row.record_id}`;
  const date = String(row.entry_date || "").trim() || "-";
  return `${source} ${ref} • ${date} • ${formatMoney(row[amountKey] || 0)}`;
};

const getFinancialIntegrityReport = (db, options = {}) => {
  const limit = Math.max(1, Math.min(20, Number(options.limit || 5)));

  const exportSplitRows = db.prepare(
    `SELECT id as record_id,
            'EXPORT' as source,
            export_date as entry_date,
            COALESCE(receipt_no, '') as reference_no,
            ABS(COALESCE(total_amount, 0) - (COALESCE(paid_amount, 0) + COALESCE(credit_amount, 0))) as delta
     FROM exports
     WHERE ABS(COALESCE(total_amount, 0) - (COALESCE(paid_amount, 0) + COALESCE(credit_amount, 0))) > ?
     ORDER BY delta DESC, export_date DESC, id DESC`
  ).all(moneyTolerance);

  const exportChannelRows = db.prepare(
    `SELECT id as record_id,
            'EXPORT' as source,
            export_date as entry_date,
            COALESCE(receipt_no, '') as reference_no,
            ABS(
              COALESCE(paid_amount, 0) - (
                COALESCE(paid_cash_amount, 0) +
                COALESCE(paid_bank_amount, 0) +
                COALESCE(paid_ewallet_amount, 0)
              )
            ) as delta
     FROM exports
     WHERE ABS(
            COALESCE(paid_amount, 0) - (
              COALESCE(paid_cash_amount, 0) +
              COALESCE(paid_bank_amount, 0) +
              COALESCE(paid_ewallet_amount, 0)
            )
          ) > ?
     ORDER BY delta DESC, export_date DESC, id DESC`
  ).all(moneyTolerance);

  const overpaidRows = db.prepare(
    `SELECT *
     FROM (
       SELECT 'CREDIT' as source,
              credits.id as record_id,
              credits.credit_date as entry_date,
              COALESCE(credits.receipt_no, '') as reference_no,
              (COALESCE(credits.paid_amount, 0) - COALESCE(credits.amount, 0)) as delta
       FROM credits
       WHERE COALESCE(credits.paid_amount, 0) > COALESCE(credits.amount, 0) + ?

       UNION ALL

       SELECT 'JAR_SALE' as source,
              jar_sales.id as record_id,
              jar_sales.sale_date as entry_date,
              '' as reference_no,
              (COALESCE(jar_sales.paid_amount, 0) - COALESCE(jar_sales.total_amount, 0)) as delta
       FROM jar_sales
       WHERE COALESCE(jar_sales.paid_amount, 0) > COALESCE(jar_sales.total_amount, 0) + ?

       UNION ALL

       SELECT 'LEAKAGE_SALE' as source,
              leakage_jar_sales.id as record_id,
              leakage_jar_sales.sale_date as entry_date,
              '' as reference_no,
              (COALESCE(leakage_jar_sales.paid_amount, 0) - COALESCE(leakage_jar_sales.total_amount, 0)) as delta
       FROM leakage_jar_sales
       WHERE COALESCE(leakage_jar_sales.paid_amount, 0) > COALESCE(leakage_jar_sales.total_amount, 0) + ?

       UNION ALL

       SELECT 'IMPORT' as source,
              import_entries.id as record_id,
              import_entries.entry_date as entry_date,
              '' as reference_no,
              (COALESCE(import_entries.paid_amount, 0) - COALESCE(import_entries.total_amount, 0)) as delta
       FROM import_entries
       WHERE COALESCE(import_entries.paid_amount, 0) > COALESCE(import_entries.total_amount, 0) + ?

       UNION ALL

       SELECT 'COMPANY_PURCHASE' as source,
              company_purchases.id as record_id,
              company_purchases.purchase_date as entry_date,
              '' as reference_no,
              (COALESCE(company_purchases.paid_amount, 0) - COALESCE(company_purchases.amount, 0)) as delta
       FROM company_purchases
       WHERE COALESCE(company_purchases.paid_amount, 0) > COALESCE(company_purchases.amount, 0) + ?

       UNION ALL

       SELECT 'VEHICLE_EXPENSE' as source,
              vehicle_expenses.id as record_id,
              vehicle_expenses.expense_date as entry_date,
              '' as reference_no,
              (COALESCE(vehicle_expenses.paid_amount, 0) - COALESCE(vehicle_expenses.amount, 0)) as delta
       FROM vehicle_expenses
       WHERE COALESCE(vehicle_expenses.paid_amount, 0) > COALESCE(vehicle_expenses.amount, 0) + ?
     ) overpaid_rows
     ORDER BY delta DESC, entry_date DESC, record_id DESC`
  ).all(
    moneyTolerance,
    moneyTolerance,
    moneyTolerance,
    moneyTolerance,
    moneyTolerance,
    moneyTolerance
  );

  const groups = [
    {
      key: "export_split_mismatch",
      labelKey: "integrityIssueExportTotals",
      count: exportSplitRows.length,
      examples: exportSplitRows.slice(0, limit).map((row) => buildExample(row))
    },
    {
      key: "export_channel_mismatch",
      labelKey: "integrityIssueExportChannels",
      count: exportChannelRows.length,
      examples: exportChannelRows.slice(0, limit).map((row) => buildExample(row))
    },
    {
      key: "overpaid_records",
      labelKey: "integrityIssueOverpaidRecords",
      count: overpaidRows.length,
      examples: overpaidRows.slice(0, limit).map((row) => buildExample(row))
    }
  ];

  const totalIssues = groups.reduce((sum, group) => sum + Number(group.count || 0), 0);

  return {
    hasIssues: totalIssues > 0,
    totalIssues,
    groups
  };
};

module.exports = {
  getFinancialIntegrityReport
};
