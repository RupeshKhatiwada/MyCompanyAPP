const buildExportCreditTotalsJoin = (exportAlias = "exports", creditAlias = "export_credit_totals") => `
LEFT JOIN (
  SELECT
    export_id,
    COALESCE(SUM(amount), 0) AS total_paid,
    COALESCE(SUM(cash_amount), 0) AS cash_amount,
    COALESCE(SUM(bank_amount), 0) AS bank_amount,
    COALESCE(SUM(ewallet_amount), 0) AS ewallet_amount
  FROM export_credit_payments
  WHERE export_id IS NOT NULL
  GROUP BY export_id
) AS ${creditAlias} ON ${creditAlias}.export_id = ${exportAlias}.id`;

const buildNonNegativeSql = (sqlExpression) => `
CASE
  WHEN (${sqlExpression}) < 0 THEN 0
  ELSE (${sqlExpression})
END`;

const buildExportRemainingCreditSql = (exportAlias = "exports") => (
  buildNonNegativeSql(`${exportAlias}.credit_amount`)
);

const buildExportRemainingCreditAggregateSql = (exportAlias = "exports") => `
COALESCE(SUM(${buildExportRemainingCreditSql(exportAlias)}), 0)`;

const buildOpeningPaymentPartSql = (fieldName, exportAlias = "exports", creditAlias = "export_credit_totals") => `
CASE
  WHEN (${exportAlias}.${fieldName} - COALESCE(${creditAlias}.${fieldName.replace("paid_", "")}, 0)) < 0 THEN 0
  ELSE (${exportAlias}.${fieldName} - COALESCE(${creditAlias}.${fieldName.replace("paid_", "")}, 0))
END`;

const buildExportOpeningPaymentAggregateSql = (exportAlias = "exports", creditAlias = "export_credit_totals") => `
COALESCE(SUM(${buildOpeningPaymentPartSql("paid_cash_amount", exportAlias, creditAlias)}), 0) AS cash_paid,
COALESCE(SUM(${buildOpeningPaymentPartSql("paid_bank_amount", exportAlias, creditAlias)}), 0) AS bank_paid,
COALESCE(SUM(${buildOpeningPaymentPartSql("paid_ewallet_amount", exportAlias, creditAlias)}), 0) AS ewallet_paid`;

const buildExportOpeningPaymentColumnsSql = (exportAlias = "exports", creditAlias = "export_credit_totals") => `
${buildOpeningPaymentPartSql("paid_cash_amount", exportAlias, creditAlias)} AS opening_cash_amount,
${buildOpeningPaymentPartSql("paid_bank_amount", exportAlias, creditAlias)} AS opening_bank_amount,
${buildOpeningPaymentPartSql("paid_ewallet_amount", exportAlias, creditAlias)} AS opening_ewallet_amount,
(
  ${buildOpeningPaymentPartSql("paid_cash_amount", exportAlias, creditAlias)} +
  ${buildOpeningPaymentPartSql("paid_bank_amount", exportAlias, creditAlias)} +
  ${buildOpeningPaymentPartSql("paid_ewallet_amount", exportAlias, creditAlias)}
) AS opening_paid_amount`;

const buildCompanyEffectiveCollectionSql = (exportAlias = "exports", vehicleAlias = "vehicles") => `
CASE
  WHEN COALESCE(${vehicleAlias}.is_company, 0) = 1
    THEN ROUND(COALESCE(${exportAlias}.collection_amount, 0) + COALESCE(${exportAlias}.sold_jar_amount, 0), 2)
  ELSE COALESCE(${exportAlias}.collection_amount, 0)
END`;

const buildCompanyEffectiveStoredAmountSql = (fieldName, exportAlias = "exports", vehicleAlias = "vehicles") => {
  const effectiveCollectionSql = buildCompanyEffectiveCollectionSql(exportAlias, vehicleAlias);
  return `
CASE
  WHEN COALESCE(${vehicleAlias}.is_company, 0) = 1
   AND COALESCE(${exportAlias}.${fieldName}, 0) < ${effectiveCollectionSql}
    THEN ${effectiveCollectionSql}
  ELSE COALESCE(${exportAlias}.${fieldName}, 0)
END`;
};

const buildCompanyMissingAmountSql = (fieldName, exportAlias = "exports", vehicleAlias = "vehicles") => `
(${buildCompanyEffectiveStoredAmountSql(fieldName, exportAlias, vehicleAlias)} - COALESCE(${exportAlias}.${fieldName}, 0))`;

module.exports = {
  buildExportCreditTotalsJoin,
  buildExportRemainingCreditSql,
  buildExportRemainingCreditAggregateSql,
  buildNonNegativeSql,
  buildExportOpeningPaymentAggregateSql,
  buildExportOpeningPaymentColumnsSql,
  buildCompanyEffectiveCollectionSql,
  buildCompanyEffectiveStoredAmountSql,
  buildCompanyMissingAmountSql
};
