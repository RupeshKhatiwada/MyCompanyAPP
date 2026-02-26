const dayjs = require("dayjs");

const readSettingNumber = (db, key, fallback, min, max) => {
  const row = db.prepare("SELECT value FROM settings WHERE key = ?").get(key);
  const raw = row ? Number(row.value) : Number(fallback);
  const normalized = Number.isNaN(raw) ? Number(fallback) : raw;
  const withMin = typeof min === "number" ? Math.max(min, normalized) : normalized;
  return typeof max === "number" ? Math.min(max, withMin) : withMin;
};

const normalizePrefix = (value, fallback = "DOC") => {
  const safe = String(value || fallback)
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "");
  return safe || fallback;
};

const getNumberingConfig = (db) => {
  const fiscalStartMonth = Math.floor(readSettingNumber(db, "numbering_fiscal_start_month", 7, 1, 12));
  const sequencePad = Math.floor(readSettingNumber(db, "numbering_sequence_pad", 5, 3, 8));
  return {
    fiscalStartMonth,
    sequencePad
  };
};

const getFiscalYearLabel = (dateText, fiscalStartMonth = 7) => {
  const parsed = dayjs(dateText);
  const date = parsed.isValid() ? parsed : dayjs();
  const month = date.month() + 1;
  let startYear = date.year();
  if (month < fiscalStartMonth) {
    startYear -= 1;
  }
  const endYear = startYear + 1;
  const shortEndYear = String(endYear).slice(-2);
  return `${startYear}-${shortEndYear}`;
};

const issueSequence = (db, docType, fiscalYear) => {
  db.exec("BEGIN IMMEDIATE;");
  try {
    const existing = db.prepare(
      "SELECT id, next_value FROM doc_number_sequences WHERE doc_type = ? AND fiscal_year = ?"
    ).get(docType, fiscalYear);

    let value = 1;
    if (!existing) {
      db.prepare(
        "INSERT INTO doc_number_sequences (doc_type, fiscal_year, next_value, updated_at) VALUES (?, ?, 2, datetime('now'))"
      ).run(docType, fiscalYear);
    } else {
      value = Math.max(1, Number(existing.next_value || 1));
      db.prepare(
        "UPDATE doc_number_sequences SET next_value = ?, updated_at = datetime('now') WHERE id = ?"
      ).run(value + 1, existing.id);
    }

    db.exec("COMMIT;");
    return value;
  } catch (err) {
    db.exec("ROLLBACK;");
    throw err;
  }
};

const generateDocumentNumber = (db, options = {}) => {
  const config = getNumberingConfig(db);
  const docType = normalizePrefix(options.docType, "DOC");
  const prefix = normalizePrefix(options.prefix, docType);
  const fiscalYear = getFiscalYearLabel(options.dateText, config.fiscalStartMonth);
  const sequence = issueSequence(db, docType, fiscalYear);
  const sequenceText = String(sequence).padStart(config.sequencePad, "0");
  return `${prefix}-${fiscalYear}-${sequenceText}`;
};

const createReceiptNo = (db, prefix, dateText) => generateDocumentNumber(db, {
  docType: prefix,
  prefix,
  dateText
});

const createInvoiceNo = (db, dateText) => generateDocumentNumber(db, {
  docType: "INV",
  prefix: "INV",
  dateText
});

module.exports = {
  createReceiptNo,
  createInvoiceNo,
  generateDocumentNumber,
  getFiscalYearLabel,
  getNumberingConfig
};
