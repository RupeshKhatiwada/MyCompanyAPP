const dayjs = require("dayjs");

const normalizeDateToken = (dateText) => {
  const parsed = dayjs(dateText);
  if (parsed.isValid()) return parsed.format("YYYYMMDD");
  return dayjs().format("YYYYMMDD");
};

const buildReceiptNo = (prefix, dateText, idValue) => {
  const safePrefix = String(prefix || "REC").trim().toUpperCase();
  const dateToken = normalizeDateToken(dateText);
  const numericId = Number(idValue || 0);
  const suffix = Number.isNaN(numericId) ? "000000" : String(Math.max(0, Math.floor(numericId))).padStart(6, "0");
  return `${safePrefix}-${dateToken}-${suffix}`;
};

module.exports = {
  buildReceiptNo
};
