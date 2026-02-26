const dayjs = require("dayjs");

const CALENDAR_COOKIE = "calendar_mode";
const CALENDAR_AD = "AD";
const CALENDAR_BS = "BS";

let NepaliDateConverter = null;
try {
  // Optional dependency. If unavailable, app safely falls back to AD-only behavior.
  NepaliDateConverter = require("nepali-date-converter");
} catch (err) {
  NepaliDateConverter = null;
}

const normalizeCalendarMode = (value) => {
  const safe = String(value || "").trim().toUpperCase();
  return safe === CALENDAR_BS ? CALENDAR_BS : CALENDAR_AD;
};

const parseIsoDate = (value) => {
  const safe = String(value || "").trim();
  const match = safe.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return null;
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  if (!year || month < 1 || month > 12 || day < 1 || day > 31) return null;
  return { year, month, day };
};

const formatIsoDate = (year, month, day) => (
  `${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`
);

const parseBsDate = (value) => {
  const safe = String(value || "").trim();
  const match = safe.match(/^(\d{4})-(\d{1,2})-(\d{1,2})$/);
  if (!match) return null;
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  if (!year || month < 1 || month > 12 || day < 1 || day > 32) return null;
  return { year, month, day };
};

const normalizeConverterExport = (mod) => {
  if (!mod) return null;
  if (typeof mod === "function") return mod;
  if (typeof mod.default === "function") return mod.default;
  if (typeof mod.NepaliDate === "function") return mod.NepaliDate;
  return null;
};

const ConverterClass = normalizeConverterExport(NepaliDateConverter);

const isConverterReady = () => Boolean(ConverterClass);

const getDateInNepal = (jsDate) => {
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Kathmandu",
    year: "numeric",
    month: "2-digit",
    day: "2-digit"
  });
  const parts = formatter.formatToParts(jsDate);
  const partMap = parts.reduce((acc, part) => {
    acc[part.type] = part.value;
    return acc;
  }, {});
  return {
    year: Number(partMap.year),
    month: Number(partMap.month),
    day: Number(partMap.day)
  };
};

const buildConverterFromAd = (adDateText) => {
  if (!ConverterClass) return null;
  const parsed = parseIsoDate(adDateText);
  if (!parsed) return null;
  // Nepal local noon to avoid timezone edge issues near midnight.
  const jsDate = new Date(Date.UTC(parsed.year, parsed.month - 1, parsed.day, 6, 15, 0));
  try {
    return new ConverterClass(jsDate);
  } catch (err) {
    return null;
  }
};

const buildConverterFromBs = (bsDateText) => {
  if (!ConverterClass) return null;
  const parsed = parseBsDate(bsDateText);
  if (!parsed) return null;

  const attempts = [
    () => new ConverterClass(parsed.year, parsed.month - 1, parsed.day),
    () => new ConverterClass(parsed.year, parsed.month, parsed.day),
    () => new ConverterClass({ year: parsed.year, month: parsed.month - 1, day: parsed.day }),
    () => new ConverterClass({ year: parsed.year, month: parsed.month, day: parsed.day }),
    () => new ConverterClass(`${parsed.year}-${String(parsed.month).padStart(2, "0")}-${String(parsed.day).padStart(2, "0")}`)
  ];

  for (const create of attempts) {
    try {
      const instance = create();
      if (instance) return instance;
    } catch (err) {
      // continue trying
    }
  }

  return null;
};

const extractBsFromInstance = (instance) => {
  if (!instance) return null;

  if (typeof instance.format === "function") {
    try {
      const formatted = String(instance.format("YYYY-MM-DD") || "").trim();
      if (/^\d{4}-\d{2}-\d{2}$/.test(formatted)) {
        return formatted;
      }
    } catch (err) {
      // ignore and fall back
    }
  }

  const year = typeof instance.getYear === "function" ? instance.getYear() : instance.year;
  let month = typeof instance.getMonth === "function" ? instance.getMonth() : instance.month;
  const day = typeof instance.getDate === "function"
    ? instance.getDate()
    : (typeof instance.date !== "undefined" ? instance.date : instance.day);

  if (![year, month, day].every((num) => typeof num === "number" && Number.isFinite(num))) return null;
  if (month >= 0 && month <= 11) month += 1;
  if (month < 1 || month > 12 || day < 1 || day > 32) return null;

  return formatIsoDate(year, month, day);
};

const extractAdFromInstance = (instance) => {
  if (!instance) return null;
  let jsDate = null;

  if (typeof instance.toJsDate === "function") {
    try {
      jsDate = instance.toJsDate();
    } catch (err) {
      jsDate = null;
    }
  }

  if (!jsDate && typeof instance.toDate === "function") {
    try {
      jsDate = instance.toDate();
    } catch (err) {
      jsDate = null;
    }
  }

  if (!(jsDate instanceof Date) || Number.isNaN(jsDate.getTime())) return null;
  const parts = getDateInNepal(jsDate);
  return formatIsoDate(parts.year, parts.month, parts.day);
};

const adToBs = (adDateText) => {
  const parsed = parseIsoDate(adDateText);
  if (!parsed) return null;
  if (!ConverterClass) return null;

  const instance = buildConverterFromAd(formatIsoDate(parsed.year, parsed.month, parsed.day));
  if (!instance) return null;
  return extractBsFromInstance(instance);
};

const bsToAd = (bsDateText) => {
  const parsed = parseBsDate(bsDateText);
  if (!parsed) return null;
  if (!ConverterClass) return null;

  const instance = buildConverterFromBs(formatIsoDate(parsed.year, parsed.month, parsed.day));
  if (!instance) return null;
  return extractAdFromInstance(instance);
};

const formatDateForMode = (adDateText, mode = CALENDAR_AD) => {
  const parsed = parseIsoDate(adDateText);
  if (!parsed) return String(adDateText || "");

  const ad = formatIsoDate(parsed.year, parsed.month, parsed.day);
  const safeMode = normalizeCalendarMode(mode);
  if (safeMode === CALENDAR_AD) return ad;

  const bs = adToBs(ad);
  return bs || ad;
};

const formatDateDual = (adDateText) => {
  const parsed = parseIsoDate(adDateText);
  if (!parsed) return String(adDateText || "");

  const ad = formatIsoDate(parsed.year, parsed.month, parsed.day);
  const bs = adToBs(ad);
  if (!bs) return ad;
  return `${ad} / ${bs}`;
};

const todayForMode = (mode = CALENDAR_AD) => {
  const adToday = dayjs().format("YYYY-MM-DD");
  return formatDateForMode(adToday, mode);
};

module.exports = {
  CALENDAR_COOKIE,
  CALENDAR_AD,
  CALENDAR_BS,
  normalizeCalendarMode,
  isConverterReady,
  parseIsoDate,
  parseBsDate,
  formatDateForMode,
  formatDateDual,
  adToBs,
  bsToAd,
  todayForMode
};
