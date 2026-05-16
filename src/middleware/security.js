const DEFAULT_WINDOW_MS = 15 * 60 * 1000;

const getHeaderHost = (value) => String(value || "")
  .trim()
  .toLowerCase();

const parseUrlHost = (value) => {
  const safe = String(value || "").trim();
  if (!safe) return "";
  try {
    return new URL(safe).host.toLowerCase();
  } catch (err) {
    return "";
  }
};

const isUnsafeMethod = (method) => ["POST", "PUT", "PATCH", "DELETE"].includes(String(method || "").toUpperCase());

const isTrustedOriginRequest = ({ origin, referer, host, fetchSite }) => {
  const safeFetchSite = String(fetchSite || "").trim().toLowerCase();
  if (["same-origin", "same-site", "none"].includes(safeFetchSite)) {
    return true;
  }
  const expectedHost = getHeaderHost(host);
  if (!expectedHost) return false;
  const originHost = parseUrlHost(origin);
  if (originHost) return originHost === expectedHost;
  const refererHost = parseUrlHost(referer);
  if (refererHost) return refererHost === expectedHost;
  return false;
};

const createOriginProtection = (options = {}) => {
  const exemptPaths = Array.isArray(options.exemptPaths) ? options.exemptPaths : [];
  return (req, res, next) => {
    if (!isUnsafeMethod(req.method)) return next();
    if (exemptPaths.some((prefix) => req.path === prefix || req.path.startsWith(`${prefix}/`))) {
      return next();
    }
    if (isTrustedOriginRequest({
      origin: req.get("origin"),
      referer: req.get("referer"),
      host: req.get("host"),
      fetchSite: req.get("sec-fetch-site")
    })) {
      return next();
    }
    const message = res.locals && typeof res.locals.t === "function"
      ? res.locals.t("requestOriginInvalid")
      : "Request blocked.";
    if (req.xhr || (req.headers.accept || "").includes("application/json")) {
      return res.status(403).json({ ok: false, error: "invalid_origin", message });
    }
    return res.status(403).send(message);
  };
};

const createMemoryRateLimiter = (options = {}) => {
  const windowMs = Number(options.windowMs) > 0 ? Number(options.windowMs) : DEFAULT_WINDOW_MS;
  const max = Number(options.max) > 0 ? Number(options.max) : 5;
  const now = typeof options.now === "function" ? options.now : () => Date.now();
  const keyGenerator = typeof options.keyGenerator === "function"
    ? options.keyGenerator
    : (req) => String(req.ip || "unknown");
  const stores = new Map();

  return (req, res, next) => {
    const key = String(keyGenerator(req) || "unknown");
    const currentTime = now();
    const existing = stores.get(key);
    const active = existing && existing.resetAt > currentTime
      ? existing
      : { count: 0, resetAt: currentTime + windowMs };
    active.count += 1;
    stores.set(key, active);
    if (active.count <= max) return next();

    const retryAfterSeconds = Math.max(1, Math.ceil((active.resetAt - currentTime) / 1000));
    res.setHeader("Retry-After", String(retryAfterSeconds));
    const messageKey = options.messageKey || "tooManyRequests";
    const message = res.locals && typeof res.locals.t === "function"
      ? res.locals.t(messageKey)
      : "Too many requests.";
    if (typeof options.onLimit === "function") {
      return options.onLimit(req, res, message);
    }
    if (req.xhr || (req.headers.accept || "").includes("application/json")) {
      return res.status(429).json({ ok: false, error: "rate_limited", message });
    }
    return res.status(429).send(message);
  };
};

const applySecurityHeaders = (req, res, next) => {
  res.setHeader("X-Frame-Options", "DENY");
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.setHeader("Referrer-Policy", "strict-origin-when-cross-origin");
  res.setHeader("Permissions-Policy", "camera=(), microphone=(), geolocation=()");
  res.setHeader(
    "Content-Security-Policy",
    [
      "default-src 'self'",
      "base-uri 'self'",
      "form-action 'self'",
      "frame-ancestors 'none'",
      "object-src 'none'",
      "img-src 'self' data: blob:",
      "font-src 'self' data:",
      "script-src 'self' 'unsafe-inline'",
      "style-src 'self' 'unsafe-inline'",
      "connect-src 'self'"
    ].join("; ")
  );
  next();
};

module.exports = {
  applySecurityHeaders,
  createMemoryRateLimiter,
  createOriginProtection,
  isTrustedOriginRequest
};
