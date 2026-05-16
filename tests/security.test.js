const test = require("node:test");
const assert = require("node:assert/strict");

const {
  applySecurityHeaders,
  createMemoryRateLimiter,
  isTrustedOriginRequest
} = require("../src/middleware/security");

const createResponse = () => ({
  headers: {},
  statusCode: 200,
  body: null,
  payload: null,
  setHeader(name, value) {
    this.headers[name] = value;
  },
  status(code) {
    this.statusCode = code;
    return this;
  },
  send(body) {
    this.body = body;
    return this;
  },
  json(payload) {
    this.payload = payload;
    return this;
  }
});

test("isTrustedOriginRequest accepts matching origin or referer and rejects others", () => {
  assert.equal(isTrustedOriginRequest({
    origin: "https://app.example.com",
    referer: "",
    host: "app.example.com"
  }), true);
  assert.equal(isTrustedOriginRequest({
    origin: "",
    referer: "https://app.example.com/admin",
    host: "app.example.com"
  }), true);
  assert.equal(isTrustedOriginRequest({
    origin: "https://evil.example.com",
    referer: "",
    host: "app.example.com"
  }), false);
});

test("applySecurityHeaders sets the expected baseline headers", () => {
  const res = createResponse();
  let nextCalled = false;
  applySecurityHeaders({}, res, () => {
    nextCalled = true;
  });
  assert.equal(nextCalled, true);
  assert.equal(res.headers["X-Frame-Options"], "DENY");
  assert.equal(res.headers["X-Content-Type-Options"], "nosniff");
  assert.match(String(res.headers["Content-Security-Policy"] || ""), /form-action 'self'/);
});

test("createMemoryRateLimiter blocks after the threshold and resets after the window", () => {
  let now = 0;
  const limiter = createMemoryRateLimiter({
    windowMs: 1000,
    max: 2,
    now: () => now
  });
  const request = {
    ip: "127.0.0.1",
    xhr: false,
    headers: { accept: "text/html" }
  };

  let nextCalls = 0;
  limiter(request, createResponse(), () => { nextCalls += 1; });
  limiter(request, createResponse(), () => { nextCalls += 1; });
  assert.equal(nextCalls, 2);

  const limitedResponse = createResponse();
  limiter(request, limitedResponse, () => { nextCalls += 1; });
  assert.equal(limitedResponse.statusCode, 429);
  assert.equal(limitedResponse.headers["Retry-After"], "1");
  assert.equal(limitedResponse.body, "Too many requests.");
  assert.equal(nextCalls, 2);

  now = 1001;
  limiter(request, createResponse(), () => { nextCalls += 1; });
  assert.equal(nextCalls, 3);
});
