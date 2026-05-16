const express = require("express");
const bcrypt = require("bcryptjs");
const { db } = require("../db");
const { createMemoryRateLimiter } = require("../middleware/security");

const normalizeAnswer = (value) => String(value || "").trim().toLowerCase();

const router = express.Router();

const renderSetup = (req, res, error = null) => res.render("setup", {
  title: req.t("setupTitle"),
  error
});

const renderLogin = (req, res, {
  error = null,
  created = req.query.created || null
} = {}) => res.render("login", {
  title: req.t("loginTitle"),
  error,
  created
});

const loadRecoveryContext = (selectedId) => {
  const superAdmins = db.prepare("SELECT id, username, full_name FROM users WHERE role = 'SUPER_ADMIN' ORDER BY created_at ASC").all();
  const fallbackId = selectedId || (superAdmins[0] ? String(superAdmins[0].id) : "");
  const selected = superAdmins.find((admin) => String(admin.id) === String(fallbackId)) || superAdmins[0] || null;
  const recovery = selected ? db.prepare("SELECT * FROM account_recovery WHERE user_id = ?").get(selected.id) : null;
  return {
    superAdmins,
    selected,
    recovery
  };
};

const renderRecover = (req, res, {
  userId = (req.body && req.body.user_id) || req.query.user || "",
  error = null,
  success = null,
  recoveryOverride
} = {}) => {
  const context = loadRecoveryContext(userId);
  res.render("recover", {
    title: req.t("recoveryTitle"),
    superAdmins: context.superAdmins,
    selected: context.selected,
    recovery: recoveryOverride !== undefined ? recoveryOverride : context.recovery,
    error,
    success
  });
};

const loginRateLimit = createMemoryRateLimiter({
  windowMs: 15 * 60 * 1000,
  max: 7,
  keyGenerator: (req) => `${req.ip || "unknown"}|${String(req.body.username || "").trim().toLowerCase()}`,
  messageKey: "tooManyLoginAttempts",
  onLimit: (req, res, message) => renderLogin(req, res, {
    error: message,
    created: null
  })
});

const recoveryRateLimit = createMemoryRateLimiter({
  windowMs: 30 * 60 * 1000,
  max: 5,
  keyGenerator: (req) => `${req.ip || "unknown"}|${String(req.body.user_id || req.query.user || "recover").trim()}`,
  messageKey: "tooManyRecoveryAttempts",
  onLimit: (req, res, message) => renderRecover(req, res, {
    userId: (req.body && req.body.user_id) || req.query.user,
    error: message,
    success: null
  })
});

router.get("/setup", (req, res) => {
  const count = db.prepare("SELECT COUNT(*) as count FROM users").get().count;
  if (count > 0) {
    return res.redirect("/login");
  }
  return renderSetup(req, res);
});

router.post("/setup", (req, res) => {
  const count = db.prepare("SELECT COUNT(*) as count FROM users").get().count;
  if (count > 0) {
    return res.redirect("/login");
  }

  const { full_name, username, phone, password } = req.body;
  if (!full_name || !username || !password) {
    return renderSetup(req, res, req.t("requiredFields"));
  }

  const hash = bcrypt.hashSync(password, 10);
  db.prepare(
    "INSERT INTO users (full_name, username, phone, password_hash, role) VALUES (?, ?, ?, ?, 'SUPER_ADMIN')"
  ).run(full_name.trim(), username.trim(), phone ? phone.trim() : null, hash);

  return res.redirect("/login?created=1");
});

router.get("/login", (req, res) => {
  return renderLogin(req, res);
});

router.post("/login", loginRateLimit, (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) {
    return renderLogin(req, res, { error: req.t("enterCredentials"), created: null });
  }

  const user = db.prepare("SELECT * FROM users WHERE username = ?").get(username.trim());
  if (user && Number(user.is_active) !== 1) {
    return renderLogin(req, res, { error: req.t("accountInactive"), created: null });
  }
  if (!user || !bcrypt.compareSync(password, user.password_hash)) {
    return renderLogin(req, res, { error: req.t("invalidCredentials"), created: null });
  }

  req.session.regenerate((regenErr) => {
    if (regenErr) {
      return renderLogin(req, res, { error: req.t("loginSessionError"), created: null });
    }

    req.session.userId = user.id;
    req.session.save((saveErr) => {
      if (saveErr) {
        return renderLogin(req, res, { error: req.t("loginSessionError"), created: null });
      }
      return res.redirect("/");
    });
  });
});

router.get("/recover", (req, res) => {
  return renderRecover(req, res);
});

router.post("/recover/key", recoveryRateLimit, (req, res) => {
  const { user_id, recovery_key, new_password } = req.body;
  const { selected, recovery } = loadRecoveryContext(user_id);

  if (!selected) {
    return renderRecover(req, res, {
      userId: user_id,
      error: req.t("recoveryUserRequired"),
      success: null,
      recoveryOverride: null
    });
  }
  if (!recovery || !recovery.key_hash) {
    return renderRecover(req, res, { userId: user_id, error: req.t("recoveryKeyNotSet"), success: null });
  }
  if (!recovery_key || !new_password) {
    return renderRecover(req, res, { userId: user_id, error: req.t("recoveryKeyRequired"), success: null });
  }
  const keyOk = bcrypt.compareSync(recovery_key.trim(), recovery.key_hash);
  if (!keyOk) {
    return renderRecover(req, res, { userId: user_id, error: req.t("recoveryKeyInvalid"), success: null });
  }
  const hash = bcrypt.hashSync(new_password, 10);
  db.prepare("UPDATE users SET password_hash = ? WHERE id = ?").run(hash, selected.id);
  db.prepare("UPDATE account_recovery SET key_hash = NULL, updated_at = datetime('now') WHERE user_id = ?").run(selected.id);

  return renderRecover(req, res, {
    userId: user_id,
    recoveryOverride: { ...recovery, key_hash: null },
    error: null,
    success: req.t("recoveryPasswordReset")
  });
});

router.post("/recover/questions", recoveryRateLimit, (req, res) => {
  const { user_id, a1, a2, a3, new_password } = req.body;
  const { selected, recovery } = loadRecoveryContext(user_id);

  if (!selected) {
    return renderRecover(req, res, {
      userId: user_id,
      error: req.t("recoveryUserRequired"),
      success: null,
      recoveryOverride: null
    });
  }
  if (!recovery || !recovery.a1_hash || !recovery.a2_hash || !recovery.a3_hash) {
    return renderRecover(req, res, { userId: user_id, error: req.t("recoveryQuestionsNotSet"), success: null });
  }
  if (!a1 || !a2 || !a3 || !new_password) {
    return renderRecover(req, res, { userId: user_id, error: req.t("recoveryAnswersRequired"), success: null });
  }
  const ok1 = bcrypt.compareSync(normalizeAnswer(a1), recovery.a1_hash);
  const ok2 = bcrypt.compareSync(normalizeAnswer(a2), recovery.a2_hash);
  const ok3 = bcrypt.compareSync(normalizeAnswer(a3), recovery.a3_hash);
  if (!ok1 || !ok2 || !ok3) {
    return renderRecover(req, res, { userId: user_id, error: req.t("recoveryAnswersInvalid"), success: null });
  }
  const hash = bcrypt.hashSync(new_password, 10);
  db.prepare("UPDATE users SET password_hash = ? WHERE id = ?").run(hash, selected.id);

  return renderRecover(req, res, {
    userId: user_id,
    error: null,
    success: req.t("recoveryPasswordReset")
  });
});

router.post("/logout", (req, res) => {
  req.session.destroy(() => {
    res.redirect("/login");
  });
});

module.exports = router;
