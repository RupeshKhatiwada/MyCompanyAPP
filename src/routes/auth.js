const express = require("express");
const bcrypt = require("bcryptjs");
const { db } = require("../db");

const normalizeAnswer = (value) => String(value || "").trim().toLowerCase();

const router = express.Router();

router.get("/setup", (req, res) => {
  const count = db.prepare("SELECT COUNT(*) as count FROM users").get().count;
  if (count > 0) {
    return res.redirect("/login");
  }
  res.render("setup", { title: req.t("setupTitle"), error: null });
});

router.post("/setup", (req, res) => {
  const count = db.prepare("SELECT COUNT(*) as count FROM users").get().count;
  if (count > 0) {
    return res.redirect("/login");
  }

  const { full_name, username, phone, password } = req.body;
  if (!full_name || !username || !password) {
    return res.render("setup", { title: req.t("setupTitle"), error: req.t("requiredFields") });
  }

  const hash = bcrypt.hashSync(password, 10);
  db.prepare(
    "INSERT INTO users (full_name, username, phone, password_hash, role) VALUES (?, ?, ?, ?, 'SUPER_ADMIN')"
  ).run(full_name.trim(), username.trim(), phone ? phone.trim() : null, hash);

  res.redirect("/login?created=1");
});

router.get("/login", (req, res) => {
  res.render("login", { title: req.t("loginTitle"), error: null, created: req.query.created });
});

router.post("/login", (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) {
    return res.render("login", { title: req.t("loginTitle"), error: req.t("enterCredentials"), created: null });
  }

  const user = db.prepare("SELECT * FROM users WHERE username = ?").get(username.trim());
  if (user && Number(user.is_active) !== 1) {
    return res.render("login", { title: req.t("loginTitle"), error: req.t("accountInactive"), created: null });
  }
  if (!user || !bcrypt.compareSync(password, user.password_hash)) {
    return res.render("login", { title: req.t("loginTitle"), error: req.t("invalidCredentials"), created: null });
  }

  req.session.regenerate((regenErr) => {
    if (regenErr) {
      return res.render("login", {
        title: req.t("loginTitle"),
        error: req.t("loginSessionError"),
        created: null
      });
    }

    req.session.userId = user.id;
    req.session.save((saveErr) => {
      if (saveErr) {
        return res.render("login", {
          title: req.t("loginTitle"),
          error: req.t("loginSessionError"),
          created: null
        });
      }
      return res.redirect("/");
    });
  });
});

router.get("/recover", (req, res) => {
  const superAdmins = db.prepare("SELECT id, username, full_name FROM users WHERE role = 'SUPER_ADMIN' ORDER BY created_at ASC").all();
  const selectedId = req.query.user || (superAdmins[0] ? String(superAdmins[0].id) : "");
  const selected = superAdmins.find((admin) => String(admin.id) === String(selectedId)) || superAdmins[0] || null;
  const recovery = selected ? db.prepare("SELECT * FROM account_recovery WHERE user_id = ?").get(selected.id) : null;

  res.render("recover", {
    title: req.t("recoveryTitle"),
    superAdmins,
    selected,
    recovery,
    error: null,
    success: null
  });
});

router.post("/recover/key", (req, res) => {
  const { user_id, recovery_key, new_password } = req.body;
  const superAdmins = db.prepare("SELECT id, username, full_name FROM users WHERE role = 'SUPER_ADMIN' ORDER BY created_at ASC").all();
  const selected = superAdmins.find((admin) => String(admin.id) === String(user_id)) || null;
  const recovery = selected ? db.prepare("SELECT * FROM account_recovery WHERE user_id = ?").get(selected.id) : null;

  if (!selected) {
    return res.render("recover", {
      title: req.t("recoveryTitle"),
      superAdmins,
      selected: null,
      recovery: null,
      error: req.t("recoveryUserRequired"),
      success: null
    });
  }
  if (!recovery || !recovery.key_hash) {
    return res.render("recover", {
      title: req.t("recoveryTitle"),
      superAdmins,
      selected,
      recovery,
      error: req.t("recoveryKeyNotSet"),
      success: null
    });
  }
  if (!recovery_key || !new_password) {
    return res.render("recover", {
      title: req.t("recoveryTitle"),
      superAdmins,
      selected,
      recovery,
      error: req.t("recoveryKeyRequired"),
      success: null
    });
  }
  const keyOk = bcrypt.compareSync(recovery_key.trim(), recovery.key_hash);
  if (!keyOk) {
    return res.render("recover", {
      title: req.t("recoveryTitle"),
      superAdmins,
      selected,
      recovery,
      error: req.t("recoveryKeyInvalid"),
      success: null
    });
  }
  const hash = bcrypt.hashSync(new_password, 10);
  db.prepare("UPDATE users SET password_hash = ? WHERE id = ?").run(hash, selected.id);
  db.prepare("UPDATE account_recovery SET key_hash = NULL, updated_at = datetime('now') WHERE user_id = ?").run(selected.id);

  res.render("recover", {
    title: req.t("recoveryTitle"),
    superAdmins,
    selected,
    recovery: { ...recovery, key_hash: null },
    error: null,
    success: req.t("recoveryPasswordReset")
  });
});

router.post("/recover/questions", (req, res) => {
  const { user_id, a1, a2, a3, new_password } = req.body;
  const superAdmins = db.prepare("SELECT id, username, full_name FROM users WHERE role = 'SUPER_ADMIN' ORDER BY created_at ASC").all();
  const selected = superAdmins.find((admin) => String(admin.id) === String(user_id)) || null;
  const recovery = selected ? db.prepare("SELECT * FROM account_recovery WHERE user_id = ?").get(selected.id) : null;

  if (!selected) {
    return res.render("recover", {
      title: req.t("recoveryTitle"),
      superAdmins,
      selected: null,
      recovery: null,
      error: req.t("recoveryUserRequired"),
      success: null
    });
  }
  if (!recovery || !recovery.a1_hash || !recovery.a2_hash || !recovery.a3_hash) {
    return res.render("recover", {
      title: req.t("recoveryTitle"),
      superAdmins,
      selected,
      recovery,
      error: req.t("recoveryQuestionsNotSet"),
      success: null
    });
  }
  if (!a1 || !a2 || !a3 || !new_password) {
    return res.render("recover", {
      title: req.t("recoveryTitle"),
      superAdmins,
      selected,
      recovery,
      error: req.t("recoveryAnswersRequired"),
      success: null
    });
  }
  const ok1 = bcrypt.compareSync(normalizeAnswer(a1), recovery.a1_hash);
  const ok2 = bcrypt.compareSync(normalizeAnswer(a2), recovery.a2_hash);
  const ok3 = bcrypt.compareSync(normalizeAnswer(a3), recovery.a3_hash);
  if (!ok1 || !ok2 || !ok3) {
    return res.render("recover", {
      title: req.t("recoveryTitle"),
      superAdmins,
      selected,
      recovery,
      error: req.t("recoveryAnswersInvalid"),
      success: null
    });
  }
  const hash = bcrypt.hashSync(new_password, 10);
  db.prepare("UPDATE users SET password_hash = ? WHERE id = ?").run(hash, selected.id);

  res.render("recover", {
    title: req.t("recoveryTitle"),
    superAdmins,
    selected,
    recovery,
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
