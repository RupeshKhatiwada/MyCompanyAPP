const { db } = require("../db");

function attachUser(req, res, next) {
  if (!req.session.userId) {
    res.locals.currentUser = null;
    return next();
  }

  const user = db.prepare(
    "SELECT id, username, full_name, phone, role, is_active FROM users WHERE id = ?"
  ).get(req.session.userId);
  if (!user) {
    req.session.destroy(() => {});
    res.locals.currentUser = null;
    return next();
  }
  if (Number(user.is_active) !== 1) {
    req.session.destroy(() => {});
    res.locals.currentUser = null;
    return next();
  }

  res.locals.currentUser = user;
  next();
}

function requireAuth(req, res, next) {
  if (!req.session.userId || !res.locals.currentUser) {
    return res.redirect("/login");
  }
  next();
}

function requireRole(roles = []) {
  return (req, res, next) => {
    const user = res.locals.currentUser;
    if (!user || (roles.length > 0 && !roles.includes(user.role))) {
      const title = res.locals.t ? res.locals.t("notAllowedTitle") : "Not allowed";
      return res.status(403).render("unauthorized", { title });
    }
    next();
  };
}

module.exports = {
  attachUser,
  requireAuth,
  requireRole
};
