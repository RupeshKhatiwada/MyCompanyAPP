const { db } = require("../db");
const { getRolePermissionMap, canRoleAccessModule } = require("../utils/modulePermissions");

const canRequestModule = (req, user, moduleKey, action = "view") => {
  if (!user) return false;
  if (user.role === "SUPER_ADMIN") return true;
  const roleMap = req.rolePermissionMap || null;
  return canRoleAccessModule(db, user.role, moduleKey, action, roleMap);
};

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
  const rolePermissionMap = getRolePermissionMap(db, user.role);
  req.rolePermissionMap = rolePermissionMap;
  res.locals.rolePermissionMap = rolePermissionMap;
  req.canModuleAccess = (moduleKey, action = "view") => canRequestModule(req, user, moduleKey, action);
  res.locals.canModuleAccess = req.canModuleAccess;
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

function requireModule(moduleKey, action = "view") {
  return (req, res, next) => {
    const user = res.locals.currentUser;
    if (!user) return res.redirect("/login");
    if (user.role === "SUPER_ADMIN") return next();
    if (!canRequestModule(req, user, moduleKey, action)) {
      const title = res.locals.t ? res.locals.t("notAllowedTitle") : "Not allowed";
      return res.status(403).render("unauthorized", { title });
    }
    next();
  };
}

module.exports = {
  attachUser,
  requireAuth,
  requireRole,
  requireModule
};
