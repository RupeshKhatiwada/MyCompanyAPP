const toNumber = (value) => {
  const num = Number(value || 0);
  return Number.isNaN(num) ? 0 : num;
};

const buildBaseMap = (vehicles) => vehicles.reduce((acc, vehicle) => {
  acc[vehicle.id] = {
    vehicle_id: Number(vehicle.id),
    total_added_jar_containers: 0,
    total_leakage_jars: 0,
    total_jar_sold: 0,
    total_lent_from_vehicle: 0,
    current_container_balance: 0
  };
  return acc;
}, {});

const applyMetricRows = (map, rows, key) => {
  rows.forEach((row) => {
    const vehicleId = Number(row.vehicle_id || 0);
    if (!vehicleId || !map[vehicleId]) return;
    map[vehicleId][key] = toNumber(row.qty || 0);
  });
};

const getVehicleContainerMetricsMap = (db, { includeInactive = true, companyOnly = false, excludeLendingId = null } = {}) => {
  const where = [];
  if (!includeInactive) where.push("COALESCE(is_active, 1) = 1");
  if (companyOnly) where.push("is_company = 1");
  const vehicleWhere = where.length ? `WHERE ${where.join(" AND ")}` : "";

  const vehicles = db.prepare(
    `SELECT id
     FROM vehicles
     ${vehicleWhere}`
  ).all();

  const map = buildBaseMap(vehicles);
  if (!vehicles.length) return map;

  applyMetricRows(
    map,
    db.prepare(
      `SELECT vehicle_id, COALESCE(SUM(quantity), 0) as qty
       FROM jar_sales
       WHERE vehicle_id IS NOT NULL
       GROUP BY vehicle_id`
    ).all(),
    "total_added_jar_containers"
  );

  const exportAddedRows = db.prepare(
    `SELECT vehicle_id, COALESCE(SUM(jar_container_given_count), 0) as qty
     FROM exports
     WHERE vehicle_id IS NOT NULL
     GROUP BY vehicle_id`
  ).all();
  exportAddedRows.forEach((row) => {
    const vehicleId = Number(row.vehicle_id || 0);
    if (!vehicleId || !map[vehicleId]) return;
    map[vehicleId].total_added_jar_containers = toNumber(
      map[vehicleId].total_added_jar_containers + toNumber(row.qty || 0)
    );
  });

  applyMetricRows(
    map,
    db.prepare(
      `SELECT vehicle_id, COALESCE(SUM(leakage_jar_count), 0) as qty
       FROM exports
       WHERE vehicle_id IS NOT NULL
       GROUP BY vehicle_id`
    ).all(),
    "total_leakage_jars"
  );

  applyMetricRows(
    map,
    db.prepare(
      `SELECT vehicle_id, COALESCE(SUM(sold_jar_count), 0) as qty
       FROM exports
       WHERE vehicle_id IS NOT NULL
       GROUP BY vehicle_id`
    ).all(),
    "total_jar_sold"
  );

  applyMetricRows(
    map,
    db.prepare(
      `SELECT jar_container_lendings.vehicle_id,
              COALESCE(SUM(
                CASE
                  WHEN jar_container_lendings.quantity - COALESCE(return_rows.returned_qty, 0) < 0 THEN 0
                  ELSE jar_container_lendings.quantity - COALESCE(return_rows.returned_qty, 0)
                END
              ), 0) as qty
       FROM jar_container_lendings
       LEFT JOIN (
         SELECT lending_id, COALESCE(SUM(quantity), 0) as returned_qty
         FROM jar_container_lending_returns
         GROUP BY lending_id
       ) return_rows ON return_rows.lending_id = jar_container_lendings.id
       WHERE jar_container_lendings.vehicle_id IS NOT NULL
         AND COALESCE(NULLIF(jar_container_lendings.source_type, ''), 'STORAGE') = 'VEHICLE'
         AND (? IS NULL OR jar_container_lendings.id != ?)
       GROUP BY jar_container_lendings.vehicle_id`
    ).all(excludeLendingId, excludeLendingId),
    "total_lent_from_vehicle"
  );

  Object.values(map).forEach((row) => {
    const available = toNumber(row.total_added_jar_containers)
      - toNumber(row.total_leakage_jars)
      - toNumber(row.total_jar_sold)
      - toNumber(row.total_lent_from_vehicle);
    row.current_container_balance = available > 0 ? available : 0;
  });

  return map;
};

module.exports = {
  getVehicleContainerMetricsMap
};
