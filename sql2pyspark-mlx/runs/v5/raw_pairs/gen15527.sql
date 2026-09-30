SELECT x.method, y.status FROM (SELECT DISTINCT method FROM payments) x CROSS JOIN (SELECT DISTINCT status FROM shipments) y ORDER BY method, status;
