SELECT x.method, y.carrier FROM (SELECT DISTINCT method FROM payments) x CROSS JOIN (SELECT DISTINCT carrier FROM shipments) y ORDER BY method, carrier
