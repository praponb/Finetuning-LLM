SELECT a.status, b.carrier FROM (SELECT DISTINCT status FROM payments) a CROSS JOIN (SELECT DISTINCT carrier FROM shipments) b ORDER BY status, carrier;
