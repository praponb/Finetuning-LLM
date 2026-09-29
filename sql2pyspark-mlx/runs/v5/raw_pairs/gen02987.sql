SELECT id, shipped_date, cost, delivered_date FROM shipments WHERE shipped_date < (SELECT MIN(shipped_date) FROM shipments WHERE carrier IN ('Flash', 'Kerry', 'UPS'));
