SELECT status, SUM(CASE WHEN carrier = 'UPS' THEN cost ELSE 0 END) AS ups_cost FROM shipments WHERE carrier IN ('UPS', 'DHL', 'Kerry') GROUP BY status HAVING ups_cost >= 36;
