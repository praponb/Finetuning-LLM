SELECT status, delivered_date FROM shipments WHERE carrier IN ('UPS', 'Kerry', 'Flash') UNION SELECT status, delivered_date FROM shipments WHERE carrier = 'Flash';
