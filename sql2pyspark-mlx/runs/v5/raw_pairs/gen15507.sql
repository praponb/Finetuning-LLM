WITH latest AS (SELECT * FROM shipments QUALIFY RANK() OVER (PARTITION BY status ORDER BY cost DESC) = 1) SELECT id, carrier, delivered_date, cost, status FROM latest;
