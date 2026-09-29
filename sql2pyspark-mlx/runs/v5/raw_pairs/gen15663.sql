WITH ranked_shipments AS (SELECT id, status, cost, delivered_date, ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost DESC, id) AS rn FROM shipments) SELECT * FROM ranked_shipments WHERE rn <= 2
