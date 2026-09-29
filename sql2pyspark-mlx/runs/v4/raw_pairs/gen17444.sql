WITH top_shipments AS (SELECT id, status, cost, RANK() OVER (PARTITION BY status ORDER BY cost) AS row_num FROM shipments) SELECT id, status, cost FROM top_shipments WHERE row_num = 1 ORDER BY id
