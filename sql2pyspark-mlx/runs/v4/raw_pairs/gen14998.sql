SELECT * FROM (SELECT id, status, shipped_date, order_id, RANK() OVER (PARTITION BY status ORDER BY shipped_date DESC) AS row_num FROM shipments) AS ranked WHERE row_num <= 2 ORDER BY row_num DESC
