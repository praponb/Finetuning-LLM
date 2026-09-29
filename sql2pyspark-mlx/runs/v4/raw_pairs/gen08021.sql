WITH numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipped_date, id) AS row_num FROM shipments WHERE order_id = 29) SELECT * FROM numbered WHERE row_num <= 1 ORDER BY status
