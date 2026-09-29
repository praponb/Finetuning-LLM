SELECT id, cost, ROW_NUMBER() OVER (ORDER BY cost DESC, id) AS row_num FROM shipments WHERE order_id IN (10, 19, 36, 38) ORDER BY row_num DESC;
