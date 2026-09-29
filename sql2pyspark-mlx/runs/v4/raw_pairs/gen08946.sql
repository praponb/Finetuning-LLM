SELECT id, order_id, shipped_date, DENSE_RANK() OVER (PARTITION BY order_id ORDER BY shipped_date) AS dense_rnk FROM shipments;
