SELECT id, order_id, shipped_date, RANK() OVER (PARTITION BY order_id ORDER BY shipped_date) AS rnk FROM shipments;
