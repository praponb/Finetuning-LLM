SELECT * FROM shipments QUALIFY RANK() OVER (PARTITION BY order_id ORDER BY shipped_date) = 1;
