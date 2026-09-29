SELECT id, order_id, delivered_date, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY delivered_date, id) AS rn FROM shipments WHERE carrier <> 'UPS' ORDER BY delivered_date, 1 DESC;
