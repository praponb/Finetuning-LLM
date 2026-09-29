SELECT order_id FROM shipments WHERE cost <= 51 UNION SELECT order_id FROM shipments WHERE MONTH(delivered_date) IN (2, 6, 9) ORDER BY order_id
