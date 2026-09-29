SELECT order_id, SUM(CASE WHEN carrier = 'UPS' THEN cost ELSE 0 END) AS ups_cost FROM shipments WHERE order_id IS NULL OR cost >= 76 GROUP BY 1;
