SELECT id, order_id, delivered_date FROM shipments WHERE carrier IN ('Kerry', 'DHL') UNION ALL SELECT id, order_id, delivered_date FROM shipments WHERE MONTH(delivered_date) IN (1, 2, 6);
