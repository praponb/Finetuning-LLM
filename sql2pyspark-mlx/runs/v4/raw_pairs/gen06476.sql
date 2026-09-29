SELECT status, 'order' AS source FROM orders WHERE store_id IN (31, 35) UNION SELECT status, 'shipment' AS source FROM shipments WHERE MONTH(delivered_date) IN (8, 10, 11) ORDER BY source;
