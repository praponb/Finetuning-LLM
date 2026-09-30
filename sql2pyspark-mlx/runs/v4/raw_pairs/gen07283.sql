SELECT status, 'order' source FROM orders UNION ALL SELECT status, 'shipment' source FROM shipments ORDER BY source;
