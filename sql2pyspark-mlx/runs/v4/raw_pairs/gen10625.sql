SELECT cost, 'shipment' AS source FROM shipments UNION ALL SELECT price AS cost, 'product' AS source FROM products;
