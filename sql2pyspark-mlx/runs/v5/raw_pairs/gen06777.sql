SELECT cost, 'shipment' AS origin FROM shipments UNION ALL SELECT amount, 'payment' AS origin FROM payments ORDER BY origin
