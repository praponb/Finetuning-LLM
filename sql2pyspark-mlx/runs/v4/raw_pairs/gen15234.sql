SELECT amount AS value FROM orders WHERE shipping_fee BETWEEN 37 AND 39 UNION ALL SELECT cost AS value FROM shipments ORDER BY value
