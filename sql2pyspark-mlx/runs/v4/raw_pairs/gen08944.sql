SELECT price, 'product' AS source
FROM products
UNION
SELECT cost AS price, 'shipment' AS source
FROM shipments
