SELECT shipping_fee AS value, 'order' AS record_type
FROM orders
WHERE status IN ('RETURNED', 'COMPLETED')
UNION
SELECT cost AS value, 'shipment' AS record_type
FROM shipments
WHERE delivered_date BETWEEN '2024-08-20' AND '2025-09-17';
