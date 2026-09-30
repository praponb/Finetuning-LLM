SELECT cost
FROM shipments
WHERE delivered_date BETWEEN '2023-06-14' AND '2025-08-01'
EXCEPT
SELECT shipping_fee AS cost
FROM orders;
