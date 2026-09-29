SELECT channel
FROM orders
WHERE status IN ('PENDING', 'RETURNED')
UNION ALL
SELECT channel
FROM orders
WHERE YEAR(order_date) >= 2023;
