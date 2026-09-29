SELECT price
FROM products
WHERE supplier_id = 24
UNION
SELECT amount
FROM payments
WHERE TO_DATE(paid_at) >= '2024-06-01'
ORDER BY price
