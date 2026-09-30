SELECT revenue, 'store_sale' AS source
FROM store_sales
UNION
SELECT amount, 'payment' AS source
FROM payments
ORDER BY source
