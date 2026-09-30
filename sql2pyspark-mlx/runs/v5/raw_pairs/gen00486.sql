SELECT revenue
FROM store_sales
EXCEPT
SELECT cost
FROM shipments
WHERE cost IS NOT NULL
ORDER BY revenue;
