SELECT cost
FROM shipments
UNION
SELECT revenue AS cost
FROM store_sales
WHERE revenue >= 2400
ORDER BY cost;
