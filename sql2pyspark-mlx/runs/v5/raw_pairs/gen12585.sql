SELECT delivered_date
FROM shipments
WHERE carrier IN ('Flash', 'UPS')
INTERSECT
SELECT sale_date
FROM store_sales
ORDER BY delivered_date DESC;
