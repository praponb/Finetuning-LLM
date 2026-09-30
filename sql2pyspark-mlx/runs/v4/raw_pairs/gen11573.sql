SELECT opened_date
FROM stores
WHERE region IN ('West', 'South')
INTERSECT
SELECT shipped_date
FROM shipments
WHERE order_id IN (5, 14, 18)
ORDER BY opened_date;
