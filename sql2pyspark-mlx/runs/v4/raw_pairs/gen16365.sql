SELECT opened_date
FROM stores
WHERE city NOT IN ('Berlin', 'Chiang Mai', 'New York')
UNION ALL
SELECT delivered_date
FROM shipments;
