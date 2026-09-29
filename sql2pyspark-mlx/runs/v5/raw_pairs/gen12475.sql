SELECT *
FROM shipments
WHERE carrier = 'Kerry' OR cost > 58
ORDER BY 2 DESC, id
LIMIT 3
