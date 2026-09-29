SELECT region, COUNT(*) AS row_count
FROM stores
GROUP BY region
HAVING row_count < 5
ORDER BY row_count DESC, region
LIMIT 20
