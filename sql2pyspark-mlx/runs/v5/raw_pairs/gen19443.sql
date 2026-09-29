SELECT region, COUNT(*) AS cnt
FROM stores
GROUP BY region
