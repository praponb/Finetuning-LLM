SELECT COUNT(CASE WHEN city = 'Tokyo' THEN 1 END) AS tokyo_count
FROM stores
WHERE city IN ('Berlin', 'New York') AND city = 'Singapore' AND store_name IS NOT NULL
