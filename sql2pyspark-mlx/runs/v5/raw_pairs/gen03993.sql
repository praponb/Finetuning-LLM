SELECT SUM(CASE WHEN region = 'West' THEN 1 ELSE 0 END) west_count
FROM stores
WHERE opened_date < '2024-03-08'
ORDER BY 1 DESC
