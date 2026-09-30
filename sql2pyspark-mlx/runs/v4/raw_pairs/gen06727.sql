SELECT c.is_active, SUM(CASE WHEN s.browser = 'Edge' THEN 1 ELSE 0 END) AS edge_count, COUNT(DISTINCT s.country) AS unique_countrys
FROM sessions s
LEFT JOIN customers c ON s.customer_id = c.id
WHERE s.country IN ('UK', 'DE')
GROUP BY c.is_active
ORDER BY 3 DESC
