SELECT c.segment, COUNT(*) AS session_count, SUM(CASE WHEN s.country = 'JP' THEN 1 ELSE 0 END) AS jp_count
FROM sessions s
JOIN customers c ON s.customer_id = c.id
GROUP BY c.segment;
