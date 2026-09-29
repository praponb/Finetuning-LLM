SELECT c.segment, COUNT(DISTINCT s.browser) AS unique_browsers
FROM sessions s
RIGHT JOIN customers c ON s.customer_id = c.id
GROUP BY c.segment
ORDER BY segment, unique_browsers DESC
