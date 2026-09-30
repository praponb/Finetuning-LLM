SELECT c.city, MAX(s.started_at) AS last_started_at
FROM sessions s
FULL OUTER JOIN customers c ON s.customer_id = c.id
GROUP BY c.city
ORDER BY city, last_started_at;
