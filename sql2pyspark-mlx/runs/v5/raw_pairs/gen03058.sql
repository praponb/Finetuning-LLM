WITH customer_stats AS (
    SELECT customer_id, COUNT(*) AS num_sessions
    FROM sessions
    GROUP BY customer_id
)
SELECT c.country, c.age, cs.num_sessions
FROM customers c
LEFT JOIN customer_stats cs ON c.id = cs.customer_id
WHERE cs.num_sessions > 9
ORDER BY num_sessions DESC
