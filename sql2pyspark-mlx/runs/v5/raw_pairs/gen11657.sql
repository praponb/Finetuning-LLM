WITH customer_stats AS (
    SELECT customer_id, COUNT(CASE WHEN browser = 'Firefox' THEN 1 END) AS firefox_count
    FROM sessions
    GROUP BY customer_id
)
SELECT c.id, c.city, cs.firefox_count
FROM customers c
INNER JOIN customer_stats cs ON c.id = cs.customer_id
