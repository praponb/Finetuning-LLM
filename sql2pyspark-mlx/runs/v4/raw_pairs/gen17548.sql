WITH customer_stats AS (
    SELECT customer_id, COUNT(CASE WHEN browser = 'Firefox' THEN 1 END) AS firefox_count
    FROM sessions
    WHERE customer_id IN (1, 13, 26)
    GROUP BY customer_id
)
SELECT c.id, c.signup_date, cs.firefox_count
FROM customers c
JOIN customer_stats cs ON c.id = cs.customer_id
WHERE cs.firefox_count >= 9
