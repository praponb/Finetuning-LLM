WITH customer_summary AS (
    SELECT customer_id, ROUND((100 * SUM(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_active
    FROM subscriptions
    WHERE YEAR(start_date) = 2024
    GROUP BY customer_id
)
SELECT c.id, c.segment, cs.pct_active
FROM customers c
LEFT OUTER JOIN customer_summary cs ON c.id = cs.customer_id
WHERE cs.pct_active >= 40
ORDER BY c.id
