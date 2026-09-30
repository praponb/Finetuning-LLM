WITH sessions_by_customer AS (
    SELECT customer_id, MAX(started_at) AS latest_started_at
    FROM sessions
    WHERE device = 'desktop'
    GROUP BY customer_id
)
SELECT c.id, c.name, sbc.latest_started_at
FROM customers c
INNER JOIN sessions_by_customer sbc ON c.id = sbc.customer_id
ORDER BY c.id DESC
