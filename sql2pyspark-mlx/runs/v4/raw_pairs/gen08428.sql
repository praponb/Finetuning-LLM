WITH with_last AS (
    SELECT
        s.*,
        x.event_time,
        x.page
    FROM sessions s
    LEFT JOIN LATERAL (
            SELECT c.event_time, c.page
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    s.id,
    s.customer_id,
    s.started_at,
    s.country,
    s.event_time,
    s.page,
    (SELECT MAX(o2.amount) FROM orders o2 WHERE o2.customer_id = s.customer_id) AS max_order_amount
FROM with_last s
ORDER BY max_order_amount DESC;
