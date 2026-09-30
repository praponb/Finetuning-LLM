WITH eligible AS (
    SELECT s.*
    FROM sessions s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time >= s.started_at
        )
)
SELECT
    id,
    customer_id,
    device,
    browser
FROM eligible
ORDER BY customer_id NULLS FIRST
