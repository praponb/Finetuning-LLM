WITH recent_web_events AS (
    SELECT *
    FROM web_events
    WHERE duration_sec <> 334 AND duration_sec > 385
),
Summary AS (
    SELECT event_type, ROUND((100 * SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_add_to_cart
    FROM recent_web_events
    GROUP BY event_type
)
SELECT *
FROM Summary;
