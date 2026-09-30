WITH with_gaps AS (
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at ASC, id ASC) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at ASC, id ASC) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM sessions
),
sessions_sessions AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY customer_id ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
)
SELECT
    id,
    customer_id,
    device,
    browser,
    started_at,
    country,
    island_id,
    CASE WHEN device RLIKE '^(mobile|desktop)$' THEN 'EXTENDED' WHEN device RLIKE '^(tablet)$' THEN 'PRIORITY' ELSE 'CORE' END AS device_zone
FROM sessions_sessions
ORDER BY browser ASC NULLS LAST;
