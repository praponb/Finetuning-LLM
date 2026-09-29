WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM sessions
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        customer_id,
        browser,
        started_at,
        country,
        SUM(is_new_burst) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
)
-- Grouped metrics
SELECT
    DATE_TRUNC('QUARTER', started_at) AS quarter_start,
    COUNT(CASE WHEN browser = 'Safari' THEN 1 END) AS safari_count,
    COUNT_IF(browser = 'Safari') AS safari_events,
    COUNT(*) AS event_count
FROM sessionized
GROUP BY DATE_TRUNC('QUARTER', started_at)
ORDER BY safari_events;
