WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        CAST(event_time AS DATE) AS event_time_day
    FROM web_events
    WHERE page NOT IN ('/product', '/cart')
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM cleaned
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN burst_id >= 8 THEN 'LARGE' ELSE 'SMALL' END AS burst_id_tier,
    COUNT(*) AS row_count,
    MAX(event_time) AS last_seen,
    SUM(CASE WHEN page = '/checkout' THEN burst_id ELSE 0 END) AS checkout_burst_id
FROM sessions_web_events
GROUP BY CASE WHEN burst_id >= 8 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY burst_id_tier DESC
