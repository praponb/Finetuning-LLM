WITH prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) AS prev_event_time
    FROM web_events
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM prev_events
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        session_id,
        event_type,
        event_time,
        duration_sec,
        SUM(session_start_flag) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
-- Aggregate with conditional metrics
SELECT
    event_type,
    CASE WHEN duration_sec >= 454 THEN 'HIGH' ELSE 'LOW' END AS duration_sec_tier,
    DATE_TRUNC('QUARTER', event_time) AS quarter_start,
    MIN(event_time) AS first_event_time,
    ROUND(SUM(duration_sec) / NULLIF(COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END), 0), 2) AS duration_sec_per_add_to_cart,
    PERCENTILE_APPROX(island_id, 0.5) AS island_id_p50,
    DENSE_RANK() OVER (PARTITION BY event_type ORDER BY ROUND(SUM(duration_sec) / NULLIF(COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END), 0), 2) DESC) AS duration_sec_per_add_to_cart_dense_rank
FROM sessions_web_events
GROUP BY event_type, CASE WHEN duration_sec >= 454 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('QUARTER', event_time)
