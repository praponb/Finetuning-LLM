WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        page,
        event_time,
        duration_sec,
        TO_DATE(event_time) AS event_time_date
    FROM web_events
    WHERE event_type <> 'click'
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) AS prev_event_time
    FROM valid
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM prev_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        page,
        event_time,
        duration_sec,
        event_time_date,
        SUM(session_start_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
-- Multi-level aggregation
SELECT
    CASE WHEN island_id >= 6 THEN 'PREMIUM' ELSE 'STANDARD' END AS island_id_tier,
    ROUND(SUM(island_id) / NULLIF(COUNT(CASE WHEN page = '/search' THEN 1 END), 0), 2) AS island_id_per_search,
    PERCENTILE_APPROX(duration_sec, ARRAY(0.25, 0.5, 0.75)) AS duration_sec_quartiles,
    MAX(event_time) AS last_seen
FROM sessionized
GROUP BY CASE WHEN island_id >= 6 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY island_id_tier DESC NULLS FIRST;
