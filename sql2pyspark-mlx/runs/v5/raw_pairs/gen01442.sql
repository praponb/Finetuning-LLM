WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        UPPER(event_type) AS event_type_upper
    FROM web_events
    WHERE event_time > DATE '2023-07-19'
      AND page IS DISTINCT FROM '/search'
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM cleaned
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM flagged
)
-- Multi-level aggregation
SELECT
    session_id,
    DATE_TRUNC('MONTH', event_time) AS activity_month,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS duration_min,
    ROUND(SUM(duration_sec) / NULLIF(COUNT(CASE WHEN page = '/cart' THEN 1 END), 0), 2) AS duration_sec_per_cart,
    COUNT(*) FILTER (WHERE page = '/cart') AS cart_rows,
    MAX(duration_sec) FILTER (WHERE page = '/search') AS search_max_duration_sec,
    MAX(event_time) AS last_event_time
FROM sessionized
GROUP BY session_id, DATE_TRUNC('MONTH', event_time)
HAVING cart_rows >= 1
ORDER BY cart_rows DESC NULLS FIRST;
