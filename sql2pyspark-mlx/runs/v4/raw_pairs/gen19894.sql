WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN duration_sec >= 501 THEN 'High' WHEN duration_sec >= 205 THEN 'Medium' ELSE 'Low' END AS duration_sec_tier,
        UPPER(event_type) AS event_type_upper
    FROM web_events
    WHERE page = '/product'
),
with_dims AS (
    -- Attach sessions attributes
    SELECT a.*, s.device
    FROM base a
    INNER JOIN sessions s
        ON a.session_id = s.id
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT_WS('/', event_type, device) AS event_type_device_key
    FROM with_dims
    WHERE event_time > DATE '2023-11-15'
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM cleaned
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        a.*,
        p.browser,
        p.country
    FROM sessions_web_events a
    LEFT JOIN sessions p
        ON a.session_id = p.id
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN session_no >= 8 THEN 'LARGE' ELSE 'SMALL' END AS session_no_tier,
        CASE WHEN duration_sec >= 513 THEN 'PREMIUM' ELSE 'STANDARD' END AS duration_sec_tier,
        page,
        (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS duration_min,
        MAX(event_time) AS last_seen,
        (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS duration_min_2
    FROM enriched
    GROUP BY GROUPING SETS ((CASE WHEN session_no >= 8 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN duration_sec >= 513 THEN 'PREMIUM' ELSE 'STANDARD' END, page), (CASE WHEN session_no >= 8 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN duration_sec >= 513 THEN 'PREMIUM' ELSE 'STANDARD' END), (CASE WHEN session_no >= 8 THEN 'LARGE' ELSE 'SMALL' END), ())
)
SELECT
    session_no_tier,
    duration_sec_tier,
    duration_min_2
FROM grouped
