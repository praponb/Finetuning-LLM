WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT_WS('/', event_type, page) AS event_type_page_key,
        COALESCE(page, 'UNKNOWN') AS page_clean
    FROM web_events
    WHERE page IS DISTINCT FROM '/search'
      AND session_id IN (3, 10, 15, 22)
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM valid
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        page,
        event_time,
        event_type_page_key,
        SUM(session_start_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM with_gaps
)
-- Multi-level aggregation
SELECT
    CASE WHEN session_id_2 >= 9 THEN 'HIGH' ELSE 'LOW' END AS session_id_2_tier,
    DATE_TRUNC('QUARTER', event_time) AS quarter_start,
    MAX(event_time) AS last_event_time,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS duration_min,
    MAX(event_time) AS ended_at,
    MIN(event_time) AS first_seen,
    APPROX_PERCENTILE(session_id_2, ARRAY(0.25, 0.5, 0.75)) AS session_id_2_quartiles,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN session_id_2 >= 9 THEN 'HIGH' ELSE 'LOW' END ORDER BY (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 DESC) AS duration_min_percent_rank
FROM islands
GROUP BY CASE WHEN session_id_2 >= 9 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('QUARTER', event_time)
HAVING duration_min >= 0;
