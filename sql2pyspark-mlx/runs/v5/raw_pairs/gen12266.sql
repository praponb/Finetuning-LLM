WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        UPPER(page) AS page_upper,
        INITCAP(TRIM(page)) AS page_title
    FROM web_events
    WHERE event_time > TIMESTAMP '2023-03-27 22:00:00'
      AND page IS DISTINCT FROM '/search'
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM filtered
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
)
-- Aggregate with conditional metrics
SELECT
    session_id,
    page,
    ROUND(AVG(duration_sec), 2) AS avg_duration_sec,
    BIT_OR(duration_sec) AS duration_sec_bit_or,
    GROUPING(session_id) AS gid
FROM islands
GROUP BY ROLLUP (session_id, page)
