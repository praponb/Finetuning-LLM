WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, INITCAP(TRIM(event_type)) event_type_title
    FROM web_events
    WHERE session_id IN (2, 17, 32)
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT_WS(',', page, event_type) page_event_type_key,
        LENGTH(page) page_length,
        COALESCE(page, 'UNKNOWN') page_clean
    FROM valid
    WHERE event_time >= DATE '2023-06-08'
      AND page IS NOT NULL
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM filtered
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        page,
        event_time,
        SUM(is_new_session) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(session_no) OVER w_running running_count_session_no,
        SUM(session_no) OVER w_running running_sum_session_no,
        MIN(session_no) OVER w_running running_min_session_no
    FROM islands
    WINDOW w_running AS (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    session_no,
    running_min_session_no
FROM scored
