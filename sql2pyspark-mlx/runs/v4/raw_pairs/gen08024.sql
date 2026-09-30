WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        event_type,
        page,
        event_time,
        duration_sec,
        WEEKOFYEAR(event_time) AS event_time_weekofyear
    FROM web_events
    WHERE event_type IN ('search', 'add_to_cart', 'purchase')
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(duration_sec, 1, 0) OVER (PARTITION BY page ORDER BY event_time, id) AS next_duration_sec,
        LAST_VALUE(duration_sec, TRUE) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_duration_sec,
        SUM(event_time_weekofyear) OVER (PARTITION BY event_type ORDER BY event_time_weekofyear, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_event_time_weekofyear
    FROM valid
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) AS prev_event_time
    FROM scored
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM islands
),
islands_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY page ORDER BY event_time_weekofyear DESC NULLS LAST) AS row_num
    FROM islands_web_events
    QUALIFY row_num = 1
)
-- Grouped metrics
SELECT
    DATE_TRUNC('WEEK', event_time) AS week_start,
    session_id,
    COUNT(CASE WHEN page = '/product' THEN 1 END) AS product_count,
    COUNT(CASE WHEN event_type = 'click' THEN 1 END) AS click_count,
    PERCENTILE_APPROX(duration_sec, 0.95) AS duration_sec_p95
FROM latest
GROUP BY GROUPING SETS ((DATE_TRUNC('WEEK', event_time), session_id), (DATE_TRUNC('WEEK', event_time)), ())
HAVING product_count < 1
