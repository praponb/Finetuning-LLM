WITH enriched AS (
    -- Attach sessions attributes
    SELECT
        e.*,
        p.device,
        p.country
    FROM web_events e
    LEFT JOIN sessions p
        ON e.session_id = p.id
    WHERE p.device IN ('tablet', 'desktop')
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) AS prev_event_time
    FROM enriched
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM with_prev
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        event_type,
        page,
        event_time,
        duration_sec,
        device,
        country,
        SUM(is_new_burst) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT(event_type, ', ', page) AS event_type_page,
        COALESCE(device, 'N/A') AS device_filled
    FROM islands
    WHERE event_time > TO_DATE('2023-03-17')
),
gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM valid
),
islands_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps_2
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', event_time) AS activity_month,
    ROUND(AVG(session_id), 2) AS avg_session_id,
    ROUND(SUM(is_new_session) / NULLIF(COUNT(CASE WHEN event_type = 'page_view' THEN 1 END), 0), 2) AS is_new_session_per_page_view,
    SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list,
    COUNT(CASE WHEN page = '/cart' THEN 1 END) AS cart_count,
    APPROX_PERCENTILE(is_new_session, ARRAY(0.25, 0.5, 0.75)) AS is_new_session_quartiles,
    SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list_2
FROM islands_web_events
GROUP BY DATE_TRUNC('MONTH', event_time)
ORDER BY activity_month DESC;
