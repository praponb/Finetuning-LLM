WITH filtered AS (
    SELECT
        *,
        CASE WHEN page IN ('/home', '/cart') THEN 'Core' ELSE 'Other' END AS page_group,
        COALESCE(page, 'DIRECT') AS page_clean
    FROM web_events
    WHERE event_time > DATE '2023-07-15'
      AND duration_sec <= 277
),
with_prev AS (
    SELECT *, LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) AS prev_event_time
    FROM filtered
),
with_gaps AS (
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL '7' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM with_prev
),
sessions_web_events AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
with_dims AS (
    SELECT s.*, p.browser
    FROM sessions_web_events s
    INNER JOIN sessions p
        ON s.session_id = p.id
    WHERE p.customer_id = 29
),
enriched AS (
    SELECT
        a.*,
        d.device,
        d.country
    FROM with_dims a
    LEFT JOIN sessions d
        ON a.session_id = d.id
),
enriched_web_events AS (
    SELECT
        *,
        LAG(duration_sec) OVER (PARTITION BY country ORDER BY session_no, id) AS prev_duration_sec,
        LEAD(duration_sec) OVER (PARTITION BY country ORDER BY session_no, id) AS next_duration_sec,
        LEAD(session_no) OVER (PARTITION BY country ORDER BY session_no, id) AS next_session_no,
        LAG(session_no) OVER (PARTITION BY country ORDER BY session_no, id) AS prev_session_no,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY session_no, id) AS seq_no
    FROM enriched
)
SELECT
    id,
    session_id,
    event_type,
    page,
    event_time,
    duration_sec,
    page_clean,
    prev_event_time,
    new_group_flag,
    CASE WHEN event_type RLIKE '^(purchase|search)$' THEN 'GROUP_A' WHEN event_type RLIKE '^(page_view|click)$' THEN 'PRIORITY' ELSE 'GROUP_B' END AS event_type_zone
FROM enriched_web_events
ORDER BY new_group_flag DESC NULLS FIRST, id;
