WITH valid AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        SUBSTRING(event_type, 1, 2) AS event_type_prefix,
        CEIL(duration_sec) AS duration_sec_whole
    FROM web_events
    WHERE page IS NOT NULL
),
ranked AS (
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY event_type, session_id ORDER BY duration_sec_whole DESC NULLS FIRST, id) = 1
),
with_gaps AS (
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) > INTERVAL 60 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM ranked
),
sessionized AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
)
SELECT
    id,
    session_id,
    event_type,
    page,
    event_time,
    event_type_prefix,
    duration_sec_whole,
    island_id,
    CASE WHEN event_type RLIKE '^(purchase|click)$' THEN 'CORE' WHEN event_type RLIKE '^(page_view|add_to_cart)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS event_type_zone
FROM sessionized
