WITH filtered AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        CONCAT_WS('-', page, event_type) AS page_event_type_key,
        TRIM(page) AS clean_page
    FROM web_events
    WHERE page IS NOT NULL
),
ranked AS (
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time NULLS LAST, id) = 1
),
ranked_web_events AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type, session_id ORDER BY event_time DESC, id) AS rank_in_group
        FROM ranked
    ) ranked
    WHERE rank_in_group = 1
),
prev_events AS (
    SELECT *, LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) AS prev_event_time
    FROM ranked_web_events
),
with_gaps AS (
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
),
sessionized AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
enriched AS (
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY session_id ORDER BY is_new_burst DESC, id) AS is_new_burst_bucket,
        LEAD(is_new_burst, 1, 0) OVER (ORDER BY event_time, id) AS next_is_new_burst
    FROM sessionized
)
SELECT
    id,
    session_id,
    event_type,
    page,
    event_time,
    is_new_burst,
    burst_id
FROM enriched;
