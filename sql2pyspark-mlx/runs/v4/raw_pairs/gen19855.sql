WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        INITCAP(TRIM(event_type)) AS event_type_title,
        CAST(event_time AS DATE) AS event_time_day,
        COALESCE(page, 'NONE') AS page_clean
    FROM web_events
    WHERE page IS NOT NULL
)
-- Running and moving aggregates
SELECT
    *,
    ROW_NUMBER() OVER (ORDER BY event_time_day DESC, id) AS seq_no,
    FIRST_VALUE(page) IGNORE NULLS OVER (PARTITION BY session_id, event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_page,
    ROW_NUMBER() OVER (PARTITION BY session_id, page ORDER BY event_time_day DESC, id) AS event_seq
FROM base
