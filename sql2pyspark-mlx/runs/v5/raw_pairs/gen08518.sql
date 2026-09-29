WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        session_id,
        event_type,
        duration_sec,
        LOWER(event_type) AS event_type_lower,
        COALESCE(page, 'DIRECT') AS page_clean
    FROM web_events
    WHERE page IS DISTINCT FROM '/cart'
)
-- Multi-level aggregation
SELECT
    event_type,
    ROUND(AVG(duration_sec), 2) AS avg_duration_sec,
    COUNT(*) FILTER (WHERE event_type = 'page_view') AS page_view_rows,
    STDDEV_SAMP(duration_sec) AS duration_sec_stddev,
    BIT_OR(duration_sec) AS duration_sec_bit_or,
    COUNT(*) FILTER (WHERE event_type = 'search') AS search_rows
FROM base
GROUP BY event_type
HAVING duration_sec_stddev >= 0
ORDER BY event_type DESC;
