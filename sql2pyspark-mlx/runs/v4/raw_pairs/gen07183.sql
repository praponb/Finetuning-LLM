WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LOWER(event_type) AS event_type_lower,
        LENGTH(event_type) AS event_type_length,
        COALESCE(page, 'NONE') AS page_clean
    FROM web_events
    WHERE event_time > DATE '2024-05-17'
)
-- QUALIFY-style window filter
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_type_length DESC, id) AS latest_rank
    FROM valid
) x
WHERE latest_rank = 1
