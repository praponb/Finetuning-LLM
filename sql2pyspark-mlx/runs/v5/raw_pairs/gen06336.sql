WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        event_type,
        duration_sec,
        DATE_FORMAT(event_time, 'yyyy-MM-dd') event_time_label
    FROM web_events
    WHERE event_time >= DATE '2023-04-02'
      AND page IS NOT NULL
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, event_type
    FROM filtered
    INTERSECT
    SELECT id, event_type
    FROM web_events
    WHERE event_time < '2025-09-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN matched_keys k ON s.id = k.id AND s.event_type = k.event_type
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        event_type,
        MIN(duration_sec) OVER (PARTITION BY event_type) event_type_min_duration_sec,
        CUME_DIST() OVER (ORDER BY duration_sec DESC) duration_sec_cume_dist
    FROM verified
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_type,
        event_type_min_duration_sec,
        COUNT(duration_sec_cume_dist) OVER (PARTITION BY event_type) event_type_count_duration_sec_cume_dist
    FROM with_windows
    WHERE duration_sec_cume_dist < 0.74
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN event_type_min_duration_sec >= 410 THEN 'High' WHEN event_type_min_duration_sec >= 227 THEN 'Medium' ELSE 'Low' END event_type_min_duration_sec_tier
    FROM scored
    WHERE event_type_count_duration_sec_cume_dist >= 12
)
SELECT
    id,
    event_type,
    event_type_min_duration_sec
FROM base;
