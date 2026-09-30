WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        ROUND(duration_sec * 0.85, 2) duration_sec_adjusted,
        SUBSTRING(page, 1, 4) page_prefix
    FROM web_events
    WHERE page IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        duration_sec_adjusted,
        SUM(duration_sec_adjusted) OVER w_running duration_sec_adjusted_sum_30d,
        LAST_VALUE(duration_sec_adjusted, TRUE) OVER w_running_2 last_known_duration_sec_adjusted
    FROM valid
    WINDOW w_running AS (PARTITION BY page ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW), w_running_2 AS (PARTITION BY event_type ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
valid_web_events AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        last_known_duration_sec_adjusted - duration_sec_adjusted_sum_30d last_known_duration_sec_adjusted_minus_duration_sec_adjusted_sum_30d,
        FLOOR(duration_sec_adjusted) duration_sec_adjusted_whole
    FROM metrics
    WHERE duration_sec_adjusted IS NOT NULL
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM valid_web_events
    INTERSECT
    SELECT id
    FROM web_events
    WHERE session_id IN (4, 16, 25)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid_web_events s
    INNER JOIN kept_keys k ON s.id = k.id
)
SELECT
    duration_sec_adjusted_sum_30d,
    last_known_duration_sec_adjusted,
    last_known_duration_sec_adjusted_minus_duration_sec_adjusted_sum_30d
FROM kept_rows;
