WITH enriched AS (
    -- Attach sessions attributes
    SELECT
        e.*,
        b.browser,
        b.country,
        b.device
    FROM web_events e
    JOIN sessions b
        ON e.session_id = b.id
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        event_type,
        event_time,
        duration_sec,
        browser,
        country,
        WEEKOFYEAR(event_time) event_time_weekofyear,
        ROUND(duration_sec, 1) duration_sec_rounded,
        COALESCE(country, 'NONE') resolved_country
    FROM enriched
    WHERE event_time >= TO_TIMESTAMP('2023-04-22 09:00:00')
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(event_time) OVER (PARTITION BY country ORDER BY event_time, id) AS prev_event_time
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY country ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        event_type,
        event_time,
        browser,
        country,
        event_time_weekofyear,
        duration_sec_rounded,
        resolved_country,
        is_new_session,
        island_id,
        GREATEST(island_id, duration_sec_rounded) greatest_island_id_duration_sec_rounded,
        HOUR(prev_event_time) prev_event_time_hour,
        COALESCE(country, 'NONE') country_clean
    FROM sessionized
    WHERE event_time_weekofyear >= 73
)
SELECT
    id,
    event_time,
    country,
    event_time_weekofyear,
    island_id,
    greatest_island_id_duration_sec_rounded,
    prev_event_time_hour,
    country_clean
FROM filtered;
