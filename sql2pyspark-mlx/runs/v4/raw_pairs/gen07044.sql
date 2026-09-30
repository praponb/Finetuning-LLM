WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, LENGTH(page) AS page_length
    FROM web_events
    WHERE event_time >= TO_TIMESTAMP('2024-04-02 02:00:00')
),
remaining_keys AS (
    -- Set difference on keys
    SELECT page, id
    FROM cleaned
    EXCEPT DISTINCT
    SELECT page, id
    FROM web_events
    WHERE duration_sec BETWEEN 137 AND 455
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN remaining_keys k ON s.page = k.page AND s.id = k.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY event_type ORDER BY duration_sec DESC) AS duration_sec_percent_rank,
        COUNT(duration_sec) OVER (PARTITION BY page) AS page_count_duration_sec
    FROM verified
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        s.*,
        p.country,
        p.device
    FROM with_windows s
    LEFT JOIN sessions p
        ON s.session_id = p.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN duration_sec >= 436 THEN 'High' WHEN duration_sec >= 217 THEN 'Medium' ELSE 'Low' END AS duration_sec_level
    FROM with_dims
    WHERE event_time >= DATE '2024-01-10'
      AND device IS NOT NULL
),
with_dims_web_events AS (
    -- Attach sessions attributes
    SELECT
        s.*,
        p.device AS session_device,
        p.browser
    FROM base s
    LEFT JOIN sessions p
        ON s.session_id = p.id
)
SELECT
    id,
    event_type,
    event_time,
    duration_sec,
    page_length,
    duration_sec_percent_rank,
    device,
    duration_sec_level,
    session_device,
    CASE WHEN device RLIKE '^(mobile|tablet)$' THEN 'CORE' ELSE 'PRIORITY' END AS device_zone
FROM with_dims_web_events
ORDER BY event_time DESC NULLS FIRST;
