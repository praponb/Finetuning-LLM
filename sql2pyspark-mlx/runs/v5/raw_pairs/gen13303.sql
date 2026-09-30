WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        event_type,
        event_time,
        duration_sec,
        INITCAP(TRIM(event_type)) event_type_title,
        COALESCE(page, 'N/A') resolved_page
    FROM web_events
    WHERE event_time < '2024-04-01'
),
kept_keys AS (
    -- Set difference on keys
    SELECT event_type, session_id
    FROM cleaned
    EXCEPT ALL
    SELECT event_type, session_id
    FROM web_events
    WHERE event_time < '2024-12-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    JOIN kept_keys k ON s.event_type = k.event_type AND s.session_id = k.session_id
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        a.id,
        a.session_id,
        a.event_type,
        a.duration_sec,
        a.event_type_title,
        a.resolved_page,
        d.device,
        d.country
    FROM verified a
    LEFT JOIN sessions d
        ON a.session_id = d.id
    WHERE d.customer_id IS NULL
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        a.id,
        a.session_id,
        a.event_type,
        a.duration_sec,
        a.event_type_title,
        a.resolved_page,
        b.browser
    FROM with_dims a
    JOIN sessions b
        ON a.session_id = b.id
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        session_id,
        event_type,
        duration_sec,
        ROUND(duration_sec * 100, 2) duration_sec_adjusted
    FROM enriched
    WHERE browser IS NOT NULL
      AND duration_sec <= 137
)
-- Window metrics over several different partitions
SELECT
    id,
    session_id,
    MIN(duration_sec_adjusted) OVER (PARTITION BY event_type, session_id) event_type_min_duration_sec_adjusted,
    SUM(duration_sec_adjusted) OVER (PARTITION BY event_type) event_type_sum_duration_sec_adjusted
FROM filtered
