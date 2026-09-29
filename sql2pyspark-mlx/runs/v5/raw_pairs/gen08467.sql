WITH ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM sessions
    QUALIFY RANK() OVER (PARTITION BY browser ORDER BY started_at DESC) = 1
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.duration_sec
    FROM ranked s
    JOIN LATERAL (
            SELECT c.duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
matched_keys AS (
    -- Set intersection on keys
    SELECT device, customer_id
    FROM with_last
    INTERSECT
    SELECT device, customer_id
    FROM sessions
    WHERE customer_id IS NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_last s
    JOIN matched_keys k ON s.device = k.device AND s.customer_id = k.customer_id
),
kept_keys AS (
    -- Set difference on keys
    SELECT browser, country
    FROM verified
    EXCEPT
    SELECT browser, country
    FROM sessions
    WHERE started_at < '2025-01-21'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM verified s
    JOIN kept_keys k ON s.browser = k.browser AND s.country = k.country
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time >= s.started_at
        )
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        device,
        country,
        duration_sec,
        LENGTH(country) AS country_length,
        COALESCE(country, 'DIRECT') AS country_filled
    FROM not_flagged
    WHERE device IN ('mobile', 'desktop', 'tablet')
)
-- Join a dimension and aggregate
SELECT
    c.segment,
    src.device,
    COUNT(*) AS num_rows,
    MAX(src.country_length) AS max_country_length,
    MIN(src.duration_sec) AS min_duration_sec
FROM filtered src
JOIN customers c ON src.customer_id = c.id
GROUP BY GROUPING SETS ((c.segment, src.device), (c.segment), ())
