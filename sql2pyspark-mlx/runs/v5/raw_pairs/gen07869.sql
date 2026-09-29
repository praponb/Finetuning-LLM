WITH remaining_keys AS (
    -- Set intersection on keys
    SELECT id, page
    FROM web_events
    INTERSECT
    SELECT id, page
    FROM web_events
    WHERE session_id IN (16, 26, 31)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM web_events s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.page = k.page
),
joined AS (
    -- Attach sessions attributes
    SELECT
        src.*,
        b.device,
        b.browser,
        b.country
    FROM reconciled src
    LEFT JOIN sessions b
        ON src.session_id = b.id
    WHERE b.device = 'desktop'
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        a.session_id,
        a.event_time,
        a.device,
        a.browser,
        p.device AS session_device
    FROM joined a
    LEFT JOIN sessions p
        ON a.session_id = p.id
),
remaining_keys_2 AS (
    -- Set difference on keys
    SELECT session_id
    FROM enriched
    EXCEPT ALL
    SELECT session_id
    FROM web_events
    WHERE event_type IN ('click', 'purchase')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN remaining_keys_2 k ON s.session_id = k.session_id
),
joined_web_events AS (
    -- Attach sessions attributes
    SELECT
        a.*,
        p.browser AS session_browser,
        p.country,
        p.device AS session_device_2
    FROM kept_rows a
    INNER JOIN sessions p
        ON a.session_id = p.id
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        src.session_id,
        src.event_time,
        src.browser,
        b.device AS session_device_3
    FROM joined_web_events src
    LEFT JOIN sessions b
        ON src.session_id = b.id
),
joined_web_events_2 AS (
    -- Attach sessions attributes
    SELECT
        s.*,
        p2.browser AS session_browser,
        p2.device
    FROM with_dims s
    INNER JOIN sessions p2
        ON s.session_id = p2.id
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', event_time) AS year_start,
    MIN(event_time) AS first_seen,
    COUNT(CASE WHEN session_device_3 = 'mobile' THEN 1 END) AS mobile_count
FROM joined_web_events_2
GROUP BY year_start
ORDER BY year_start DESC
