WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country, customer_id ORDER BY started_at DESC NULLS FIRST, id) AS rn
    FROM sessions
    QUALIFY rn = 1
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM deduped
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device ORDER BY rn DESC, id) = 1
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time < s.started_at
        )
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        device,
        started_at,
        rn,
        FIRST_VALUE(country, TRUE) OVER (ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_country,
        LAST_VALUE(browser) IGNORE NULLS OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_browser,
        LAST_VALUE(country, TRUE) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_country
    FROM eligible
),
ranked_sessions AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device ORDER BY rn DESC, id) = 1
),
eligible_sessions AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM ranked_sessions s
    WHERE EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
        )
),
stats AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('QUARTER', started_at) AS quarter_start,
        DATE_TRUNC('WEEK', started_at) AS week_start,
        device,
        SORT_ARRAY(COLLECT_SET(device)) AS device_list,
        SORT_ARRAY(COLLECT_SET(device)) AS device_list_2,
        MIN(started_at) AS first_seen,
        SORT_ARRAY(COLLECT_SET(device)) AS device_list_3,
        COUNT(*) AS num_rows,
        GROUPING_ID() AS grouping_level
    FROM eligible_sessions
    GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', started_at), DATE_TRUNC('WEEK', started_at), device), (DATE_TRUNC('QUARTER', started_at), DATE_TRUNC('WEEK', started_at)))
)
SELECT
    quarter_start,
    week_start,
    device,
    device_list_2,
    first_seen,
    num_rows,
    grouping_level
FROM stats
ORDER BY quarter_start DESC
