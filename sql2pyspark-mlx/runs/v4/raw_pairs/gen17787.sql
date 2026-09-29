WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN duration_sec >= 382 THEN 'High' WHEN duration_sec >= 215 THEN 'Medium' ELSE 'Low' END AS duration_sec_band,
        DATEDIFF(CURRENT_DATE(), event_time) AS days_since_event_time,
        COALESCE(page, 'DIRECT') AS page_clean
    FROM web_events
    WHERE TO_DATE(event_time) >= '2025-10-01'
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY session_id, page ORDER BY event_time DESC NULLS LAST, id) = 1
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM ranked
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN page IN ('/product', '/search') THEN 'Group A' ELSE 'Other' END AS page_group,
        COALESCE(page, 'UNKNOWN') AS resolved_page
    FROM islands
    WHERE event_time >= TO_DATE('2023-12-24')
      AND page IS NOT NULL
      AND session_no >= 7
),
by_type AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            event_type,
            page,
            days_since_event_time
        FROM cleaned
    ) src
    PIVOT (
        SUM(days_since_event_time) AS total
        FOR page IN ('/product' AS product, '/home' AS home)
    )
)
SELECT event_type, product
FROM by_type
ORDER BY event_type DESC;
