WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        CASE WHEN event_type IN ('page_view', 'search') THEN 'Group A' ELSE 'Other' END AS event_type_group,
        SUBSTRING(event_type, 1, 4) AS event_type_prefix,
        COALESCE(page, 'OTHER') AS resolved_page
    FROM web_events
    WHERE event_time >= '2025-01-01'
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LENGTH(page) AS page_length
    FROM base
    WHERE page IS NOT NULL
      AND event_time >= '2025-10-05'
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        page,
        event_time,
        duration_sec,
        event_type_group,
        page_length
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY page, session_id ORDER BY page_length DESC NULLS FIRST, id) = 1
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM ranked
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        page,
        duration_sec,
        event_type_group,
        new_group_flag
    FROM islands
    QUALIFY ROW_NUMBER() OVER (PARTITION BY page ORDER BY burst_id, id) = 1
)
SELECT
    id,
    page,
    duration_sec,
    event_type_group
FROM latest
ORDER BY page;
