WITH cleaned AS (
    SELECT
        *,
        CASE WHEN duration_sec >= 407 THEN 'High' WHEN duration_sec >= 215 THEN 'Medium' ELSE 'Low' END AS duration_sec_level,
        CONCAT_WS('-', page, event_type) AS page_event_type_key
    FROM web_events
    WHERE event_time >= TO_DATE('2024-01-19')
      AND event_type IN ('page_view', 'click', 'add_to_cart')
),
base AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        TRIM(page) AS clean_page,
        COALESCE(page, 'N/A') AS page_filled
    FROM cleaned
    WHERE event_time >= DATE '2023-06-06'
      AND page IS NOT NULL
),
enriched AS (
    SELECT
        a.*,
        s.country,
        s.device,
        s.browser
    FROM base a
    INNER JOIN sessions s
        ON a.session_id = s.id
)
SELECT
    country,
    session_id,
    COUNT(CASE WHEN device = 'mobile' THEN 1 END) AS mobile_count,
    SIZE(COLLECT_SET(device)) AS num_devices,
    COUNT(DISTINCT page) AS distinct_pages
FROM enriched
GROUP BY GROUPING SETS ((country, session_id), (country))
ORDER BY distinct_pages DESC NULLS FIRST;
