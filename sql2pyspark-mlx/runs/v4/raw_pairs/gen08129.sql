WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN event_type IN ('add_to_cart', 'click') THEN 'Group A' ELSE 'Other' END AS event_type_group,
        CONCAT(page, ' ', event_type) AS page_event_type
    FROM web_events
    WHERE event_time >= TIMESTAMP '2023-11-21 22:00:00'
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        s.*,
        p.browser,
        p.country
    FROM valid s
    LEFT JOIN sessions p
        ON s.session_id = p.id
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY session_id ORDER BY duration_sec DESC NULLS FIRST) AS row_num
        FROM enriched
    ) t
    WHERE row_num = 1
)
SELECT
    s.id,
    s.session_id,
    s.event_type,
    s.event_time,
    s.duration_sec,
    s.event_type_group,
    s.page_event_type,
    CASE WHEN event_type RLIKE '^(add_to_cart|purchase)$' THEN 'GROUP_B' WHEN event_type RLIKE '^(page_view|click)$' THEN 'CORE' ELSE 'EXTENDED' END AS event_type_zone,
    (SELECT MIN(x.duration_sec) FROM web_events x WHERE x.session_id = s.session_id) AS min_web_event_duration_sec
FROM ranked s
ORDER BY session_id DESC
