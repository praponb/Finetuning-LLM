WITH base AS (
    SELECT
        *,
        SUBSTRING(event_type, 1, 4) AS event_type_prefix,
        COALESCE(page, 'DIRECT') AS page_clean
    FROM web_events
    WHERE page <> '/product'
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY page, session_id ORDER BY duration_sec DESC, id) AS row_num
        FROM base
    ) x
    WHERE row_num = 1
),
kept_keys AS (
    SELECT event_type
    FROM ranked
    EXCEPT DISTINCT
    SELECT event_type
    FROM web_events
    WHERE event_type <> 'page_view'
),
reconciled AS (
    SELECT s.*
    FROM ranked s
    INNER JOIN kept_keys k ON s.event_type = k.event_type
),
enriched AS (
    SELECT
        s.id,
        s.session_id,
        s.event_type,
        s.page,
        s.event_time,
        s.event_type_prefix,
        d.device,
        d.country,
        d.browser
    FROM reconciled s
    LEFT JOIN sessions d
        ON s.session_id = d.id
),
cleaned AS (
    SELECT *, CASE WHEN event_type IN ('purchase', 'click') THEN 'Core' ELSE 'Other' END AS event_type_group
    FROM enriched
    WHERE event_time >= TO_TIMESTAMP('2023-08-30 03:00:00')
      AND browser IS NOT NULL
)
SELECT
    s.session_id,
    s.event_type,
    s.page,
    s.event_time,
    s.event_type_prefix,
    s.device,
    s.country,
    s.event_type_group,
    CASE WHEN event_type RLIKE '^(search|purchase)$' THEN 'GROUP_A' WHEN event_type RLIKE '^(page_view|click)$' THEN 'PRIORITY' ELSE 'CORE' END AS event_type_zone,
    (SELECT MAX(x.duration_sec) FROM web_events x WHERE x.session_id = s.session_id) AS max_web_event_duration_sec
FROM cleaned s
ORDER BY device DESC, page DESC;
