WITH enriched AS (
    -- Attach sessions attributes
    SELECT
        s.*,
        d.device,
        d.country
    FROM web_events s
    INNER JOIN sessions d
        ON s.session_id = d.id
    WHERE d.browser NOT IN ('Safari', 'Chrome')
)
SELECT
    s.id,
    s.session_id,
    s.event_type,
    s.page,
    s.duration_sec,
    s.country,
    CASE WHEN country RLIKE '^(DE|SG)$' THEN 'CORE' WHEN country RLIKE '^(TH|US)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS country_zone,
    (SELECT MAX(x.duration_sec) FROM web_events x WHERE x.session_id = s.session_id) AS max_web_event_duration_sec
FROM enriched s
ORDER BY max_web_event_duration_sec DESC NULLS LAST, country_zone DESC;
