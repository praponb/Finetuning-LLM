WITH valid AS (
    SELECT
        id,
        device,
        browser,
        country,
        CASE WHEN device IN ('tablet', 'mobile') THEN 'Group A' ELSE 'Other' END AS device_group,
        SUBSTRING(device, 1, 4) AS device_prefix,
        COALESCE(browser, 'NONE') AS resolved_browser
    FROM sessions
    WHERE browser IS NOT DISTINCT FROM 'Edge'
),
with_last AS (
    SELECT
        s.id,
        s.browser,
        s.country,
        s.device_prefix,
        s.resolved_browser,
        latest.web_event_page,
        latest.event_time
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.page AS web_event_page, c.event_time
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY browser ORDER BY event_time DESC) AS row_num
        FROM with_last
    ) x
    WHERE row_num = 1
),
gaps AS (
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY web_event_page ORDER BY event_time, id) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM deduped
),
islands AS (
    SELECT
        id,
        device_prefix,
        web_event_page,
        event_time,
        row_num,
        SUM(session_start_flag) OVER (PARTITION BY web_event_page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
)
SELECT
    id,
    web_event_page,
    event_time,
    row_num,
    session_no
FROM islands
ORDER BY id
LIMIT 20
