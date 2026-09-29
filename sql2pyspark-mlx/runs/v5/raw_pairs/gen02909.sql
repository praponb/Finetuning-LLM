WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        SUBSTRING(browser, 1, 4) AS browser_prefix,
        TRIM(device) AS clean_device,
        COALESCE(country, 'N/A') AS country_filled
    FROM sessions
    WHERE started_at >= TIMESTAMP '2023-03-10 22:00:00'
),
with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.web_event_page,
        x.web_event_duration_sec
    FROM valid s
    JOIN LATERAL (
            SELECT c.page AS web_event_page, c.duration_sec AS web_event_duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        device,
        started_at,
        country,
        browser_prefix,
        country_filled,
        web_event_page,
        LAST_VALUE(web_event_duration_sec, TRUE) OVER (PARTITION BY browser ORDER BY web_event_duration_sec ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_web_event_duration_sec,
        AVG(web_event_duration_sec) OVER w_running AS web_event_duration_sec_avg_2
    FROM with_latest
    WINDOW w_running AS (PARTITION BY customer_id ORDER BY web_event_duration_sec ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time >= s.started_at
        )
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', started_at) AS activity_month,
    COUNT(CASE WHEN device = 'mobile' THEN 1 END) AS mobile_count,
    MIN(started_at) AS first_seen,
    MIN(last_known_web_event_duration_sec) AS min_last_known_web_event_duration_sec,
    SORT_ARRAY(COLLECT_SET(web_event_page)) AS web_event_page_list,
    COUNT_IF(country = 'JP') AS jp_events,
    GROUPING(DATE_TRUNC('MONTH', started_at)) AS grouping_id
FROM eligible
GROUP BY CUBE (DATE_TRUNC('MONTH', started_at))
ORDER BY activity_month ASC;
