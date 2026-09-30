WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device,
        browser,
        LAST_VALUE(country, TRUE) OVER w_running AS last_known_country,
        FIRST_VALUE(browser, TRUE) OVER w_grp AS first_known_browser
    FROM sessions
    WINDOW w_running AS (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device,
        browser,
        first_known_browser,
        SUBSTRING(device, 1, 2) AS device_prefix
    FROM scored
    WHERE device IN ('desktop', 'tablet', 'mobile')
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.duration_sec BETWEEN 220 AND 273
        )
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        first_known_browser,
        UPPER(browser) AS browser_upper
    FROM eligible
    WHERE browser IS DISTINCT FROM 'Edge'
),
with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.first_known_browser,
        top1.web_event_event_type,
        top1.duration_sec
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.event_type AS web_event_event_type, c.duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND TO_DATE(ch.event_time) >= '2024-01-04'
        )
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY web_event_event_type ORDER BY duration_sec DESC, id) AS dedup_rank
    FROM not_flagged
    QUALIFY dedup_rank = 1
)
-- Multi-level aggregation
SELECT
    CASE WHEN duration_sec >= 444 THEN 'LARGE' ELSE 'SMALL' END AS duration_sec_tier,
    SIZE(COLLECT_SET(web_event_event_type)) AS num_web_event_event_types,
    ROUND(AVG(duration_sec), 2) AS avg_duration_sec,
    COUNT(*) AS event_count
FROM latest
GROUP BY CASE WHEN duration_sec >= 444 THEN 'LARGE' ELSE 'SMALL' END
HAVING ROUND(AVG(duration_sec), 2) > 0
