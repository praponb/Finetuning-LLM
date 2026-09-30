WITH with_latest AS (
    SELECT
        s.*,
        top1.web_event_event_type,
        top1.web_event_event_time
    FROM sessions s
    LEFT JOIN LATERAL (
            SELECT c.event_type AS web_event_event_type, c.event_time AS web_event_event_time
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
eligible AS (
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.page = '/checkout'
        )
),
remaining_keys AS (
    SELECT id, browser
    FROM eligible
    INTERSECT ALL
    SELECT id, browser
    FROM sessions
    WHERE country IN ('TH', 'DE', 'SG')
),
verified AS (
    SELECT s.*
    FROM eligible s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.browser = k.browser
),
not_flagged AS (
    SELECT s.*
    FROM verified s
    WHERE EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time <= s.web_event_event_time
        )
),
enriched AS (
    SELECT
        src.*,
        c.age,
        c.email
    FROM not_flagged src
    LEFT JOIN customers c
        ON src.customer_id = c.id
),
grouped AS (
    SELECT
        DATE_TRUNC('QUARTER', web_event_event_time) AS quarter_start,
        CASE WHEN age >= 56 THEN 'HIGH' ELSE 'LOW' END AS age_tier,
        DATE_TRUNC('WEEK', started_at) AS week_start,
        COUNT_IF(device = 'tablet') AS tablet_events,
        ROUND(AVG(age), 2) AS avg_age,
        SUM(CASE WHEN browser = 'Safari' THEN age ELSE 0 END) AS safari_age,
        ROUND(AVG(age), 2) AS avg_age_2,
        (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS duration_min
    FROM enriched
    GROUP BY DATE_TRUNC('QUARTER', web_event_event_time), CASE WHEN age >= 56 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('WEEK', started_at)
)
SELECT
    quarter_start,
    age_tier,
    week_start,
    tablet_events,
    avg_age,
    avg_age_2,
    duration_min
FROM grouped
ORDER BY age_tier DESC, avg_age_2
