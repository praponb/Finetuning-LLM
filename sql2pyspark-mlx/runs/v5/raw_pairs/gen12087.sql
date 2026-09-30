WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, TRIM(device) AS clean_device
    FROM sessions
    WHERE started_at > DATE '2023-06-23'
      AND device IN ('tablet', 'desktop', 'mobile')
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.*,
        d.email,
        d.credit_limit
    FROM base src
    LEFT JOIN customers d
        ON src.customer_id <=> d.id
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_dims s
    WHERE EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.duration_sec BETWEEN 179 AND 470
        )
),
with_dims_sessions AS (
    -- Attach customers attributes
    SELECT
        s.*,
        d.city,
        d.age
    FROM eligible s
    LEFT JOIN customers d
        ON s.customer_id = d.id
),
latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.duration_sec,
        x.web_event_page
    FROM with_dims_sessions s
    LEFT JOIN LATERAL (
            SELECT c.duration_sec, c.page AS web_event_page
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
summary AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('WEEK', started_at) AS week_start,
        DATE_TRUNC('YEAR', started_at) AS year_start,
        MIN(started_at) AS started_at,
        (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS duration_min,
        COUNT_IF(web_event_page = '/home') AS home_events,
        COUNT(*) FILTER (WHERE city = 'New York') AS new_york_rows,
        COUNT(*) FILTER (WHERE country = 'JP') AS jp_rows,
        SIZE(COLLECT_SET(country)) AS num_countries
    FROM latest_match
    GROUP BY 1, 2
)
SELECT
    week_start,
    year_start,
    home_events,
    jp_rows,
    num_countries
FROM summary
ORDER BY home_events DESC NULLS FIRST
