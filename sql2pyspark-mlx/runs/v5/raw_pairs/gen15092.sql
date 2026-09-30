WITH joined AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        s.started_at,
        s.country,
        c.name
    FROM sessions s
    LEFT JOIN customers c
        ON s.customer_id = c.id
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM joined s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.page IN ('/search', '/cart')
        )
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) AS prev_started_at
    FROM not_flagged
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM prev_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(session_start_flag) OVER (PARTITION BY country) AS country_avg_session_start_flag,
        FIRST_VALUE(country, TRUE) OVER (ORDER BY prev_started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_country
    FROM islands
    WHERE customer_id IS NOT NULL
),
with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.country,
        s.session_start_flag,
        s.session_no,
        s.country_avg_session_start_flag,
        s.first_known_country,
        latest.page
    FROM with_windows s
    JOIN LATERAL (
            SELECT c.page
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Multi-level aggregation
SELECT
    session_no,
    CASE WHEN session_start_flag >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS session_start_flag_tier,
    SIZE(COLLECT_SET(page)) AS num_pages,
    SUM(session_start_flag) FILTER (WHERE country = 'SG') AS sg_total_session_start_flag,
    GROUPING_ID() AS grouping_level
FROM with_latest
GROUP BY CUBE (session_no, CASE WHEN session_start_flag >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END);
