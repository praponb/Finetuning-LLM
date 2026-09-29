WITH base AS (
    SELECT
        id,
        customer_id,
        browser,
        started_at,
        MONTH(started_at) AS started_at_month,
        DATE_FORMAT(started_at, 'yyyy-MM-dd') AS started_at_label
    FROM sessions
    WHERE started_at >= TIMESTAMP '2023-07-04 19:00:00'
      AND customer_id IS NOT NULL
),
with_last AS (
    SELECT
        s.*,
        latest.web_event_duration_sec,
        latest.event_type
    FROM base s
    JOIN LATERAL (
            SELECT c.duration_sec AS web_event_duration_sec, c.event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
flagged AS (
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM with_last
),
islands AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
valid AS (
    SELECT
        id,
        customer_id,
        started_at,
        started_at_month,
        started_at_label,
        web_event_duration_sec,
        event_type,
        session_start_flag,
        QUARTER(started_at) AS started_at_quarter,
        COALESCE(browser, 'UNKNOWN') AS resolved_browser
    FROM islands
    WHERE started_at > TO_DATE('2024-01-12')
),
kept_keys AS (
    SELECT id, customer_id
    FROM valid
    EXCEPT DISTINCT
    SELECT id, customer_id
    FROM sessions
    WHERE started_at < '2024-01-08'
),
verified AS (
    SELECT s.*
    FROM valid s
    INNER JOIN kept_keys k ON s.id = k.id AND s.customer_id = k.customer_id
),
enriched AS (
    SELECT
        *,
        COUNT(started_at_month) OVER (PARTITION BY customer_id ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS started_at_month_count_90d,
        AVG(started_at_month) OVER w_running AS started_at_month_avg_7d
    FROM verified
    WINDOW w_running AS (PARTITION BY event_type ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW)
),
filtered AS (
    SELECT *, DATE_FORMAT(started_at, 'yyyy-MM') AS started_at_label_2
    FROM enriched
    WHERE started_at >= TO_DATE('2023-10-19')
      AND customer_id IS NOT NULL
      AND started_at_month_avg_7d <> 59
)
SELECT
    DATE_TRUNC('WEEK', started_at) AS week_start,
    SUM(CASE WHEN event_type = 'search' THEN started_at_month ELSE 0 END) AS search_started_at_month,
    MIN(started_at) AS first_started_at,
    SUM(session_start_flag) AS total_session_start_flag
FROM filtered
GROUP BY DATE_TRUNC('WEEK', started_at)
