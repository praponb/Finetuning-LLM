WITH valid AS (
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        HOUR(started_at) AS started_at_hour,
        LOWER(device) AS device_lower
    FROM sessions
    WHERE country IS NOT NULL
),
prev_events AS (
    SELECT *, LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) AS prev_started_at
    FROM valid
),
with_gaps AS (
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events
),
sessions_sessions AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
eligible AS (
    SELECT s.*
    FROM sessions_sessions s
    WHERE EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time >= s.prev_started_at
        )
),
matched_keys AS (
    SELECT id, customer_id
    FROM eligible
    EXCEPT ALL
    SELECT id, customer_id
    FROM sessions
    WHERE HOUR(started_at) BETWEEN 8 AND 16
),
reconciled AS (
    SELECT s.*
    FROM eligible s
    INNER JOIN matched_keys k ON s.id = k.id AND s.customer_id = k.customer_id
)
SELECT
    CASE WHEN new_group_flag >= 1 THEN 'LARGE' ELSE 'SMALL' END AS new_group_flag_tier,
    ROUND(SUM(session_no) / NULLIF(COUNT(CASE WHEN browser = 'Firefox' THEN 1 END), 0), 2) AS session_no_per_firefox,
    COUNT(*) FILTER (WHERE device = 'mobile') AS mobile_rows,
    SUM(CASE WHEN device = 'desktop' THEN session_no ELSE 0 END) AS desktop_session_no,
    COUNT(*) AS event_count,
    PERCENTILE_APPROX(new_group_flag, ARRAY(0.25, 0.5, 0.75)) AS new_group_flag_quartiles
FROM reconciled
GROUP BY CASE WHEN new_group_flag >= 1 THEN 'LARGE' ELSE 'SMALL' END
HAVING ROUND(SUM(session_no) / NULLIF(COUNT(CASE WHEN browser = 'Firefox' THEN 1 END), 0), 2) >= 0
ORDER BY new_group_flag_tier DESC NULLS FIRST
