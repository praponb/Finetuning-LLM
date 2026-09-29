WITH prev_events AS (
    SELECT *, LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) AS prev_started_at
    FROM sessions
),
with_gaps AS (
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM prev_events
),
sessionized AS (
    SELECT
        id,
        customer_id,
        device,
        started_at,
        country,
        SUM(is_new_session) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
)
SELECT
    CASE WHEN session_no >= 8 THEN 'LARGE' ELSE 'SMALL' END AS session_no_tier,
    DATE_TRUNC('YEAR', started_at) AS year_start,
    SORT_ARRAY(COLLECT_SET(device)) AS device_list,
    PERCENTILE_APPROX(session_no, 0.5) AS session_no_p50,
    APPROX_PERCENTILE(session_no, ARRAY(0.25, 0.5, 0.75)) AS session_no_quartiles,
    GROUPING_ID() AS gid
FROM sessionized
GROUP BY GROUPING SETS ((CASE WHEN session_no >= 8 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('YEAR', started_at)), (CASE WHEN session_no >= 8 THEN 'LARGE' ELSE 'SMALL' END))
