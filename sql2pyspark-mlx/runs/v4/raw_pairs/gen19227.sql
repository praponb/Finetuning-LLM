WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at DESC, id) AS dedup_rank
    FROM sessions
    QUALIFY dedup_rank = 1
),
gaps AS (
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM ranked
),
islands AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
with_windows AS (
    SELECT
        *,
        LAST_VALUE(browser, TRUE) OVER (PARTITION BY country, customer_id ORDER BY dedup_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_browser,
        AVG(island_id) OVER (PARTITION BY device ORDER BY session_start_flag, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS island_id_avg_6,
        NTILE(4) OVER (PARTITION BY browser ORDER BY session_start_flag, id) AS session_start_flag_bucket,
        SUM(session_start_flag) OVER (PARTITION BY device ORDER BY session_start_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_session_start_flag,
        ROW_NUMBER() OVER (PARTITION BY device, country ORDER BY island_id DESC, id) AS row_seq
    FROM islands
),
with_window AS (
    SELECT
        e.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM with_windows e
    LEFT JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.started_at BETWEEN sub.start_date AND sub.end_date
)
SELECT
    DATE_TRUNC('YEAR', started_at) AS year_start,
    DATE_TRUNC('MONTH', started_at) AS activity_month,
    DATE_TRUNC('WEEK', started_at) AS week_start,
    SORT_ARRAY(COLLECT_SET(device)) AS device_list,
    COUNT(DISTINCT device) AS distinct_devices,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS duration_min,
    APPROX_PERCENTILE(island_id_avg_6, 0.95) AS island_id_avg_6_p95,
    COUNT_IF(plan = 'basic') AS basic_events,
    COUNT(CASE WHEN device = 'tablet' THEN 1 END) AS tablet_count
FROM with_window
GROUP BY DATE_TRUNC('YEAR', started_at), DATE_TRUNC('MONTH', started_at), DATE_TRUNC('WEEK', started_at);
