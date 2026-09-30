WITH prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) AS prev_paid_at
    FROM payments
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
prev_events_2 AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) AS prev_paid_at_2
    FROM islands
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at_2 IS NULL OR paid_at > prev_paid_at_2 + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events_2
),
islands_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY method ORDER BY amount) AS latest_rank
    FROM islands_payments
    QUALIFY latest_rank = 1
),
joined AS (
    -- Attach orders attributes
    SELECT
        a.*,
        d.order_date,
        d.channel
    FROM ranked a
    LEFT JOIN orders d
        ON a.order_id = d.id
),
daily AS (
    -- Grouped metrics
    SELECT
        CASE WHEN new_group_flag >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS new_group_flag_tier,
        DATE_TRUNC('WEEK', prev_paid_at) AS week_start,
        DATE_TRUNC('MONTH', prev_paid_at_2) AS activity_month,
        PERCENTILE_APPROX(island_id, 0.95) AS island_id_p95,
        ROUND(AVG(session_id), 2) AS avg_session_id,
        GROUPING(CASE WHEN new_group_flag >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END) AS gid
    FROM joined
    GROUP BY GROUPING SETS ((CASE WHEN new_group_flag >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('WEEK', prev_paid_at), DATE_TRUNC('MONTH', prev_paid_at_2)), (CASE WHEN new_group_flag >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('WEEK', prev_paid_at)), (CASE WHEN new_group_flag >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END), ())
    HAVING island_id_p95 >= 0
)
SELECT
    new_group_flag_tier,
    week_start,
    island_id_p95,
    avg_session_id
FROM daily
ORDER BY island_id_p95 DESC
