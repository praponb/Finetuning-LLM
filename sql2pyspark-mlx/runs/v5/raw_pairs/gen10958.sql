WITH filtered AS (
    SELECT *, CASE WHEN amount >= 1700 THEN 'High' WHEN amount >= 470 THEN 'Medium' ELSE 'Low' END AS amount_level
    FROM payments
    WHERE amount BETWEEN 660 AND 1300
),
prev_events AS (
    SELECT *, LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) AS prev_paid_at
    FROM filtered
),
flagged AS (
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events
),
sessions_payments AS (
    SELECT
        id,
        order_id,
        paid_at,
        SUM(new_group_flag) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
SELECT
    CASE WHEN session_no >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS session_no_tier,
    MIN(paid_at) AS first_paid_at,
    MIN(paid_at) AS first_seen,
    MIN(paid_at) AS first_seen_2
FROM sessions_payments
GROUP BY session_no_tier
