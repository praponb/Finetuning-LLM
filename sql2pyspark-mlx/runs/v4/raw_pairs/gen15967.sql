WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(amount * 1.07, 2) AS amount_scaled,
        COALESCE(method, 'NONE') AS resolved_method
    FROM payments
    WHERE paid_at > DATE '2023-06-03'
),
cleaned_payments AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(amount_scaled, 0) AS amount_scaled_filled,
        ROUND(amount_scaled * 1.15, 2) AS amount_scaled_adjusted
    FROM cleaned
    WHERE method IS NOT NULL
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) AS prev_paid_at
    FROM cleaned_payments
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL '90' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN prev_paid_at - LAG(prev_paid_at) OVER (PARTITION BY status ORDER BY prev_paid_at, id) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM sessionized
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY status ORDER BY prev_paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN burst_id >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS burst_id_tier,
        SUM(amount) AS total_amount,
        COUNT_IF(method = 'card') AS card_events,
        MAX(prev_paid_at) AS ended_at,
        COUNT(*) AS event_count
    FROM islands
    GROUP BY CASE WHEN burst_id >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    burst_id_tier,
    total_amount,
    card_events
FROM grouped
ORDER BY total_amount DESC, card_events DESC;
