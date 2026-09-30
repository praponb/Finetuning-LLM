WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LOWER(method) AS method_lower,
        SUBSTRING(method, 1, 4) AS method_prefix
    FROM payments
    WHERE method != 'promptpay'
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) AS prev_paid_at
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL '60' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM with_prev
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
flagged_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) > INTERVAL '7' DAY THEN 1 ELSE 0 END AS new_group_flag_2
    FROM islands
),
islands_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag_2) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged_2
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        order_id,
        status,
        amount
    FROM islands_payments
) src
PIVOT (
    SUM(amount) AS amount,
    COUNT(1) AS item_count
    FOR status IN ('FAILED', 'REFUNDED')
);
