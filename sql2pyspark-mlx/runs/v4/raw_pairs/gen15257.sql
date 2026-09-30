WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN status IN ('REFUNDED', 'SUCCESS') THEN 'Group A' ELSE 'Other' END AS status_group
    FROM payments
    WHERE paid_at > TIMESTAMP '2023-06-29 23:00:00'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(amount) OVER (PARTITION BY order_id, status) AS order_id_max_amount,
        ROUND(amount / SUM(amount) OVER (PARTITION BY status, method), 4) AS amount_share
    FROM base
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at ASC, id ASC) AS prev_paid_at
    FROM metrics
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY status ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, WEEKOFYEAR(prev_paid_at) AS prev_paid_at_weekofyear
    FROM sessions_payments
    WHERE method IS NOT NULL
      AND amount BETWEEN 1100 AND 1360
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        a.*,
        d.amount AS order_amount,
        d.discount
    FROM cleaned a
    JOIN orders d
        ON a.order_id = d.id
)
SELECT
    id,
    order_id,
    method,
    status,
    status_group,
    order_id_max_amount,
    prev_paid_at,
    is_new_burst,
    burst_id
FROM with_dims
ORDER BY is_new_burst ASC
