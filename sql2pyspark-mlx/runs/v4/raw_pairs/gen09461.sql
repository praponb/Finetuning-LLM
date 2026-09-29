WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM payments
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
joined AS (
    -- Attach orders attributes
    SELECT a.*, p.discount
    FROM islands a
    LEFT JOIN orders p
        ON a.order_id = p.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        session_start_flag,
        HOUR(paid_at) AS paid_at_hour,
        COALESCE(method, 'NONE') AS method_clean
    FROM joined
    WHERE method IS NOT NULL
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        a.*,
        p.amount AS order_amount,
        p.channel
    FROM base a
    LEFT JOIN orders p
        ON a.order_id = p.id
),
flagged_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY channel ORDER BY paid_at, id) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM with_dims
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY channel ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged_2
)
SELECT
    id,
    order_id,
    method,
    amount,
    paid_at,
    status,
    session_start_flag,
    method_clean,
    order_amount,
    CASE WHEN method RLIKE '^(card|promptpay)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS method_zone
FROM sessions_payments
ORDER BY id
