WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(amount * 1.07, 2) AS amount_adjusted
    FROM payments
    WHERE order_id IS NOT NULL
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM cleaned
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        s.*,
        p.order_date,
        p.shipping_fee,
        p.status AS order_status
    FROM sessions_payments s
    INNER JOIN orders p
        ON s.order_id = p.id
),
joined AS (
    -- Attach orders attributes
    SELECT src.*, p.channel
    FROM with_dims src
    INNER JOIN orders p
        ON src.order_id = p.id
),
with_dims_payments AS (
    -- Attach orders attributes
    SELECT s.*, d.amount AS order_amount
    FROM joined s
    LEFT JOIN orders d
        ON s.order_id = d.id
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY channel ORDER BY paid_at, id) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM with_dims_payments
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY channel ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id_2
    FROM gaps
)
SELECT
    id,
    amount,
    amount_adjusted,
    new_group_flag,
    island_id,
    order_date,
    shipping_fee,
    order_status,
    order_amount,
    CASE WHEN order_status RLIKE '^(PENDING|COMPLETED)$' THEN 'EXTENDED' ELSE 'CORE' END AS order_status_zone
FROM sessionized
ORDER BY id DESC NULLS FIRST, order_status DESC;
