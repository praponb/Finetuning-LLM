WITH with_dims AS (
    -- Attach orders attributes
    SELECT src.*, p.amount AS order_amount
    FROM payments src
    LEFT JOIN orders p
        ON src.order_id = p.id
),
with_dims_payments AS (
    -- Attach orders attributes
    SELECT
        a.*,
        b.discount,
        b.shipping_fee
    FROM with_dims a
    LEFT JOIN orders b
        ON a.order_id = b.id
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM with_dims_payments
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, status ORDER BY paid_at DESC NULLS LAST, id) AS row_num
        FROM islands
    ) t
    WHERE row_num = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(is_new_session) OVER (PARTITION BY order_id, method ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS is_new_session_max_7d,
        ROW_NUMBER() OVER (PARTITION BY method, status ORDER BY paid_at, id) AS seq_no,
        FIRST_VALUE(discount) IGNORE NULLS OVER (PARTITION BY method ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_discount,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY row_num DESC, id) AS seq_no_2,
        AVG(order_amount) OVER (PARTITION BY status) AS status_avg_order_amount
    FROM latest
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        amount,
        status,
        order_amount,
        shipping_fee,
        is_new_session,
        session_no,
        row_num,
        is_new_session_max_7d,
        first_known_discount,
        status_avg_order_amount,
        COALESCE(status_avg_order_amount, 0) AS status_avg_order_amount_filled
    FROM metrics
    WHERE paid_at > TIMESTAMP '2023-05-30 14:00:00'
)
SELECT
    id,
    amount,
    status,
    shipping_fee,
    is_new_session,
    session_no,
    row_num,
    status_avg_order_amount,
    status_avg_order_amount_filled,
    CASE WHEN status RLIKE '^(SUCCESS|FAILED)$' THEN 'GROUP_A' WHEN status RLIKE '^(REFUNDED)$' THEN 'EXTENDED' ELSE 'GROUP_B' END AS status_zone
FROM base
ORDER BY is_new_session
