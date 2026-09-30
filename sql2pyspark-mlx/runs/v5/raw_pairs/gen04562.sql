WITH with_dims AS (
    SELECT
        src.id,
        src.order_id,
        src.method,
        src.amount,
        src.paid_at,
        src.status,
        p.status AS order_status,
        p.amount AS order_amount
    FROM payments src
    LEFT JOIN orders p
        ON src.order_id = p.id
),
scored AS (
    SELECT
        id,
        order_id,
        status,
        order_amount,
        LEAD(order_amount, 1) OVER (PARTITION BY order_status ORDER BY paid_at, id) AS next_order_amount,
        LAST_VALUE(order_status) IGNORE NULLS OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_order_status
    FROM with_dims
),
enriched AS (
    SELECT
        id,
        order_id,
        next_order_amount,
        last_known_order_status,
        LAST_VALUE(next_order_amount, TRUE) OVER (PARTITION BY status, order_id ORDER BY order_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_next_order_amount,
        SUM(next_order_amount) OVER (PARTITION BY status) AS status_sum_next_order_amount,
        DENSE_RANK() OVER (PARTITION BY order_id ORDER BY order_amount DESC NULLS LAST) AS order_amount_dense_rank,
        AVG(order_amount) OVER (PARTITION BY status ORDER BY next_order_amount, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS order_amount_avg_3_prior
    FROM scored
),
joined AS (
    SELECT
        src.id,
        src.order_id,
        src.last_known_order_status,
        src.last_known_next_order_amount,
        src.order_amount_dense_rank,
        src.order_amount_avg_3_prior,
        o.status
    FROM enriched src
    LEFT JOIN orders o
        ON src.order_id <=> o.id
),
base AS (
    SELECT *, LENGTH(status) AS status_length
    FROM joined
    WHERE status = 'PENDING'
),
with_dims_payments AS (
    SELECT
        e.id,
        e.order_id,
        e.last_known_order_status,
        e.status,
        e.status_length,
        b.discount,
        b.channel,
        b.status AS order_status
    FROM base e
    LEFT JOIN orders b
        ON e.order_id = b.id
)
SELECT
    s.id,
    s.order_id,
    s.last_known_order_status,
    s.status,
    s.status_length,
    s.channel,
    s.order_status,
    CASE WHEN channel RLIKE '^(store|web)$' THEN 'PRIORITY' WHEN channel RLIKE '^(mobile|partner)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS channel_zone,
    (SELECT MAX(t2.amount) FROM payments t2 WHERE t2.order_id = s.order_id) AS max_payment_amount
FROM with_dims_payments s
ORDER BY channel DESC NULLS FIRST
