WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        method,
        status,
        CAST(amount AS INT) AS amount_int,
        COALESCE(method, 'OTHER') AS method_clean
    FROM payments
    WHERE paid_at >= TIMESTAMP '2023-10-11 20:00:00'
),
joined AS (
    -- Attach orders attributes
    SELECT
        a.*,
        d.order_date,
        d.shipping_fee,
        d.amount
    FROM base a
    LEFT JOIN orders d
        ON a.order_id = d.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        method,
        status,
        amount_int,
        method_clean,
        order_date,
        shipping_fee,
        amount,
        FIRST_VALUE(method, TRUE) OVER (PARTITION BY status, order_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_method,
        NTILE(5) OVER (ORDER BY amount DESC, id) AS amount_bucket
    FROM joined
),
with_dims AS (
    -- Attach orders attributes
    SELECT src.*, o.amount AS order_amount
    FROM scored src
    LEFT JOIN orders o
        ON src.order_id = o.id
)
-- Ranking and gap features
SELECT
    *,
    AVG(amount) OVER (ORDER BY order_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS amount_avg_6_prior,
    DENSE_RANK() OVER (PARTITION BY status ORDER BY amount_int) AS amount_int_dense_rank,
    AVG(amount_bucket) OVER (PARTITION BY order_id ORDER BY shipping_fee, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS amount_bucket_avg_3,
    LAG(order_amount, 1) OVER (PARTITION BY status ORDER BY amount, id) AS prev_order_amount,
    NTILE(4) OVER (PARTITION BY status ORDER BY order_amount DESC, id) AS order_amount_bucket
FROM with_dims
