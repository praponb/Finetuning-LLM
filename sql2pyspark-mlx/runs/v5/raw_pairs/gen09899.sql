WITH scored AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY method ORDER BY amount, id) AS amount_bucket,
        LAG(amount, 1, 0) OVER w_part AS prev_amount
    FROM payments
    WINDOW w_part AS (PARTITION BY order_id ORDER BY paid_at, id)
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        amount_bucket,
        prev_amount,
        amount * prev_amount AS amount_times_prev_amount,
        COALESCE(method, 'DIRECT') AS resolved_method
    FROM scored
    WHERE paid_at >= DATE '2023-09-22'
      AND method IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        STDDEV_SAMP(amount_bucket) OVER (PARTITION BY order_id) AS order_id_stddev_samp_amount_bucket,
        SUM(amount) OVER w_running AS method_sum_amount
    FROM base
    WINDOW w_running AS (PARTITION BY method)
),
scored_payments AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(amount_bucket, TRUE) OVER (PARTITION BY order_id, status ORDER BY amount_times_prev_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount_bucket,
        NTILE(10) OVER (PARTITION BY status, order_id ORDER BY amount, id) AS amount_bucket_2
    FROM enriched
),
enriched_payments AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.amount,
        e.prev_amount,
        e.resolved_method,
        e.method_sum_amount,
        d.shipping_fee,
        d.status AS order_status
    FROM scored_payments e
    JOIN orders d
        ON e.order_id = d.id
)
-- Join a dimension and aggregate
SELECT
    o.channel,
    s.order_status,
    COUNT(*) AS rows_total,
    MIN(s.shipping_fee) AS min_shipping_fee
FROM enriched_payments s
JOIN orders o ON s.order_id = o.id
GROUP BY CUBE (o.channel, s.order_status)
ORDER BY channel NULLS FIRST;
