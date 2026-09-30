WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM orders s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.order_id = s.id
              AND c.unit_price > 350
        )
),
enriched AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.status,
        src.channel,
        src.amount,
        src.shipping_fee,
        b.age,
        b.city
    FROM kept src
    LEFT JOIN customers b
        ON src.customer_id <=> b.id
    WHERE b.segment = 'Consumer'
),
enriched_orders AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        status,
        shipping_fee,
        age,
        city,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY shipping_fee, id) seq_no,
        CORR(amount, shipping_fee) OVER (PARTITION BY status) corr_amount_shipping_fee
    FROM enriched
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        s.shipping_fee,
        s.city,
        d.segment,
        d.credit_limit
    FROM enriched_orders s
    LEFT JOIN customers d
        ON s.customer_id = d.id
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY segment ORDER BY credit_limit DESC NULLS LAST) AS row_num
    FROM with_dims
    QUALIFY row_num = 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id, segment ORDER BY shipping_fee DESC NULLS LAST, id) = 1
),
summary AS (
    -- Grouped metrics
    SELECT
        segment,
        city,
        SUM(CASE WHEN city = 'Tokyo' THEN credit_limit ELSE 0 END) AS tokyo_credit_limit,
        STDDEV_SAMP(shipping_fee) AS shipping_fee_stddev,
        APPROX_PERCENTILE(credit_limit, 0.5) AS credit_limit_p50,
        COUNT(CASE WHEN city = 'Singapore' THEN 1 END) AS singapore_count,
        PERCENTILE_APPROX(credit_limit, 0.9) AS credit_limit_p90
    FROM deduped
    GROUP BY segment, city
)
SELECT
    segment,
    city,
    credit_limit_p50,
    singapore_count,
    credit_limit_p90
FROM summary
ORDER BY singapore_count, credit_limit_p50;
