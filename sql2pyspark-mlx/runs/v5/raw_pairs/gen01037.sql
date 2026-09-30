WITH joined AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.status,
        o.amount AS order_amount,
        o.status AS order_status,
        o.channel
    FROM payments e
    JOIN orders o
        ON e.order_id = o.id
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        src.*,
        d.channel AS order_channel,
        d.shipping_fee,
        d.amount
    FROM joined src
    LEFT JOIN orders d
        ON src.order_id = d.id
    WHERE d.shipping_fee <= 35
),
enriched AS (
    -- Attach orders attributes
    SELECT
        e.*,
        b.amount AS order_amount_2,
        b.order_date,
        b.shipping_fee AS order_shipping_fee
    FROM with_dims e
    JOIN orders b
        ON e.order_id = b.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        order_amount,
        order_status,
        shipping_fee,
        order_date,
        order_shipping_fee,
        RANK() OVER (PARTITION BY order_status ORDER BY order_amount_2 DESC NULLS LAST) AS order_amount_2_rank,
        LEAD(order_amount_2, 1) OVER (ORDER BY amount, id) AS next_order_amount_2,
        LEAD(order_shipping_fee, 1) OVER (PARTITION BY channel ORDER BY amount, id) AS next_order_shipping_fee
    FROM enriched
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', order_date) AS quarter_start,
    DATE_TRUNC('MONTH', order_date) AS activity_month,
    COUNT_IF(order_status = 'RETURNED') AS returned_events,
    COUNT(DISTINCT id) AS distinct_ids,
    MAX(order_amount) AS max_order_amount,
    APPROX_PERCENTILE(order_amount_2_rank, ARRAY(0.25, 0.5, 0.75)) AS order_amount_2_rank_quartiles,
    COUNT(CASE WHEN order_status = 'RETURNED' THEN 1 END) AS returned_count,
    SIZE(COLLECT_SET(order_status)) AS num_order_status,
    RANK() OVER (PARTITION BY DATE_TRUNC('QUARTER', order_date) ORDER BY SIZE(COLLECT_SET(order_status)) DESC) AS num_order_status_rank
FROM metrics
GROUP BY quarter_start, activity_month
HAVING COUNT(DISTINCT id) > 1
