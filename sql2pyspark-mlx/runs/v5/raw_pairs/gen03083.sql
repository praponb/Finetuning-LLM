WITH cleaned AS (
    SELECT *, CASE WHEN status = 'LOST' THEN 1 ELSE 0 END AS is_lost
    FROM shipments
    WHERE shipped_date >= DATE '2023-04-09'
      AND order_id IS NULL
),
enriched AS (
    SELECT
        id,
        order_id,
        shipped_date,
        status,
        SUM(is_lost) OVER (PARTITION BY order_id ORDER BY is_lost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_is_lost,
        COUNT(is_lost) OVER (PARTITION BY order_id ORDER BY is_lost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_is_lost,
        MAX(is_lost) OVER (PARTITION BY order_id ORDER BY is_lost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_is_lost,
        SUM(is_lost) OVER (PARTITION BY order_id ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_is_lost
    FROM cleaned
),
enriched_shipments AS (
    SELECT
        s.id,
        s.order_id,
        s.shipped_date,
        s.status,
        s.cumulative_is_lost,
        p.shipping_fee
    FROM enriched s
    LEFT JOIN orders p
        ON s.order_id <=> p.id
),
joined AS (
    SELECT
        src.id,
        src.order_id,
        src.status,
        src.shipping_fee,
        o.discount
    FROM enriched_shipments src
    LEFT JOIN orders o
        ON src.order_id = o.id
),
with_dims AS (
    SELECT
        src.*,
        p.amount,
        p.discount AS order_discount
    FROM joined src
    LEFT JOIN orders p
        ON src.order_id = p.id
),
grouped AS (
    SELECT
        order_id,
        CASE WHEN shipping_fee >= 34 THEN 'PREMIUM' ELSE 'STANDARD' END AS shipping_fee_tier,
        SIZE(COLLECT_SET(status)) AS num_status,
        APPROX_PERCENTILE(discount, 0.5) AS discount_p50,
        COUNT_IF(status = 'LOST') AS lost_events
    FROM with_dims
    GROUP BY order_id, shipping_fee_tier
    HAVING SIZE(COLLECT_SET(status)) < 1000
)
SELECT
    s.order_id,
    s.shipping_fee_tier,
    s.num_status,
    (SELECT MIN(x.amount) FROM payments x WHERE x.order_id = s.order_id) AS min_payment_amount
FROM grouped s;
