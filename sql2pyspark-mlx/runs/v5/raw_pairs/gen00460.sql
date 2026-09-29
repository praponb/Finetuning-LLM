WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        CORR(shipping_fee, amount) OVER (PARTITION BY channel) AS corr_shipping_fee_amount,
        AVG(discount) OVER (ORDER BY discount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS discount_avg_3
    FROM orders
    WHERE store_id IN (19, 36)
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(discount_avg_3) OVER (PARTITION BY customer_id) AS customer_id_avg_discount_avg_3,
        CORR(shipping_fee, discount) OVER (PARTITION BY store_id) AS corr_shipping_fee_discount
    FROM with_windows
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.order_id = s.id
              AND c.product_id IS NOT NULL
        )
),
latest_match AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.order_date,
        s.channel,
        s.discount,
        s.discount_avg_3,
        s.customer_id_avg_discount_avg_3,
        x.payment_amount
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.amount AS payment_amount
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Join a dimension and aggregate
SELECT
    c.country,
    src.channel,
    COUNT(*) AS cnt,
    MIN(src.discount_avg_3) AS min_discount_avg_3,
    SUM(src.customer_id_avg_discount_avg_3) AS total_customer_id_avg_discount_avg_3
FROM latest_match src
INNER JOIN customers c ON src.customer_id = c.id
GROUP BY ROLLUP (c.country, src.channel)
