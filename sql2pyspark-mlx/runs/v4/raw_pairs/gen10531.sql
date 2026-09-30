WITH eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM orders s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.order_id = s.id
              AND c.product_id IN (15, 16)
        )
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(discount) OVER (PARTITION BY channel ORDER BY discount, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS discount_avg_2_prior,
        ROUND(amount / SUM(amount) OVER (PARTITION BY customer_id), 4) AS amount_share,
        CUME_DIST() OVER (PARTITION BY customer_id ORDER BY shipping_fee DESC NULLS LAST) AS shipping_fee_cume_dist,
        COUNT(discount) OVER (PARTITION BY store_id) AS store_id_count_discount,
        LAG(amount, 2) OVER (PARTITION BY channel, store_id ORDER BY amount, id) AS prev_amount
    FROM eligible
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM payments c
            WHERE c.order_id = s.id
              AND c.order_id IS NULL
        )
),
with_last AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.order_item_unit_price
    FROM not_flagged s
    LEFT JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(amount, TRUE) OVER (PARTITION BY store_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount,
        LEAD(shipping_fee_cume_dist, 1) OVER (ORDER BY discount, id) AS next_shipping_fee_cume_dist,
        MAX(prev_amount) OVER (PARTITION BY customer_id) AS customer_id_max_prev_amount
    FROM with_last
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        channel,
        customer_id,
        status,
        prev_amount
    FROM with_windows
) src
PIVOT (
    SUM(prev_amount) AS total
    FOR status IN ('COMPLETED', 'RETURNED', 'SHIPPED')
)
