WITH with_windows AS (
    SELECT
        *,
        COUNT(discount) OVER (PARTITION BY channel, customer_id ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_discount,
        MAX(discount) OVER (PARTITION BY channel, customer_id ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_discount,
        MIN(discount) OVER (PARTITION BY channel, customer_id ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_discount,
        AVG(discount) OVER (PARTITION BY channel, customer_id ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_discount,
        FIRST_VALUE(discount) IGNORE NULLS OVER (PARTITION BY channel, store_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_discount
    FROM orders
),
with_windows_orders AS (
    SELECT
        id,
        status,
        discount,
        shipping_fee,
        running_max_discount,
        running_min_discount,
        running_avg_discount,
        LAG(running_count_discount, 2) OVER (ORDER BY running_avg_discount, id) AS prev_running_count_discount,
        CORR(discount, shipping_fee) OVER (PARTITION BY customer_id) AS corr_discount_shipping_fee,
        AVG(amount) OVER w_ordered AS amount_avg_6
    FROM with_windows
    WINDOW w_ordered AS (PARTITION BY status ORDER BY running_min_discount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
with_last AS (
    SELECT
        s.id,
        s.prev_running_count_discount,
        top1.order_item_unit_price
    FROM with_windows_orders s
    LEFT JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
not_flagged AS (
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments c
            WHERE c.order_id = s.id
              AND c.order_id IN (9, 32)
        )
),
filtered AS (
    SELECT
        *,
        ROUND(order_item_unit_price * 1.1, 2) AS order_item_unit_price_adjusted,
        FLOOR(order_item_unit_price) AS order_item_unit_price_whole
    FROM not_flagged
    WHERE order_item_unit_price <= 180
)
SELECT
    CASE WHEN order_item_unit_price_whole >= 72 THEN 'HIGH' ELSE 'LOW' END AS order_item_unit_price_whole_tier,
    COUNT(*) AS row_count,
    APPROX_PERCENTILE(order_item_unit_price, ARRAY(0.25, 0.5, 0.75)) AS order_item_unit_price_quartiles
FROM filtered
GROUP BY CASE WHEN order_item_unit_price_whole >= 72 THEN 'HIGH' ELSE 'LOW' END
ORDER BY order_item_unit_price_whole_tier DESC NULLS LAST;
