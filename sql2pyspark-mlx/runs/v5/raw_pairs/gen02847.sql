WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, GREATEST(unit_price, quantity) AS greatest_unit_price_quantity
    FROM order_items
    WHERE quantity < 3
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT id, order_id
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price, id) AS rn
        FROM filtered
    ) x
    WHERE rn = 1
),
enriched AS (
    -- Attach orders attributes
    SELECT
        s.*,
        p.discount,
        p.shipping_fee,
        p.channel
    FROM ranked s
    LEFT JOIN orders p
        ON s.order_id = p.id
    WHERE p.store_id IS NULL
),
ranked_order_items AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY shipping_fee DESC NULLS LAST, id) AS rn
    FROM enriched
    QUALIFY rn = 1
),
enriched_order_items AS (
    -- Attach orders attributes
    SELECT
        e.*,
        p.channel AS order_channel,
        p.shipping_fee AS order_shipping_fee,
        p.amount
    FROM ranked_order_items e
    LEFT JOIN orders p
        ON e.order_id = p.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        amount,
        SUM(discount) OVER (PARTITION BY order_id, channel ORDER BY discount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_discount,
        COUNT(discount) OVER (PARTITION BY order_id, channel ORDER BY discount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_discount,
        MIN(discount) OVER (PARTITION BY order_id, channel ORDER BY discount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_discount
    FROM enriched_order_items
)
-- Multi-level aggregation
SELECT
    CASE WHEN amount >= 1430 THEN 'HIGH' ELSE 'LOW' END AS amount_tier,
    MIN(last4_min_discount) AS min_last4_min_discount,
    COUNT(*) AS row_count,
    MAX(last4_sum_discount) AS max_last4_sum_discount,
    COUNT(*) AS cnt,
    PERCENTILE_APPROX(last4_min_discount, 0.9) AS last4_min_discount_p90,
    APPROX_PERCENTILE(last4_count_discount, 0.95) AS last4_count_discount_p95
FROM scored
GROUP BY CASE WHEN amount >= 1430 THEN 'HIGH' ELSE 'LOW' END
HAVING last4_count_discount_p95 < 1000000
ORDER BY cnt NULLS FIRST
