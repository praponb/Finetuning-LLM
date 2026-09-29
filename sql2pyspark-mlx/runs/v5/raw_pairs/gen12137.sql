WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_name,
        category,
        price,
        cost,
        stock_qty,
        tags,
        supplier_id,
        REPLACE(product_name, ' ', '_') AS product_name_slug,
        UPPER(product_name) AS product_name_upper
    FROM products
    WHERE launch_date > DATE '2023-05-03'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(cost) OVER (PARTITION BY category ORDER BY stock_qty, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_cost,
        COUNT(cost) OVER (PARTITION BY category ORDER BY stock_qty, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_cost,
        MAX(cost) OVER (PARTITION BY category ORDER BY stock_qty, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_cost
    FROM filtered
    WHERE stock_qty > 207
),
with_latest AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.stock_qty,
        s.supplier_id,
        s.last4_sum_cost,
        s.last4_count_cost,
        top1.quantity
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        stock_qty,
        last4_sum_cost,
        last4_count_cost,
        quantity,
        ROUND(last4_sum_cost * 1.1, 2) AS last4_sum_cost_scaled
    FROM with_latest
    WHERE supplier_id = 8
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.product_id = s.id
              AND ch.quantity != 6
        )
)
SELECT
    id,
    stock_qty,
    last4_sum_cost,
    last4_count_cost
FROM eligible
ORDER BY last4_sum_cost NULLS FIRST
