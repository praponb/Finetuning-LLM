WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        LENGTH(brand) AS brand_length
    FROM products
    WHERE launch_date > DATE '2023-02-26'
),
with_last AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.category,
        s.price,
        s.cost,
        s.brand_length,
        top1.order_item_unit_price
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_last_products AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.order_item_unit_price_2
    FROM with_last s
    JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price_2
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(order_item_unit_price) OVER w_running AS next_order_item_unit_price,
        LAG(cost) OVER w_running AS prev_cost,
        LEAD(cost) OVER w_running AS next_cost,
        ROW_NUMBER() OVER w_running AS row_seq
    FROM with_last_products
    WINDOW w_running AS (PARTITION BY category ORDER BY cost, id)
)
-- Multi-level aggregation
SELECT
    CASE WHEN brand_length >= 66 THEN 'HIGH' ELSE 'LOW' END AS brand_length_tier,
    category,
    COUNT(CASE WHEN category = 'Books' THEN 1 END) AS books_count,
    SIZE(COLLECT_SET(category)) AS num_categories,
    ROUND(SUM(next_cost) / NULLIF(COUNT(CASE WHEN category = 'Books' THEN 1 END), 0), 2) AS next_cost_per_books,
    SIZE(COLLECT_SET(category)) AS num_categories_2
FROM with_windows
GROUP BY GROUPING SETS ((CASE WHEN brand_length >= 66 THEN 'HIGH' ELSE 'LOW' END, category), (CASE WHEN brand_length >= 66 THEN 'HIGH' ELSE 'LOW' END), ())
ORDER BY num_categories DESC NULLS LAST;
