WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        SUBSTRING(category, 1, 4) AS category_prefix,
        SIZE(tags) AS tags_count
    FROM products
    WHERE brand IS NOT NULL
      AND YEAR(launch_date) = 2025
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        category_prefix,
        tags_count,
        FIRST_VALUE(tags_count, TRUE) OVER (PARTITION BY category ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_tags_count,
        SUM(price) OVER (PARTITION BY category, supplier_id) AS category_sum_price
    FROM filtered
),
with_last AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.unit_price,
        top1.order_item_quantity
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.unit_price, c.quantity AS order_item_quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        category_prefix,
        first_known_tags_count,
        category_sum_price,
        unit_price,
        first_known_tags_count + tags_count AS first_known_tags_count_plus_tags_count,
        cost / NULLIF(stock_qty, 0) AS cost_per_stock_qty
    FROM with_last
    WHERE launch_date > DATE '2023-08-05'
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.product_id = s.id
              AND x.sale_date < s.launch_date
        )
)
-- Grouped metrics
SELECT
    CASE WHEN unit_price >= 420 THEN 'HIGH' ELSE 'LOW' END AS unit_price_tier,
    SIZE(COLLECT_SET(brand)) AS num_brands,
    COUNT(DISTINCT brand) AS distinct_brands,
    ROUND(AVG(stock_qty), 2) AS avg_stock_qty,
    GROUPING(CASE WHEN unit_price >= 420 THEN 'HIGH' ELSE 'LOW' END) AS grouping_id
FROM not_flagged
GROUP BY ROLLUP (CASE WHEN unit_price >= 420 THEN 'HIGH' ELSE 'LOW' END)
HAVING num_brands < 1
ORDER BY distinct_brands DESC;
