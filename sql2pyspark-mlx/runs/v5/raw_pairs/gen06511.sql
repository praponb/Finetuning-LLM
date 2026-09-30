WITH valid AS (
    SELECT
        *,
        INITCAP(TRIM(product_name)) AS product_name_title,
        COALESCE(brand, 'UNKNOWN') AS brand_filled
    FROM products
    WHERE launch_date > DATE '2023-08-25'
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY price DESC NULLS LAST, id) AS rn
    FROM valid
    QUALIFY rn = 1
),
metrics AS (
    SELECT
        id,
        category,
        LEAD(stock_qty, 1) OVER w_grp AS next_stock_qty,
        CUME_DIST() OVER (PARTITION BY supplier_id, category ORDER BY price DESC) AS price_cume_dist
    FROM latest
    WINDOW w_grp AS (PARTITION BY category ORDER BY price, id)
),
kept AS (
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.product_id = s.id
              AND ch.product_id = 17
        )
)
SELECT
    category,
    CASE WHEN next_stock_qty >= 331 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_stock_qty_tier,
    COUNT(*) AS num_rows,
    COUNT(*) FILTER (WHERE category = 'Electronics') AS electronics_rows,
    SUM(next_stock_qty) FILTER (WHERE category = 'Electronics') AS electronics_total_next_stock_qty
FROM kept
GROUP BY GROUPING SETS ((category, CASE WHEN next_stock_qty >= 331 THEN 'PREMIUM' ELSE 'STANDARD' END), (category))
