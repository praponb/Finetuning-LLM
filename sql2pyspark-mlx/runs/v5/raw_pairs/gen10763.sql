WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        country,
        city,
        rating,
        TRIM(supplier_name) AS clean_supplier_name,
        CONCAT(supplier_name, ' - ', city) AS supplier_name_city,
        COALESCE(city, 'N/A') AS city_clean
    FROM suppliers
    WHERE rating >= 3.4
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.product_category,
        latest.product_stock_qty
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.category AS product_category, c.stock_qty AS product_stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.category NOT IN ('Home', 'Sports')
        )
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        country,
        rating,
        clean_supplier_name,
        city_clean,
        product_category,
        product_stock_qty,
        MIN(product_stock_qty) OVER (PARTITION BY product_category, country ORDER BY product_stock_qty, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_product_stock_qty,
        MAX(product_stock_qty) OVER (PARTITION BY product_category, country ORDER BY product_stock_qty, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_product_stock_qty,
        COUNT(product_stock_qty) OVER (PARTITION BY product_category, country ORDER BY product_stock_qty, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_product_stock_qty,
        SUM(product_stock_qty) OVER (ORDER BY product_stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_product_stock_qty
    FROM kept
)
-- Grouped metrics
SELECT
    CASE WHEN last3_max_product_stock_qty >= 410 THEN 'HIGH' ELSE 'LOW' END AS last3_max_product_stock_qty_tier,
    SIZE(COLLECT_SET(country)) AS num_countries,
    SORT_ARRAY(COLLECT_SET(product_category)) AS product_category_list,
    MAX(rating) FILTER (WHERE product_category = 'Grocery') AS grocery_max_rating,
    COUNT(*) FILTER (WHERE product_category = 'Sports') AS sports_rows,
    ROUND(SUM(rating) / NULLIF(COUNT(CASE WHEN product_category = 'Electronics' THEN 1 END), 0), 2) AS rating_per_electronics,
    SIZE(COLLECT_SET(country)) AS num_countries_2
FROM metrics
GROUP BY 1
ORDER BY num_countries DESC NULLS LAST
