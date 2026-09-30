WITH latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        product_name,
        category,
        price,
        stock_qty,
        launch_date
    FROM products
    QUALIFY ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY stock_qty DESC NULLS FIRST, id) = 1
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.inventory_quantity
    FROM latest s
    JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.last_restock <= s.launch_date
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LOWER(product_name) AS product_name_lower
    FROM with_latest
    WHERE stock_qty IS NOT NULL
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        product_name,
        category,
        stock_qty,
        launch_date,
        inventory_quantity
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC, id) = 1
),
ranked_products AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM ranked
    QUALIFY RANK() OVER (PARTITION BY category ORDER BY launch_date DESC) = 1
),
with_latest_products AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.product_name,
        s.category,
        s.stock_qty,
        latest.store_sale_units,
        latest.sale_date
    FROM ranked_products s
    LEFT JOIN LATERAL (
            SELECT c.units AS store_sale_units, c.sale_date
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('WEEK', sale_date) AS week_start,
    category,
    CASE WHEN store_sale_units >= 35 THEN 'PREMIUM' ELSE 'STANDARD' END AS store_sale_units_tier,
    SORT_ARRAY(COLLECT_SET(category)) AS category_list,
    SUM(stock_qty) AS total_stock_qty,
    COUNT(*) AS event_count
FROM with_latest_products
GROUP BY DATE_TRUNC('WEEK', sale_date), category, CASE WHEN store_sale_units >= 35 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING total_stock_qty >= 0
ORDER BY week_start DESC;
