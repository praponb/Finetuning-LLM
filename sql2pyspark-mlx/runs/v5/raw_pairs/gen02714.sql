WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        category,
        stock_qty,
        tags,
        supplier_id,
        CAST(cost AS INT) AS cost_int,
        COALESCE(brand, 'N/A') AS resolved_brand
    FROM products
    WHERE launch_date >= '2023-04-30'
),
matched_keys AS (
    -- Set difference on keys
    SELECT supplier_id
    FROM cleaned
    EXCEPT ALL
    SELECT supplier_id
    FROM products
    WHERE supplier_id = 16
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN matched_keys k ON s.supplier_id = k.supplier_id
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.product_id = s.id
              AND c.product_id = 27
        )
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.product_id = s.id
              AND ch.units BETWEEN 12 AND 23
        )
),
kept_products AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.last_restock BETWEEN '2024-06-12' AND '2025-09-01'
        )
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(cost_int) OVER (PARTITION BY supplier_id, category) AS supplier_id_min_cost_int,
        AVG(stock_qty) OVER (PARTITION BY category) AS category_avg_stock_qty
    FROM kept_products
)
-- Grouped metrics
SELECT
    category,
    CASE WHEN supplier_id_min_cost_int >= 66 THEN 'HIGH' ELSE 'LOW' END AS supplier_id_min_cost_int_tier,
    ROUND(AVG(cost_int), 2) AS avg_cost_int,
    COUNT(*) FILTER (WHERE category = 'Grocery') AS grocery_rows,
    COUNT(*) FILTER (WHERE category = 'Books') AS books_rows,
    APPROX_PERCENTILE(category_avg_stock_qty, 0.95) AS category_avg_stock_qty_p95,
    SORT_ARRAY(COLLECT_SET(category)) AS category_list,
    AVG(category_avg_stock_qty) AS avg_category_avg_stock_qty
FROM enriched
GROUP BY 1, 2
