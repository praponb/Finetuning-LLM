WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        category,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id
    FROM products
    QUALIFY ROW_NUMBER() OVER (PARTITION BY category, supplier_id ORDER BY cost DESC NULLS FIRST, id) = 1
),
joined AS (
    -- Attach suppliers attributes
    SELECT
        src.*,
        s.city,
        s.supplier_name
    FROM latest src
    LEFT JOIN suppliers s
        ON src.supplier_id = s.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        category,
        MIN(stock_qty) OVER (PARTITION BY supplier_id, city ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_stock_qty,
        COUNT(stock_qty) OVER (PARTITION BY supplier_id, city ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_stock_qty,
        AVG(stock_qty) OVER (PARTITION BY supplier_id, city ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_stock_qty
    FROM joined
),
with_last AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.units
    FROM metrics s
    JOIN LATERAL (
            SELECT c.units
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Grouped metrics
SELECT
    CASE WHEN running_count_stock_qty >= 13 THEN 'LARGE' ELSE 'SMALL' END AS running_count_stock_qty_tier,
    MIN(running_min_stock_qty) AS min_running_min_stock_qty,
    COUNT(*) FILTER (WHERE category = 'Sports') AS sports_rows,
    ROUND(AVG(units), 2) AS avg_units,
    SUM(running_count_stock_qty) FILTER (WHERE category = 'Grocery') AS grocery_total_running_count_stock_qty,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS event_count
FROM with_last
GROUP BY CASE WHEN running_count_stock_qty >= 13 THEN 'LARGE' ELSE 'SMALL' END
HAVING COUNT(*) FILTER (WHERE category = 'Sports') > 1
