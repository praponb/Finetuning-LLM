WITH eligible AS (
    SELECT s.*
    FROM products s
    WHERE EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.product_id = s.id
              AND c.unit_price > 170
        )
),
kept AS (
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.last_restock >= s.launch_date
        )
),
with_windows AS (
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
        SUM(price) OVER w_running AS cumulative_price,
        SUM(stock_qty) OVER (PARTITION BY brand) AS brand_sum_stock_qty,
        SUM(stock_qty) OVER (PARTITION BY supplier_id, brand ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_stock_qty
    FROM kept
    WINDOW w_running AS (PARTITION BY supplier_id ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
stats AS (
    SELECT
        DATE_TRUNC('MONTH', launch_date) AS activity_month,
        category,
        SUM(cumulative_price) FILTER (WHERE brand = 'Globex') AS globex_total_cumulative_price,
        ROUND(AVG(stock_qty), 2) AS avg_stock_qty,
        SORT_ARRAY(COLLECT_SET(category)) AS category_list,
        BIT_AND(stock_qty) AS stock_qty_bit_and,
        PERCENTILE_APPROX(stock_qty, 0.95) AS stock_qty_p95,
        COUNT(*) AS row_count,
        GROUPING(DATE_TRUNC('MONTH', launch_date)) AS grouping_level
    FROM with_windows
    GROUP BY ROLLUP (DATE_TRUNC('MONTH', launch_date), category)
)
SELECT
    activity_month,
    category,
    globex_total_cumulative_price,
    category_list,
    stock_qty_bit_and,
    stock_qty_p95,
    row_count,
    grouping_level
FROM stats;
