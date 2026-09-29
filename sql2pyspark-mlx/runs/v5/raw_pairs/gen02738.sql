WITH filtered AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        cost,
        stock_qty,
        launch_date,
        supplier_id,
        LAST_DAY(launch_date) AS launch_date_month_end,
        LOWER(product_name) AS product_name_lower,
        COALESCE(brand, 'OTHER') AS brand_clean
    FROM products
    WHERE supplier_id IS NOT NULL
)
SELECT
    DATE_TRUNC('WEEK', launch_date) AS week_start,
    DATE_TRUNC('YEAR', launch_date_month_end) AS year_start,
    CASE WHEN stock_qty >= 377 THEN 'PREMIUM' ELSE 'STANDARD' END AS stock_qty_tier,
    PERCENTILE_APPROX(cost, 0.5) AS cost_p50,
    APPROX_PERCENTILE(stock_qty, 0.5) AS stock_qty_p50,
    COUNT(*) FILTER (WHERE category = 'Electronics') AS electronics_rows,
    AVG(stock_qty) FILTER (WHERE category = 'Grocery') AS grocery_avg_stock_qty,
    APPROX_PERCENTILE(stock_qty, 0.5) AS stock_qty_p50_2
FROM filtered
GROUP BY ROLLUP (DATE_TRUNC('WEEK', launch_date), DATE_TRUNC('YEAR', launch_date_month_end), CASE WHEN stock_qty >= 377 THEN 'PREMIUM' ELSE 'STANDARD' END)
