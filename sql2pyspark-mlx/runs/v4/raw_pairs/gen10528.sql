WITH cleaned AS (
    SELECT
        *,
        cost - stock_qty cost_minus_stock_qty,
        MONTH(launch_date) launch_date_month,
        COALESCE(brand, 'N/A') resolved_brand
    FROM products
    WHERE category IN ('Fashion', 'Electronics', 'Grocery')
)
SELECT
    *,
    LAG(cost_minus_stock_qty, 1) OVER (PARTITION BY category ORDER BY stock_qty, id) prev_cost_minus_stock_qty,
    LAG(launch_date_month, 1) OVER (PARTITION BY category ORDER BY price, id) prev_launch_date_month
FROM cleaned
