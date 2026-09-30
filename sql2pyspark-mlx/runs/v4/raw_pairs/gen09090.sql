SELECT
    CASE WHEN YEAR(launch_date) < 2024 THEN 'before_2024' WHEN YEAR(launch_date) = 2024 THEN '2024' ELSE 'after_2024' END AS launch_cohort,
    COUNT(CASE WHEN category = 'Grocery' THEN 1 END) AS grocery_count,
    SUM(stock_qty) AS total_stock_qty,
    MAX(price) AS max_price
FROM products
GROUP BY 1
ORDER BY grocery_count DESC;
