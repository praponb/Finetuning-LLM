SELECT
    category,
    SUM(stock_qty) AS total_stock_qty,
    COUNT(*) AS cnt,
    ROUND(SUM(stock_qty) * 100.0 / SUM(SUM(stock_qty)) OVER (), 2) AS pct_of_total
FROM products
WHERE YEAR(launch_date) >= 2024
GROUP BY category
