SELECT
    MONTH(launch_date) AS launch_month,
    SUM(CASE WHEN category = 'Books' THEN stock_qty ELSE 0 END) AS books_stock_qty,
    MAX(launch_date) AS last_launch_date,
    SUM(price) AS total_price
FROM products
GROUP BY MONTH(launch_date)
HAVING SUM(price) > 5300
ORDER BY launch_month;
