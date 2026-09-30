SELECT brand, SUM(stock_qty) AS total_stock_qty, ROUND(SUM(stock_qty) * 100.0 / SUM(SUM(stock_qty)) OVER (), 2) AS pct_of_total
FROM products
WHERE launch_date BETWEEN '2023-12-01' AND '2025-04-18'
GROUP BY brand
ORDER BY pct_of_total;
