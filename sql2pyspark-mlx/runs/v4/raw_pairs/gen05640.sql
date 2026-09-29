SELECT YEAR(launch_date) AS launch_year, CASE WHEN stock_qty >= 400 THEN 'high' WHEN stock_qty >= 87 THEN 'mid' ELSE 'low' END AS stock_qty_bucket, SUM(stock_qty) AS total_stock_qty
FROM products
GROUP BY 1, 2;
