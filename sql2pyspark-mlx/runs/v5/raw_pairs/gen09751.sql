SELECT QUARTER(launch_date) AS launch_quarter, category, MIN(stock_qty) AS min_stock_qty FROM products GROUP BY QUARTER(launch_date), category ORDER BY launch_quarter DESC, category LIMIT 3
