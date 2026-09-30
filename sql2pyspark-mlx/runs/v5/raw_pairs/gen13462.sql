SELECT category, SUM(stock_qty) total_stock_qty, COUNT(*) cnt, SUM(SUM(stock_qty)) OVER (ORDER BY category) AS cumulative_stock_qty FROM products WHERE SIZE(tags) > 2 GROUP BY category;
