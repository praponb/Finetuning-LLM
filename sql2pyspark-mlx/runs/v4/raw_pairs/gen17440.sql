SELECT tag, SUM(stock_qty) AS total_stock FROM products LATERAL VIEW EXPLODE(tags) t AS tag WHERE cost > 600 GROUP BY tag ORDER BY total_stock DESC, tag;
