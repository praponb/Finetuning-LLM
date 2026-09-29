SELECT label, SUM(stock_qty) AS total_stock FROM products LATERAL VIEW EXPLODE(tags) t AS label WHERE stock_qty <= 206 GROUP BY label
