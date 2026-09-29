SELECT label, SUM(stock_qty) AS total_stock FROM products LATERAL VIEW EXPLODE(tags) t AS label WHERE stock_qty = 375 GROUP BY label ORDER BY total_stock DESC, label
