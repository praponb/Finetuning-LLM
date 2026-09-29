SELECT label, SUM(stock_qty) AS total_stock FROM products LATERAL VIEW EXPLODE(tags) t AS label WHERE supplier_id IN (11, 24, 32, 38) GROUP BY label;
