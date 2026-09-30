SELECT ss.id, ss.revenue
FROM store_sales ss
WHERE ss.product_id NOT IN (SELECT id FROM products WHERE stock_qty BETWEEN 232 AND 366)
ORDER BY ss.revenue DESC;
