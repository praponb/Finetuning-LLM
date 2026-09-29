SELECT p.id, p.category, COUNT(ss.id) AS store_sale_count
FROM products p
LEFT JOIN store_sales ss ON ss.product_id = p.id
WHERE p.stock_qty BETWEEN 176 AND 213
GROUP BY p.id, p.category;
