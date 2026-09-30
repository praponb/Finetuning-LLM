SELECT p.category, COUNT(ss.id) AS store_sale_count
FROM products p
LEFT OUTER JOIN store_sales ss ON p.id = ss.product_id
WHERE ss.store_id IN (5, 6)
GROUP BY p.category
ORDER BY p.category;
