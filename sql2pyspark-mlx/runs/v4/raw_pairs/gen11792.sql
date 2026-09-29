SELECT
    p.id,
    p.category,
    COUNT(ss.id) AS store_sale_count,
    COUNT(DISTINCT ss.units) AS unique_stores
FROM products p
LEFT JOIN store_sales ss ON p.id = ss.product_id
GROUP BY p.id, p.category
HAVING COUNT(DISTINCT ss.units) > 2;
