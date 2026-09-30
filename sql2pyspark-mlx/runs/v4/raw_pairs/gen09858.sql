SELECT p.brand, COUNT(DISTINCT ss.revenue) AS unique_products, COUNT(*) AS num_store_sales
FROM store_sales ss
INNER JOIN products p ON p.id = ss.product_id
WHERE p.price < 400
GROUP BY p.brand
ORDER BY num_store_sales DESC;
