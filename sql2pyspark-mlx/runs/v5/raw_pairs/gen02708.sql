SELECT p.brand, COUNT(ss.id) store_sale_count, COUNT(DISTINCT ss.revenue) unique_products
FROM products p
LEFT JOIN store_sales ss ON p.id = ss.product_id
GROUP BY p.brand
