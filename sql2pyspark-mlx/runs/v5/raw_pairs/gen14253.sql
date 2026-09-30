SELECT s.city, COUNT(ss.id) AS store_sale_count, COUNT(DISTINCT ss.revenue) AS unique_products
FROM stores s
LEFT OUTER JOIN store_sales ss ON ss.store_id = s.id
GROUP BY s.city
ORDER BY unique_products
