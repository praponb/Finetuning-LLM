SELECT s.region, COUNT(DISTINCT ss.revenue) AS unique_products FROM store_sales ss RIGHT OUTER JOIN stores s ON s.id = ss.store_id GROUP BY s.region;
