SELECT product_id, unique_products FROM (SELECT product_id, COUNT(DISTINCT quantity) AS unique_products FROM inventory GROUP BY product_id) AS x WHERE unique_products > 9 ORDER BY 2 ASC;
