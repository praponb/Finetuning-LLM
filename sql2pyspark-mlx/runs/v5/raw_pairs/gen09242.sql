SELECT product_id, SUM(quantity) AS total_quantity FROM inventory GROUP BY product_id HAVING SUM(quantity) > 1430 ORDER BY product_id;
