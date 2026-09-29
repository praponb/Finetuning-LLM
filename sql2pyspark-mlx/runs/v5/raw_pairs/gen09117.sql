SELECT product_id, COUNT(DISTINCT quantity) unique_products, COUNT(quantity) quantity_count FROM inventory GROUP BY product_id HAVING COUNT(DISTINCT quantity) < 2
