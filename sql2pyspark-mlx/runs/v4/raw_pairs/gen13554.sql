SELECT product_id, SUM(quantity) AS total_quantity, DENSE_RANK() OVER (ORDER BY SUM(quantity) DESC) AS quantity_rank FROM inventory GROUP BY product_id;
