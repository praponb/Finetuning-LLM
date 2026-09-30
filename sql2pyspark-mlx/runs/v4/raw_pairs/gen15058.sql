SELECT DATE_TRUNC('MONTH', last_restock) AS last_restock_month, SUM(quantity) AS total_quantity, DENSE_RANK() OVER (ORDER BY SUM(quantity) DESC) AS rnk
FROM inventory
WHERE product_id = 5
GROUP BY DATE_TRUNC('MONTH', last_restock);
