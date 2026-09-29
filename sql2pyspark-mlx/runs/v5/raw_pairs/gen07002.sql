SELECT product_id, YEAR(last_restock) AS last_restock_year, AVG(quantity) AS avg_quantity FROM inventory WHERE store_id IN (2, 4, 6) GROUP BY product_id, YEAR(last_restock) ORDER BY product_id
