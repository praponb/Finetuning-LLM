SELECT * FROM inventory WHERE product_id IN (2, 4, 8, 10) AND store_id IN (2, 4, 5) AND YEAR(last_restock) = 2024
