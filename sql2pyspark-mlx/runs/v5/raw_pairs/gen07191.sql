SELECT MONTH(last_restock) AS last_restock_month, MIN(last_restock) AS first_last_restock FROM inventory WHERE product_id IN (2, 6, 7, 9) GROUP BY MONTH(last_restock);
