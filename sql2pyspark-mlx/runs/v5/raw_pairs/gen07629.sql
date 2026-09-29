SELECT YEAR(last_restock) AS last_restock_year, COUNT(quantity) AS quantity_count, MAX(quantity) AS max_quantity FROM inventory WHERE quantity BETWEEN 144 AND 211 GROUP BY 1 HAVING max_quantity > 90;
