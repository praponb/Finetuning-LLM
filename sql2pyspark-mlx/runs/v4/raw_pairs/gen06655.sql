SELECT YEAR(last_restock) AS last_restock_year, SUM(quantity) AS total_quantity, COUNT(quantity) AS quantity_count, ROUND(AVG(quantity), 2) AS avg_quantity FROM inventory GROUP BY YEAR(last_restock)
