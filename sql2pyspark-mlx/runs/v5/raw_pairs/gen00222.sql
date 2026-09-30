SELECT YEAR(last_restock) AS last_restock_year, SUM(quantity) AS total_quantity, RANK() OVER (ORDER BY SUM(quantity) DESC) AS quantity_rank
FROM inventory
WHERE store_id IN (4, 5)
GROUP BY YEAR(last_restock)
ORDER BY last_restock_year DESC
LIMIT 10;
