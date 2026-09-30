SELECT product_id, YEAR(last_restock) AS last_restock_year, MIN(last_restock) AS first_last_restock
FROM inventory
WHERE NOT (product_id IN (1, 4, 6) AND store_id = 6 AND store_id IN (1, 4, 6))
GROUP BY product_id, last_restock_year;
