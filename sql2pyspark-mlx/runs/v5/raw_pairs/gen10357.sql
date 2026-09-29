SELECT DATE_TRUNC('MONTH', last_restock) AS last_restock_month, MAX(last_restock) AS latest_last_restock, COUNT(DISTINCT quantity) AS unique_products
FROM inventory
WHERE last_restock <= '2024-04-01' AND store_id IN (1, 3, 5, 6)
GROUP BY DATE_TRUNC('MONTH', last_restock)
HAVING COUNT(DISTINCT quantity) >= 2
ORDER BY last_restock_month DESC;
