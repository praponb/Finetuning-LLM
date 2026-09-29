SELECT store_id, quantity, DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock
FROM inventory
WHERE last_restock <= '2023-07-01' AND YEAR(last_restock) = 2025;
