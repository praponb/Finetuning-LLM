SELECT *
FROM inventory
WHERE quantity BETWEEN 82 AND 85 AND last_restock < '2025-07-31'
ORDER BY last_restock ASC, 2 DESC;
