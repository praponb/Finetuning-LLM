SELECT *
FROM inventory
WHERE last_restock < '2024-10-24'
ORDER BY last_restock DESC;
