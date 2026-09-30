SELECT QUARTER(last_restock) AS last_restock_quarter, MIN(last_restock) AS min_last_restock
FROM inventory
WHERE quantity IS NULL AND quantity <= 64
GROUP BY QUARTER(last_restock);
