SELECT last_restock, product_id
FROM inventory
WHERE quantity < 170
EXCEPT
SELECT last_restock, product_id
FROM inventory
WHERE last_restock BETWEEN '2024-04-02' AND '2025-09-07'
