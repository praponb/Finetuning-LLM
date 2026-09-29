SELECT product_id, id
FROM inventory
EXCEPT
SELECT product_id, id
FROM inventory
WHERE last_restock < '2024-03-02'
