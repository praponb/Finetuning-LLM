WITH InventoryByProduct AS (
    SELECT product_id, MAX(last_restock) last_last_restock
    FROM inventory
    WHERE last_restock >= '2024-05-14'
    GROUP BY product_id
)
SELECT p.id, p.launch_date, I.last_last_restock
FROM products p
INNER JOIN InventoryByProduct I ON p.id = I.product_id;
