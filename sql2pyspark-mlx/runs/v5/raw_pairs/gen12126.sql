SELECT id, store_id, quantity
FROM inventory
WHERE (quantity <= 88 AND last_restock < '2024-02-07') OR product_id IN (1, 3, 6)
ORDER BY quantity;
