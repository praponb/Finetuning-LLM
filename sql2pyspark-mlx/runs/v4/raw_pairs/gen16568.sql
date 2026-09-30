SELECT id, store_id, quantity
FROM (
    SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY quantity DESC) rn
    FROM inventory
    WHERE last_restock > '2023-03-01'
) r
WHERE rn = 1
ORDER BY id DESC
