SELECT
    i.id,
    i.store_id,
    i.quantity,
    i.last_restock
FROM inventory i
WHERE i.store_id IN (SELECT id FROM stores WHERE opened_date BETWEEN '2023-08-25' AND '2024-03-01')
ORDER BY i.store_id DESC;
