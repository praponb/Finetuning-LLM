SELECT
    i.id,
    i.quantity,
    ss.product_id,
    ss.revenue
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.last_restock < '2023-10-01' AND i.store_id IN (1, 6)
