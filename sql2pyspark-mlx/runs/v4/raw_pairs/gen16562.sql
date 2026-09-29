SELECT
    i.id,
    i.store_id,
    i.quantity,
    i.last_restock,
    p.cost
FROM inventory i
JOIN products p ON i.product_id = p.id
WHERE LENGTH(p.product_name) >= 9 AND p.price >= 1120
ORDER BY quantity
