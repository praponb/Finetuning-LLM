SELECT
    i.id,
    i.quantity,
    i.last_restock,
    i.store_id,
    p.brand
FROM inventory i
LEFT JOIN products p ON p.id = i.product_id
ORDER BY brand;
