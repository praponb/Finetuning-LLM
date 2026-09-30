SELECT
    i.id,
    i.last_restock,
    i.quantity,
    i.product_id,
    p.product_name,
    p.brand
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
