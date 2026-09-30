SELECT
    i.id,
    i.last_restock,
    i.store_id,
    s.store_name,
    s.city,
    p.category,
    p.stock_qty
FROM inventory i
INNER JOIN stores s ON i.store_id = s.id
RIGHT JOIN products p ON i.product_id = p.id
WHERE p.price IS NOT NULL;
