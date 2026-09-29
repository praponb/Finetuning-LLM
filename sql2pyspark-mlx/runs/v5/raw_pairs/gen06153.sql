SELECT
    i.id,
    i.store_id,
    i.last_restock,
    s.opened_date,
    s.city,
    p.price
FROM inventory i
INNER JOIN stores s ON i.store_id = s.id
INNER JOIN products p ON i.product_id = p.id
ORDER BY opened_date DESC, id
LIMIT 1;
