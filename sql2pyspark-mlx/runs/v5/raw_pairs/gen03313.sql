SELECT
    i.id,
    i.last_restock,
    i.store_id,
    p.supplier_id,
    s.rating,
    s.city
FROM inventory i
FULL OUTER JOIN products p ON p.id = i.product_id
INNER JOIN suppliers s ON p.supplier_id = s.id;
