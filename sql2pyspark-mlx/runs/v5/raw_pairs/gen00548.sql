SELECT
    i.quantity,
    i.last_restock,
    p.brand,
    s.country,
    s.rating
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id;
