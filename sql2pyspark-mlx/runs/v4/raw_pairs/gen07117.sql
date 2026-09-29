SELECT
    i.id,
    i.quantity,
    i.last_restock,
    p.brand,
    s.city,
    s.country
FROM inventory i
JOIN products p ON i.product_id = p.id
JOIN suppliers s ON p.supplier_id = s.id
WHERE p.stock_qty BETWEEN 158 AND 282 AND s.rating BETWEEN 3.0 AND 3.2;
