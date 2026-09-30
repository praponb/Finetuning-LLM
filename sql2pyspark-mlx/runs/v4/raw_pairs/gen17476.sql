SELECT
    p.id,
    p.price,
    s.rating,
    s.country
FROM products p
JOIN suppliers s ON s.id = p.supplier_id
