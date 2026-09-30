SELECT
    s.city,
    p.brand,
    p.price,
    p.supplier_id
FROM suppliers s
LEFT JOIN products p ON p.supplier_id = s.id
WHERE p.price >= 1180;
