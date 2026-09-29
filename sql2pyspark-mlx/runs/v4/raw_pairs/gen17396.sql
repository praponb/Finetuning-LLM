SELECT
    p.id,
    p.cost,
    s.supplier_name,
    s.city
FROM products p
LEFT OUTER JOIN suppliers s ON p.supplier_id = s.id
