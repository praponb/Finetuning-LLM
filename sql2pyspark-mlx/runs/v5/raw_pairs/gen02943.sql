SELECT
    s.city,
    s.country,
    s.rating,
    p.supplier_id
FROM suppliers s
LEFT OUTER JOIN products p ON p.supplier_id = s.id
ORDER BY s.country DESC
