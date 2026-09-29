SELECT
    s.supplier_name,
    p.brand,
    p.cost,
    p.price
FROM suppliers s
LEFT OUTER JOIN products p ON p.supplier_id = s.id
ORDER BY 2
