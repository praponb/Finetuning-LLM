SELECT s.rating, p.brand
FROM suppliers s
LEFT OUTER JOIN products p ON p.supplier_id = s.id
