SELECT
    s.supplier_name,
    p.price,
    p.cost,
    p.supplier_id
FROM suppliers s
LEFT OUTER JOIN products p ON p.supplier_id = s.id;
