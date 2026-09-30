SELECT
    s.city,
    s.supplier_name,
    p.supplier_id,
    p.product_name
FROM suppliers s
LEFT JOIN products p ON p.supplier_id = s.id
ORDER BY supplier_id;
