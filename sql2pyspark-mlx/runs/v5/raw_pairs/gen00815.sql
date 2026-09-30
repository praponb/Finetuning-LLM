SELECT
    s.country,
    s.supplier_name,
    s.rating,
    p.product_name
FROM suppliers s
LEFT OUTER JOIN products p ON p.supplier_id = s.id;
