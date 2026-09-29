SELECT
    s.country,
    s.city,
    p.launch_date,
    p.product_name,
    p.brand,
    ARRAY_CONTAINS(p.tags, 'sale') AS is_sale
FROM suppliers s
LEFT OUTER JOIN products p ON p.supplier_id = s.id;
