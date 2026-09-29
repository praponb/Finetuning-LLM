SELECT
    ss.id,
    ss.product_id,
    p.category,
    s.country,
    s.supplier_name
FROM store_sales ss
JOIN products p ON ss.product_id = p.id
JOIN suppliers s ON p.supplier_id = s.id
WHERE s.country IN ('JP', 'UK');
