SELECT
    ss.sale_date,
    p.launch_date,
    p.brand,
    s.rating,
    s.supplier_name,
    LOWER(s.supplier_name) AS supplier_name_lower
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
INNER JOIN suppliers s ON s.id = p.supplier_id
WHERE p.brand = 'Acme';
