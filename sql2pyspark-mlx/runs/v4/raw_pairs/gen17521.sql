SELECT
    p.price,
    p.product_name,
    p.launch_date,
    s.country,
    s.rating,
    s.supplier_name
FROM products p
JOIN suppliers s ON p.supplier_id = s.id
WHERE s.country = 'US'
