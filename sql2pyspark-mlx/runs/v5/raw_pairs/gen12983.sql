SELECT
    p.cost,
    p.brand,
    p.launch_date,
    s.rating
FROM products p
LEFT JOIN suppliers s ON p.supplier_id = s.id
WHERE s.city = 'New York' AND s.country = 'TH';
