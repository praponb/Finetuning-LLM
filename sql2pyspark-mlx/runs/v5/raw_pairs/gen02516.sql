SELECT
    s.id,
    s.supplier_name,
    s.country,
    s.rating
FROM suppliers s
WHERE s.id IN (SELECT supplier_id FROM products WHERE brand = 'Initech');
