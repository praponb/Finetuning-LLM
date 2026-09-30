SELECT
    p.cost,
    s.rating,
    s.city,
    s.country
FROM products p
JOIN suppliers s ON p.supplier_id = s.id
WHERE s.country IN ('TH', 'SG')
ORDER BY rating DESC;
