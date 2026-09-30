SELECT
    s.city,
    s.country,
    s.rating,
    p.product_name
FROM suppliers s
LEFT OUTER JOIN products p ON s.id = p.supplier_id
WHERE s.rating >= 3.3
ORDER BY city;
