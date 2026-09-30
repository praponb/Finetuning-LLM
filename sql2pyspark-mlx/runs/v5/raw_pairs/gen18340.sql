SELECT
    s.id,
    s.supplier_name,
    s.rating,
    s.country
FROM suppliers s
WHERE s.id IN (SELECT supplier_id FROM products WHERE product_name LIKE '%Hub%')
ORDER BY rating DESC;
