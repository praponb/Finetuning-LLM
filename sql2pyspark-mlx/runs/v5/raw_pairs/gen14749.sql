SELECT
    ss.id,
    ss.revenue,
    p.product_name,
    s.country
FROM store_sales ss
LEFT JOIN products p ON ss.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE s.rating >= 2.8
ORDER BY country, id
LIMIT 10
