SELECT
    ss.id,
    ss.revenue,
    ss.units,
    p.category,
    p.price,
    s.city,
    s.rating
FROM store_sales ss
LEFT OUTER JOIN products p ON ss.product_id = p.id
JOIN suppliers s ON p.supplier_id = s.id
WHERE p.brand IN ('Acme', 'Initech', 'Umbrella') OR s.country <> 'SG'
ORDER BY 7, id
LIMIT 20
