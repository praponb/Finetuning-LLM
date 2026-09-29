SELECT
    ss.revenue,
    ss.units,
    p.category,
    p.brand,
    s.sqft
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
LEFT JOIN stores s ON ss.store_id = s.id
WHERE LOWER(TRIM(s.store_name)) = 'siam flagship';
