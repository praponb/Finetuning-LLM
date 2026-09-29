SELECT
    ss.id,
    ss.store_id,
    ss.units,
    p.brand,
    p.category,
    s.sqft,
    s.store_name
FROM store_sales ss
RIGHT JOIN products p ON ss.product_id = p.id
INNER JOIN stores s ON ss.store_id = s.id
WHERE s.store_name NOT LIKE '%Downto%' AND s.city IN ('Chiang Mai', 'Bangkok')
ORDER BY ss.units DESC;
