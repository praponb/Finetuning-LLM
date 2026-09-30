SELECT
    ss.id,
    ss.units,
    ss.sale_date,
    ss.store_id,
    s.city,
    s.region
FROM store_sales ss
LEFT JOIN stores s ON ss.store_id = s.id
WHERE s.sqft BETWEEN 8593 AND 12820
ORDER BY city
