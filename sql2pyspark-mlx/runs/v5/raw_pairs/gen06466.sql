SELECT ss.units, s.sqft
FROM store_sales ss
LEFT JOIN stores s ON s.id = ss.store_id
WHERE s.store_name IS NOT NULL
