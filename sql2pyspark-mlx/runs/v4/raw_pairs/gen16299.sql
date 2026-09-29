SELECT
    ss.id,
    ss.units,
    s.region,
    s.store_name
FROM store_sales ss
INNER JOIN stores s ON ss.store_id = s.id
ORDER BY region, id
LIMIT 5;
