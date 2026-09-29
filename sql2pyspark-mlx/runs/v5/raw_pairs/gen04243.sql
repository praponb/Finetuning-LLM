SELECT src.*, d.region
FROM store_sales src
INNER JOIN stores d
    ON src.store_id = d.id
WHERE d.region = 'East'
