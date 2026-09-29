WITH joined AS (
    -- Attach suppliers attributes
    SELECT
        s.id,
        s.product_name,
        s.category,
        s.brand,
        s.price,
        s.launch_date,
        s.tags,
        s.supplier_id,
        b.supplier_name,
        b.city,
        b.country
    FROM products s
    JOIN suppliers b
        ON s.supplier_id = b.id
    WHERE b.country IN ('DE', 'JP', 'TH')
)
SELECT
    id,
    product_name,
    brand,
    price,
    launch_date,
    tags,
    city,
    country,
    CASE WHEN city RLIKE '^(Singapore|Bangkok)$' THEN 'GROUP_A' WHEN city RLIKE '^(Chiang Mai|New York)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS city_zone
FROM joined
ORDER BY country DESC, launch_date;
