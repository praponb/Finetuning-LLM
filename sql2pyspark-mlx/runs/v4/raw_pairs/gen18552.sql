SELECT *
FROM (
    SELECT
        supplier_id,
        brand,
        price
    FROM products
) src
PIVOT (
    AVG(price) AS total,
    COUNT(1) AS n
    FOR brand IN ('Stark', 'Initech')
);
