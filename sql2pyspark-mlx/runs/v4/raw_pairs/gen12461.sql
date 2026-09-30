SELECT
    id,
    supplier_id,
    price,
    category
FROM (
    SELECT
        id,
        supplier_id,
        price,
        category,
        ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY price DESC, id) rn
    FROM products
    WHERE brand IN ('Stark', 'Umbrella')
) t
WHERE rn <= 3;
