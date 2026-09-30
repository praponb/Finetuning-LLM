WITH with_dims AS (
    SELECT
        a.id,
        a.brand,
        a.price,
        a.launch_date,
        a.supplier_id,
        d.country,
        d.city
    FROM products a
    LEFT JOIN suppliers d
        ON a.supplier_id <=> d.id
)
SELECT
    id,
    price,
    launch_date,
    country,
    city
FROM with_dims
